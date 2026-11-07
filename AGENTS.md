# AGENTS.md

Guide for coding agents (and humans) working on xtsdb. User-facing
documentation is [README.md](README.md); the design and its failure handling
are in [docs/architecture.md](docs/architecture.md). Read the relevant part of
docs/architecture.md before changing the hot tier, the flusher or a storage
format: most rules below exist because of an invariant described there.

## 1. Overview and architecture

xtsdb is a two-tier time series database (HeteroTSDB). Recent samples live in
Redis or Valkey; closed chunk windows are compressed and moved to a persistent
tier on Apache Cassandra or TiKV (RawKV). It ingests Prometheus Remote Write
1.0/2.0 and Graphite plaintext (TCP/UDP) and serves the Prometheus HTTP API
with the Prometheus 3 PromQL engine.

Data flow:

```
remote write / Graphite -> ingest.Writer -> index (Cassandra|TiKV) + hot.Store (Redis Lua scripts)
flush.Flusher: claim (lease) -> load -> WriteCold (persistent tier) -> complete (fenced delete)
query.Queryable: index -> Redis first, then the persistent tier -> merge -> PromQL
```

Key properties (see docs/architecture.md for the reasons):

- **At-least-once hand-off.** A chunk is deleted from Redis only after all its
  parts were written; claims are leases; cold writes are idempotent by
  content (`fragment, total, digest, part` in the key).
- **A due window is immutable.** Append, compaction and claim compare against
  the same Redis node clock inside atomic scripts.
- **Index before samples.** A stored sample always belongs to a discoverable
  series; collisions of the 64-bit series ID are detected when indexing.
- **Reads go Redis first, then the persistent tier.** Reversing or
  parallelising them loses chunks that move during the read.
- One binary, four roles: `ingester`, `flusher`, `querier`, `all`, plus the
  commands `migrate`, `import`, `quarantine`, `config`, `version`.

## 2. Directory layout

| Path | Contents |
|------|----------|
| `cmd/xtsdb` | CLI entry point and commands. |
| `internal/app` | Wiring of roles, lifecycle, end-to-end and security tests. |
| `internal/model` | Label canonicalization, series IDs, shards, windows, days (storage format). |
| `internal/chunk` | Hot raw/compacted parts, cold XOR/typed blobs, merge and de-duplication. |
| `internal/hot` | Hot tier on Redis/Valkey: Lua scripts (`scripts.go`), compaction, quarantine. |
| `internal/storage` | Tier interfaces; `storagetest` has in-memory stores and the backend conformance suite. |
| `internal/cassandra`, `internal/tikv` | Persistent tier and index backends. |
| `internal/ingest` | Writer, Remote Write handler (`prescan.go` bounds requests), Graphite server. |
| `internal/flush` | Flusher (hand-off and compaction loops). |
| `internal/query` | Matcher resolution, tier merge, Prometheus HTTP API. |
| `internal/server`, `internal/config` | HTTP server and auth, YAML configuration and TLS. |
| `internal/backfill` | `xtsdb import` (Prometheus TSDB blocks, OpenMetrics). |
| `internal/backendtest`, `internal/tlstest`, `internal/faultnet` | Test-only helpers: backend selection, certificates, a fault-injecting TCP proxy. |
| `docs/` | Architecture, hot-tier compression evaluation, release process. |
| `deploy/`, `compose.yaml`, `examples/xtsdb.yaml` | Quickstart stack, TiKV dev config, documented example configuration. |

## 3. Build

Go version: `go.mod` (1.27.3); the Dockerfile's `golang` image must match.

```bash
make build      # bin/xtsdb (CGO_ENABLED=0)
make cross      # release targets (linux/darwin, amd64/arm64) and `go vet -tags notikv`
make docker     # container image
make snapshot   # GoReleaser archives in dist/ (needs GoReleaser v2)
```

The `notikv` build tag leaves out the TiKV backend; macOS builds without cgo
leave it out too. Code under `internal/tikv` and `internal/backendtest/tikv.go`
carries `//go:build !notikv && (!darwin || cgo)`; keep the `notikv` build
compiling (`make cross` vets it).

## 4. Test

| Command | What | Needs |
|---------|------|-------|
| `make test` (= `make test-race`) | Unit tests with `-race`; integration tests skip | nothing |
| `make test-integration` | Everything against Redis and Cassandra from `compose.yaml`, including the Redis fault-injection tests | Docker |
| `make test-tikv` | TiKV backend tests against a single-node PD/TiKV | Docker, Linux (host network) |
| `make test-fuzz` | Every `FuzzXxx` target for `FUZZTIME` (30s) | nothing |
| `make bench` | Benchmarks without backends into `bench.txt` (`benchstat`) | nothing |

- Integration tests read `XTSDB_TEST_REDIS_ADDRS` (comma separated;
  `XTSDB_TEST_REDIS_CLUSTER=1` for Cluster), `XTSDB_TEST_CASSANDRA_HOSTS`,
  `XTSDB_TEST_TIKV_PD` (+ `XTSDB_TEST_TIKV_API_VERSION=v2`) and skip without
  them. `XTSDB_TEST_STRESS=1` adds a million-series index stress test. The
  Makefile exports the Redis/Cassandra defaults, which is why `make test`
  unsets them.
- Both persistent backends run `internal/storage/storagetest` conformance
  tests; TiKV also against an in-memory RawKV (`memkv_test.go`).
- **Fault injection.** `internal/flush/fault_test.go` drives the flusher
  with in-memory stores whose calls fail before or after taking effect
  (`FuzzFlushUnderFaults`, `TestPartialWriteIsRetried`).
  `internal/hot/fault_test.go` runs real Redis behind `internal/faultnet`
  (stall, lost replies, resets) and checks that no acknowledged sample is
  lost; it needs a standalone Redis and skips in Cluster mode.
- **Fuzz targets** (seeds run as normal tests): chunk decoders and round
  trips (`internal/chunk`), Remote Write prescan/decoding and Graphite lines
  (`internal/ingest`), label canonicalization and layout (`internal/model`),
  TiKV series entries and corrupt segments (`internal/tikv`), query time and
  duration parameters (`internal/query`), and the fault schedule of the
  flusher. A failing input lands in `testdata/fuzz/<Target>/`: commit it with
  the fix as a regression seed.
- **Goroutine leaks.** Packages that start goroutines have a `leak_test.go`
  with `goleak.VerifyTestMain`. Stop what a test starts (servers, flushers,
  proxies, clients) before it returns. Runs against a real TiKV
  (`XTSDB_TEST_TIKV_PD`) skip the check in `internal/tikv`, `internal/app` and
  `cmd/xtsdb` until the TiKV client's goroutines after Close are verified.
- Tests use fake clocks (`storagetest.Hot.Now`, `clock` in flush tests) and
  short real windows for Redis tests. The machine running CI can be slow:
  wait for conditions, never for fixed sleeps, when asserting.
- `internal/hot/eval_*_test.go` are evaluations behind
  docs/hot-compression.md; they skip without Redis.

## 5. Static analysis

```bash
make lint          # go vet, golangci-lint (.golangci.yml) and `go mod tidy -diff`
make fmt           # golangci-lint fmt (gofmt + goimports with local prefix github.com/yuuki/xtsdb)
make lint-actions  # actionlint + zizmor on .github/ (offline unless GH_TOKEN is set)
make vuln          # govulncheck
make check         # lint + lint-actions + test + cross: the local pre-PR gate
```

Tool versions are pinned at the top of the Makefile and in the workflows;
bump them together. `go run tool@version` tools are built with this module's
toolchain (`GO_TOOLCHAIN`). Where `GH_TOKEN` is set but the GitHub API is not
reachable, run `env -u GH_TOKEN make check`.

`.golangci.yml` enables gosec with G104 (duplicate of errcheck) and G115
(deliberate bit-for-bit integer conversions of the storage formats)
excluded. Any other suppression is a `//nolint:<linter> // <reason>` on the
line. Code complexity is tracked by `.github/workflows/complexity.yml`
(cccc, `cccc.toml`): a function over a Cognitive Complexity of 30 fails;
split it rather than raise the limit.

## 6. Coding conventions

- Logging: `log/slog`, structured key/value pairs, a `component` attribute
  per subsystem. Driver logs are routed to slog (`hot.SetLogger`, Cassandra's
  `slogAdapter`, TiKV's zap redirect).
- Errors: wrap with `%w` and context (`fmt.Errorf("redis meta: %w", err)`);
  storage failures reach clients as `ingest.ErrStorage` (503, retried);
  corrupt data is `chunk.ErrCorrupt`. Decoders of stored or received bytes
  return errors, never panic; the chunk decoders recover from panics of the
  Prometheus iterators.
- Comments explain why, in full sentences; exported identifiers have doc
  comments (revive). English only.
- Commits: a plain imperative subject that says what changes ("Split the hot
  tier's Append into its three steps"), with the reason in the body. User
  visible changes get a CHANGELOG.md entry (Keep a Changelog).

## 7. Compatibility contracts

xtsdb follows SemVer; its public interface is the storage format, the
configuration file and the command line (docs/releasing.md). Do not change
silently:

- **Redis key layout and record formats**: `internal/hot/redis.go` (key list),
  `internal/hot/scripts.go`, `internal/chunk/chunk.go` (16-byte header and
  records, compacted part). `<prefix>:meta` records shards, chunk range,
  backend and window index.
- **Cassandra schema**: `internal/cassandra/schema.go`; **TiKV keys and
  values**: `internal/tikv/keys.go` (golden tests `TestKeysGolden`).
- **Series identity**: `model.ID`, `model.Layout` (shards, window offsets,
  days) — golden tests pin them.
- **Cold blob encodings**: `chunk.Encoding` values and their layout.
- **Configuration keys**: `internal/config/config.go`, documented with
  defaults in `examples/xtsdb.yaml` (keep both and README in sync).
- **CLI**: commands and flags in `cmd/xtsdb`.
- **HTTP API**: `/api/v1/write`, the Prometheus query endpoints in
  `internal/query/api.go`, `/metrics`, `/-/healthy`, `/-/ready`,
  `/debug/pprof/`; Remote Write 2.0 response headers.
- **Metric names and labels**: `xtsdb_*` metrics (README "Monitoring"); label
  values are fixed outcome sets, never data.
- **Remote write limits**: `ingest.Limits` and `prescan` bound what a request
  may decode into; a change that lets more through is a security change.

## 8. Concurrency and resource management

- Every goroutine has an owner that stops and waits for it: the Graphite
  server (`Stop` waits for connections and UDP workers), the flusher
  (`Run` returns after its workers), the HTTP server (`Serve` shuts down on
  context cancellation). Tests check this with goleak.
- A started flush batch finishes on `context.WithoutCancel`; leases protect
  the rest. Persisted chunks are completed with their own timeout
  (`completeTimeout`).
- Redis calls: go-redis does not apply context deadlines to socket reads
  (`ContextTimeoutEnabled` is off); it checks them between attempts. Without a
  deadline a call during a partition takes several `redis.read_timeout`s
  (retries, plus stalled connection setup); pass a deadline where latency
  matters.
- All Redis scripts of a shard touch one hash slot (`{N}`) and are atomic;
  claim, complete, quarantine and compaction are `allow-oom` so flushing works
  when Redis is full. Never add a script that touches two shards.
- Writer index writes run in an errgroup bounded by
  `ingester.index_write_concurrency`; remote write requests are bounded by
  `ingester.max_inflight_requests` (503 beyond).

## 9. Performance constraints

- Hot paths: remote write prescan/decoding, `model.Canonicalize`/`ID`,
  `Writer.Write` (index cache hits), the append script, `chunk.DecodeHot` and
  `Decode` in reads and flushes. Benchmarks: `make bench`.
- An append never reads the chunk; one script call per shard and batch;
  pipelines are bounded (`maxChunksPerCall`, `maxPipelineCmds`).
- Hot-tier reads cost one command per series plus one per chunk (window
  index); do not reintroduce per-window lookups.
- Bound memory by what the input claims: count elements on the wire before
  decoding (prescan), check chunk headers before allocating (chunk
  `layout.go`), stream query samples lazily (256 series at a time).
- Metric label cardinality is fixed: no series IDs, labels or keys in metric
  labels (they go to logs).

## 10. Before opening a PR

1. `make check` passes.
2. Touching `internal/hot`, `internal/flush`, `internal/cassandra`,
   `internal/app` or `cmd/xtsdb`: `make test-integration`.
3. Touching `internal/tikv` or backend wiring: `make test-tikv` (Linux).
4. Touching a parser, decoder or encoder: `make test-fuzz` (or the target with
   `FUZZTIME=2m`) and keep any crasher as a seed.
5. Touching a hot path: `make bench` before and after, compare with
   `benchstat`.
6. User-visible change: CHANGELOG.md, README.md and `examples/xtsdb.yaml`.

## 11. Rules for AI agents

Forbidden:

- disabling, skipping, deleting or weakening tests, fuzz properties or lint
  rules to get green, or hiding failures (`|| true`, `continue-on-error`,
  blanket excludes);
- editing generated or vendored files by hand (there is no code generation
  in this repository; protobuf types come from the Prometheus module);
- adding dependencies without a stated reason (runtime dependencies need the
  maintainer's agreement; test-only ones a justification);
- changing a compatibility contract (section 7) without calling it out in
  the PR and CHANGELOG.md;
- loosening workflow permissions, unpinning actions, enabling caches in
  release jobs, or adding third-party services or GitHub Apps;
- committing secrets, rewriting history, force-pushing, tagging or publishing
  releases.

## 12. Changes that need human review

Even with green CI: workflows and release configuration (`.github/`,
`.goreleaser.yaml`, `Dockerfile`), permissions and secrets handling
(`internal/server/auth.go`, `internal/config/tls.go`), parsers of untrusted
input (`internal/ingest`, `internal/query/api.go`), the Lua scripts and the
hand-off protocol (`internal/hot`, `internal/flush`), storage formats
(section 7), dependency additions, and large refactors.
