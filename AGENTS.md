# AGENTS.md

## Rules
- Hand-off is at-least-once: a chunk leaves Redis only after all its parts are written; claims are leases; cold writes are idempotent by content.
- A due window is immutable: append, compaction and claim compare against the same Redis node clock inside atomic scripts.
- Index before samples: every stored sample belongs to a discoverable series; 64-bit series ID collisions are detected when indexing.
- Reads go to Redis first, then the persistent tier: reversed or parallelised, chunks moving during the read are lost.
- A Redis script touches one shard's hash slot (`{N}`), never two; claim, complete, quarantine and compaction stay `allow-oom`.
- An append never reads the chunk (one script call per shard and batch); hot reads cost one command per series plus one per chunk, no per-window lookups.
- go-redis applies context deadlines only between attempts, not to socket reads: pass an explicit deadline where latency matters.
- Code that depends on the TiKV backend carries `//go:build !notikv && (!darwin || cgo)`; the `notikv` build must keep compiling.
- The Go version of `go.mod` and the Dockerfile's `golang` image change together.
- Runtime dependencies need the maintainer's agreement.

## Compatibility contracts
- Redis keys and records (`internal/hot/redis.go`, `internal/hot/scripts.go`, `internal/chunk/chunk.go`), `chunk.Encoding`, `model.ID`, `model.Layout`.
- Persistent tier: Cassandra schema (`internal/cassandra/schema.go`), TiKV keys and values (`internal/tikv/keys.go`).
- Configuration keys (`internal/config/config.go`, `examples/xtsdb.yaml`), CLI (`cmd/xtsdb`), `xtsdb_*` metric names and labels (README).
- HTTP API: `internal/app/app.go`, `internal/query/api.go`, `internal/server/server.go`, Remote Write 2.0 headers (`internal/ingest/remotewrite.go`).
- Remote write limits (`ingest.Limits`, `internal/ingest/prescan.go`): letting more through is a security change.

## Before opening a pull request
- `make check`; also `make test-integration` for `internal/hot`, `internal/flush`, `internal/cassandra`, `internal/app` or `cmd/xtsdb`.
- `internal/tikv` or backend wiring: `make test-tikv` (Linux); a parser, decoder or encoder: `make test-fuzz`; a hot path: `make bench` before and after.
- Redis scripts, the flusher or the read order: update `spec/XtsdbHandoff.tla` and `make test-tla` (Java 11+).

## Git conventions
- Branches: `<type>/<issue>-<summary>` (`<type>/<summary>` without an issue); tools' `agent/*`, `claude/*`, `copilot/*`, `dependabot/*` keep their names.
- Commits and PR titles: Conventional Commits 1.0.0, types `feat` `fix` `perf` `refactor` `test` `docs` `build` `ci` `chore` `revert`; scopes: `hot`, `flush`, `ingest`, `query`, `chunk`, `cassandra`, `tikv`, `model`.
