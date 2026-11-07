# AGENTS.md

This file holds only what the code does not show. Read the relevant part of
[docs/architecture.md](docs/architecture.md) before changing the hot tier,
the flusher or a storage format: most rules below come from it.

## Rules the code does not show

- At-least-once hand-off: delete a chunk from Redis only after all its parts
  were written. Claims are leases; cold writes are idempotent by content
  (`fragment, total, digest, part` in the key).
- A due window is immutable: append, compaction and claim compare against
  the same Redis node clock inside atomic scripts.
- Index before samples: a stored sample always belongs to a discoverable
  series; detect 64-bit series ID collisions when indexing.
- Reads go to Redis first, then the persistent tier: reversing or
  parallelising them loses chunks that move during the read.
- A started flush batch finishes on `context.WithoutCancel`; leases protect
  the rest.
- All Redis scripts of a shard touch one hash slot (`{N}`); never add a
  script that touches two shards. Claim, complete, quarantine and compaction
  stay `allow-oom` so that flushing works when Redis is full.
- go-redis does not apply context deadlines to socket reads, only between
  attempts: pass an explicit deadline where latency matters.
- An append never reads the chunk; keep one script call per shard and batch,
  and bounded pipelines.
- Hot-tier reads cost one command per series plus one per chunk: do not
  reintroduce per-window lookups.
- Bound memory by what the input claims: count elements on the wire before
  decoding (prescan), check chunk headers before allocating, stream query
  samples lazily.
- Decoders of stored or received bytes return errors (`chunk.ErrCorrupt`),
  never panic. Storage failures reach clients as `ingest.ErrStorage` (503,
  retried).
- Metric label values are fixed outcome sets: no series IDs, labels or keys
  (they go to logs).
- TiKV code carries `//go:build !notikv && (!darwin || cgo)`; keep the
  `notikv` build compiling.
- Tests wait for conditions, never for fixed sleeps: CI machines are slow.
- Keep a fuzz crasher in `testdata/fuzz/<Target>/` with its fix, as a seed.
- The Go version of `go.mod` and the Dockerfile's `golang` image change
  together; so do pinned tool versions in the `Makefile` and the workflows.
- User-visible changes get a `CHANGELOG.md` entry under `[Unreleased]`.
- English only.

## Compatibility contracts

xtsdb follows SemVer (`docs/releasing.md`). Call any change out in the pull
request and in `CHANGELOG.md`:

- Redis key layout and record formats: `internal/hot/redis.go`,
  `internal/hot/scripts.go`, `internal/chunk/chunk.go`.
- Cassandra schema: `internal/cassandra/schema.go`; TiKV keys and values:
  `internal/tikv/keys.go`.
- Series identity: `model.ID`, `model.Layout`.
- Cold blob encodings: `chunk.Encoding`.
- Configuration keys: `internal/config/config.go`, with `examples/xtsdb.yaml`
  and the README kept in sync.
- CLI commands and flags: `cmd/xtsdb`.
- HTTP API: `internal/app/app.go`, `internal/query/api.go`,
  `internal/server/server.go`, and the Remote Write 2.0 response headers
  (`internal/ingest/remotewrite.go`).
- `xtsdb_*` metric names and labels, documented in `README.md`.
- Remote write limits (`ingest.Limits`, `internal/ingest/prescan.go`): a
  change that lets more through is a security change.

## Before opening a pull request

- `make check`.
- `internal/hot`, `internal/flush`, `internal/cassandra`, `internal/app` or
  `cmd/xtsdb`: also `make test-integration`.
- `internal/tikv` or backend wiring: also `make test-tikv` (Linux).
- A parser, decoder or encoder: also `make test-fuzz` (or the target with
  `FUZZTIME=2m`).
- A hot path: `make bench` before and after, compared with `benchstat`.
- User-visible change: `CHANGELOG.md`, `README.md` and `examples/xtsdb.yaml`.

## Git conventions

Branches: `<type>/<issue>-<summary>`, or `<type>/<summary>` without an issue
(`fix/166-connect-errors`, `feat/qp-counters`). `<type>` is a commit type
below; `<summary>` is 2–5 lowercase ASCII words joined by `-`. Branches named
by tools keep their names (`agent/*`, `claude/*`, `copilot/*`,
`dependabot/*`). Never push to the default branch.

Commits follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/):

    <type>(<scope>)!: <subject>

    <body>

    <footer>

- `type`: `feat` (new user-visible behaviour), `fix` (bug fix), `perf`,
  `refactor` (no behaviour change), `test`, `docs`, `build` (build system,
  dependencies, packaging), `ci` (workflows), `chore` (anything else, e.g.
  `chore(release): prepare v1.2.3`), `revert`.
- `scope`: optional; the package or area changed: `hot`, `flush`, `ingest`,
  `query`, `chunk`, `cassandra`, `tikv`, `model`.
- `!` and a `BREAKING CHANGE: <what changed and how to migrate>` footer for
  any change to a compatibility contract above.
- `subject`: imperative, lower case (except identifiers), no final period,
  at most 72 characters including the prefix.
- `body`: why the change is needed and what it affects, wrapped at 72
  columns; required unless the subject says everything.
- `footer`: `Fixes #N` / `Refs #N`, `BREAKING CHANGE:`, trailers such as
  `Co-Authored-By:`.
- One logical change per commit; regenerated files go in the commit that
  changes their source. Pull request titles use the same format (they become
  the squash commit).

## Forbidden

- Disabling, skipping, deleting or weakening tests, fuzz properties or lint
  rules to get green, or hiding failures (`|| true`, `continue-on-error`,
  blanket excludes); a suppression other than
  `//nolint:<linter> // <reason>` on the line.
- Adding dependencies without a stated reason (runtime dependencies need the
  maintainer's agreement; test-only ones a justification).
- Changing a compatibility contract without calling it out in the pull
  request and `CHANGELOG.md`.
- Loosening workflow permissions, unpinning actions, enabling caches in
  release jobs, or adding third-party services or GitHub Apps.
- Committing secrets, rewriting history, force-pushing, tagging or
  publishing releases.

## Needs human review

Even with green CI: workflows and release configuration (`.github/`,
`.goreleaser.yaml`, `Dockerfile`), permissions and secrets handling
(`internal/server/auth.go`, `internal/config/tls.go`), parsers of untrusted
input (`internal/ingest`, `internal/query/api.go`), the Lua scripts and the
hand-off protocol (`internal/hot`, `internal/flush`), storage formats,
dependency additions, and large refactors.

## Environment notes

- With `GH_TOKEN` set but the GitHub API unreachable, run
  `env -u GH_TOKEN make check`.
