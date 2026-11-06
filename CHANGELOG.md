# Changelog

All notable changes to xtsdb are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and xtsdb
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the
storage format, the configuration file and the command line are its public
interface. See [docs/releasing.md](docs/releasing.md) for how releases are
made.

## [Unreleased]

xtsdb 2.0.0 is a ground-up rewrite of the 0.x proof of concept. Its storage
format is not compatible with 0.x; run it against a new keyspace and Redis
key prefix. Versions before 2.0.0 were not released.

### Added

- Two-tier storage on Redis 7.0+ (standalone, Sentinel or Cluster) or Valkey
  7.2+, and Apache Cassandra, with an at-least-once hand-off based on leases
  and content-idempotent writes, and no keyspace notifications (#1).
- Ingestion with Prometheus Remote Write 1.0 and 2.0 and the Graphite
  plaintext protocol over TCP and UDP, including tags (#1).
- Prometheus-compatible query API evaluated by the Prometheus 3 engine, on a
  day-partitioned series index in Cassandra (#1).
- Roles `ingester`, `flusher`, `querier` and `all`; YAML configuration with
  environment expansion; Prometheus metrics, health and readiness endpoints;
  distroless container image (#1).
- Native histograms: Remote Write 1.0 histograms and Remote Write 2.0
  integer, float and custom-bucket histograms are stored and queried like
  in Prometheus, with at most 16,384 buckets per histogram. Exemplars are
  accepted and dropped (#7).
- `xtsdb import` loads history from Prometheus TSDB blocks or timestamped
  OpenMetrics text directly into Cassandra. Imports are idempotent and can
  be resumed with `-state-file`; imported samples never override samples
  xtsdb ingested itself (#9).
- TLS for the HTTP API and the Graphite listener, with optional client
  certificates and certificate reloading, and basic or bearer authentication
  configured separately for writes and reads (`server.tls`, `server.auth`,
  `ingester.graphite_tls`) (#8).
- `xtsdb quarantine list | delete` for undecodable hot chunks, the
  `xtsdb_hot_quarantined_chunks` gauge, `outcome="quarantined"` in
  `xtsdb_flusher_chunks_total` and `flusher.quarantine_ttl` (#3).
- `storage.index_buckets` spreads the postings of a label pair and the values
  of a label over several Cassandra partitions per day (#6).
- Series ID collisions are detected with a lightweight transaction when a
  series is indexed; samples of the colliding series are rejected
  (`outcome="collision"`, `xtsdb_ingester_series_collisions_total`) (#5).
- Release process: version tags, this changelog, binaries for Linux and
  macOS attached to GitHub releases and multi-architecture images on
  `ghcr.io/yuuki/xtsdb` (#11).
- TiKV as a second backend of the persistent tier and the series index,
  through the RawKV API (API v1 with TTLs by default, or API v2):
  `storage.backend: tikv` and the `tikv` section. Ingestion, flushing,
  queries, `xtsdb import` and collision detection work as with Cassandra. A
  conformance suite in `internal/storage/storagetest` defines the semantics
  both backends provide. The build tag `notikv` leaves the backend out, and
  macOS builds without cgo (the release binaries) do not include it (#10).
- `storage.backend` is recorded in Redis, and xtsdb refuses to start when it
  changes (#10).
- Compaction of open chunks in Redis: every `flusher.compaction_interval`
  (10 minutes by default; 0 disables it), flushers compress the raw float
  samples of open chunks into a compacted part, XOR-encoded like the
  persistent tier. Redis holds about 9 bytes per sample instead of 29
  (steady state, generated data at a 15 s interval, Redis 7.0), for about
  22% more CPU time of the Redis main thread; the append path does not
  change. Chunks with fewer than 12 raw samples or with native histograms,
  and chunks that would not get smaller, are left as they are. Metrics
  `xtsdb_flusher_compaction_chunks_total`,
  `xtsdb_flusher_compaction_input_bytes_total`,
  `xtsdb_flusher_compaction_output_bytes_total` and
  `xtsdb_flusher_compaction_duration_seconds`. A chunk now has a second key
  (`<prefix>:{N}:z:<series>:<window>`) that builds without compaction do not
  know: stop every process of an earlier build before starting this one.
  [docs/hot-compression.md](docs/hot-compression.md) records the evaluation
  behind the design; encoding at append time with `BITFIELD` is tracked in
  #15.

### Changed

- Hot-tier reads fetch only the chunks that exist, through a window index
  per series, instead of one `GET` per possible window: one command per
  series plus one per chunk, whatever the range and `chunk_range` (#2).
- Chunk fragments come from a hybrid clock per Redis shard, so later
  incarnations of a window keep taking precedence when the Redis clock steps
  back; the precedence of versions of a window is defined and documented
  (#4).
- Samples expire `storage.retention` after their timestamp, and index entries
  of a day two days after the samples of that day, instead of counting from
  the time of writing; `storage.retention` must be 0 or at least a day (#9).
- `ingester.max_sample_future` must be at most a day, so that series rows
  outlive their samples.
- `storage.chunk_range` and `storage.index_buckets` are recorded in Cassandra
  (`storage.chunk_range` in TiKV), and xtsdb refuses to start when they
  change (#6, #9, #10).

### Fixed

- A corrupt chunk in the persistent tier could crash the querier: a float
  histogram chunk whose layout claims billions of buckets ran it out of
  memory, and other corrupt chunks made it panic. Chunks are now checked
  against the limits that xtsdb writes them with before they are decoded;
  parts that fail are skipped and logged with their key, and queries return
  the other samples. The flusher starts a new histogram chunk when the union
  of the bucket layouts of its histograms would exceed those limits.
- Native histograms with a NaN, infinite or negative zero threshold were
  accepted; PromQL never finished adding one with a NaN threshold to another
  histogram. They are now rejected (`outcome="invalid"`), and treated as
  corruption when read from either tier.
- Quarantining an undecodable chunk could move a healthy newer incarnation of
  its window away, and overwrote earlier quarantined chunks of the window
  (#3).
- Two label sets with the same 64-bit series ID shared postings and samples
  (#5).

[Unreleased]: https://github.com/yuuki/xtsdb/commits/master
