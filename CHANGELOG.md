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
  in Prometheus. Exemplars are accepted and dropped (#7).
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
- `storage.chunk_range` and `storage.index_buckets` are recorded in Cassandra,
  and xtsdb refuses to start when they change (#6, #9).

### Fixed

- Quarantining an undecodable chunk could move a healthy newer incarnation of
  its window away, and overwrote earlier quarantined chunks of the window
  (#3).
- Two label sets with the same 64-bit series ID shared postings and samples
  (#5).

[Unreleased]: https://github.com/yuuki/xtsdb/commits/master
