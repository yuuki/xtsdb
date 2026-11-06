# Gorilla compression in the hot tier

This document records the evaluation of Gorilla compression (the Prometheus
XOR encoding) in the hot tier, and the reasons for the design that was
chosen. The persistent tier already stores Gorilla-compressed chunks (see
[architecture.md](architecture.md#cold-tier-cassandra)); the hot tier
stored 16-byte raw records.

- **Question.** Can the hot tier hold Gorilla-compressed samples, in
  particular from the moment they are ingested?
- **Decision.** Keep the append path as it is, and compress the open
  chunks of the hot tier asynchronously ("compaction").
- **Not chosen for now.** Encoding at append time inside Redis with
  `BITFIELD` works, but costs about three times the Redis CPU. When it
  would pay off is tracked in
  [#15](https://github.com/yuuki/xtsdb/issues/15).

## Summary

| | Redis memory, chunk keys | Redis time per sample |
|---|---|---|
| Raw records, as before compaction | 28.1 B/sample | 2.3–2.6 µs |
| Encoding at append time with `BITFIELD` | 6.5 B/sample | 6.2–10.6 µs |
| Compaction every 20 samples (5 min at 15 s) | 6.9 B/sample | +0.3 µs |
| Compaction every 40 samples (10 min at 15 s) | 8.7 B/sample | +0.15 µs |
| Lower bound: every chunk one Gorilla blob | 4.5 B/sample | |

All values were measured on Redis 7.0.15 with generated data (see
[Method](#method)), with chunk fill spread uniformly over 1–240 samples, as
it is at any moment with a 1 hour `chunk_range` and a 15 s interval.

- Compressing each write batch on its own does not help: remote write and
  Graphite deliver about one sample per series per request, and a Gorilla
  block of one sample takes 17 bytes, more than a raw record.
- Gorilla needs the state of the previous sample of the series. The
  ingesters are stateless and do not read a chunk when they append to it,
  so encoding at append time has to run inside Redis, in Lua. A complete
  encoder that writes its bits with `BITFIELD` produces exactly the
  Prometheus XOR chunk and keeps every guarantee of the hot tier, but Redis
  then runs out of CPU long before it runs out of memory: it holds fewer
  series per node than raw records do.
- Compaction folds the raw records of an open chunk into a compressed part
  in the background. The append path is unchanged, the work runs in Go in
  the flusher, and Redis only performs a compare-and-swap that costs about
  6 µs per chunk.
- The raw format used more memory than documented: Redis grows a string
  greedily on `APPEND`, so a chunk held 28 bytes per sample on average, not
  16, and up to 39 just after its buffer doubled. Compaction rewrites values
  with `SET`, which allocates them to size.

## Constraints of the hot tier

Any encoding has to keep the properties the hand-off protocol relies on
([architecture.md](architecture.md#hot-tier-redis)):

1. **Appends do not read.** The append script adds raw records with `APPEND`
   in arrival order. Ingesters are stateless; consecutive samples of a
   series can reach different ingesters through the load balancer.
2. **Arrival order, duplicates and overwrites.** Until its window closes, a
   chunk accepts samples in any order, including retries and new values for
   a timestamp; reads sort the records and the latest write wins
   (`chunk.Normalize`).
3. **Fences.** `complete` and `quarantine` act only if the chunk still has
   the header (fragment) and the length that were read.
4. **Records.** The number of records of an incarnation (`total` in the
   persistent tier) orders versions of a window; it was derived from the
   length.
5. **Native histograms** are variable-length records; the Prometheus float
   histogram chunk re-encodes a whole chunk when the bucket layout changes.
6. **A due window is immutable**, decided by the clock of the Redis node in
   atomic scripts.
7. **Compatibility:** Redis 7.0+ and Valkey 7.2+, standalone or Cluster,
   managed services included: no modules, Lua 5.1 scripts on one slot.

## Method

- Redis 7.0.15 (jemalloc 5.3.0) on the same machine, without persistence.
  Go 1.27.3; Prometheus `tsdb/chunkenc` v0.315.1, the version xtsdb uses.
- Data: seven kinds of series, 15 s apart, aligned like Prometheus scrapes
  (and with ±2 ms jitter where stated): a constant (`up`), a small integer
  gauge, an integer counter, a byte counter, a CPU seconds counter with two
  decimals, a memory gauge in 4 KiB steps, and a random float gauge. The mix
  is pessimistic: real Prometheus data compresses to about 1.3–2 bytes per
  sample, this mix to 3.3.
- Memory: growth of `used_memory` (`INFO memory`) over 12,000–24,000 series.
- Redis CPU: time of `EVAL`/`EVALSHA` from `INFO commandstats`, with
  256 chunks per script call and one sample per chunk, which is what a
  remote write request produces. Results vary by about 10% between runs.
- The code is in [`internal/hot/eval_compression_test.go`](../internal/hot/eval_compression_test.go)
  and [`internal/hot/eval_bitfield_test.go`](../internal/hot/eval_bitfield_test.go),
  behind the build tag `compressioneval`. It runs against a scratch server
  and **deletes its data** (`FLUSHALL`):

  ```bash
  redis-server --port 6399 --save '' --appendonly no
  XTSDB_EVAL_REDIS=127.0.0.1:6399 go test -tags compressioneval -run Eval -v ./internal/hot/
  ```

## Results

### Compression by block size

Bytes per sample when the samples of a window are compressed in
independent blocks (`TestEvalBlockCompression`). A block of 1 is what
compressing each write batch would achieve; 240 is a whole window of one
hour.

| Series (aligned timestamps) | 1 | 2 | 4 | 8 | 20 | 40 | 80 | 240 |
|---|---|---|---|---|---|---|---|---|
| constant (`up`) | 17.00 | 10.00 | 5.00 | 2.62 | 1.20 | 0.72 | 0.49 | 0.33 |
| small integer gauge | 17.00 | 10.19 | 5.39 | 2.97 | 1.51 | 1.01 | 0.75 | 0.65 |
| integer counter | 17.00 | 10.98 | 6.59 | 4.33 | 2.97 | 2.51 | 2.32 | 2.17 |
| byte counter | 17.00 | 12.00 | 8.11 | 6.11 | 4.90 | 4.55 | 4.29 | 4.31 |
| CPU seconds (0.01) | 17.00 | 12.91 | 9.58 | 7.83 | 6.81 | 6.46 | 6.31 | 6.29 |
| memory (4 KiB steps) | 17.00 | 11.07 | 6.66 | 4.42 | 3.02 | 2.52 | 2.28 | 2.18 |
| random float | 17.00 | 13.81 | 10.70 | 9.12 | 8.05 | 7.70 | 7.48 | 7.38 |
| **mean** | 17.00 | 11.56 | 7.43 | 5.34 | 4.07 | 3.64 | 3.42 | 3.33 |
| mean, ±2 ms jitter | 17.00 | 11.55 | 8.30 | 6.62 | 5.59 | 5.23 | 5.07 | 4.97 |

Blocks of 20–40 samples reach about 90% of the compression of a whole
window; a block of one is worse than a raw record.

### Memory of raw records

Redis grows a string to twice its new length (below 1 MiB) when `APPEND`
needs more room, so a chunk built by appends holds unused memory
(`TestEvalAppendSlack`, `MEMORY USAGE`, in bytes):

| Records | Value | After one `APPEND` per record | After one `SET` |
|---|---|---|---|
| 20 | 336 | 496 | 432 |
| 80 | 1,296 | 2,608 | 1,584 |
| 160 | 2,576 | 6,192 | 3,120 |
| 240 | 3,856 | 6,192 | 4,144 |

Memory of the chunk keys in several layouts, every series holding
160 samples (`TestEvalLayoutsMemory`):

| Layout | B/sample | B/series |
|---|---|---|
| A: raw records, one `APPEND` per sample (before compaction) | 38.86 | 6,218 |
| E: raw records, one `SET` | 19.66 | 3,145 |
| B: one Gorilla blob, one `SET` | 4.17 | 668 |
| B': Gorilla blocks of 40 samples, one `SET` | 4.55 | 728 |
| C: one key, Gorilla blocks, then raw tail by `APPEND` | 9.20 | 1,473 |
| D: two keys, Gorilla blocks (`SET`) and raw tail (`APPEND`) | 7.95 | 1,272 |

The real store at that point used 39.85 B/sample (6,376 B/series) including
the due set and the window indexes. In layout C, the first `APPEND` after
the compressed value was written doubles the whole value; in layout D, the
greedy growth only affects the small raw tail.

Steady state, chunk fill uniform in 1–240 samples (`TestEvalSteadyStateMemory`):

| Layout | B/sample |
|---|---|
| A: raw records by `APPEND` (before compaction) | 28.09 |
| E: raw records, one `SET` | 18.25 |
| B: one Gorilla blob per chunk (lower bound) | 4.46 |
| D: compaction every 40 samples, merged into one blob | 8.73 |
| D: compaction every 20 samples, merged into one blob | 6.89 |

### Options

| Option | Memory saved | Why not |
|---|---|---|
| Compress each write batch in the ingester | none (17 B/sample) | one sample per series per batch |
| Encode at append time in Lua | large | Redis CPU ×3 or more, see below |
| Stateful ingesters with series affinity and a WAL (as in Cortex or Mimir) | large | gives up stateless ingesters, a different system |
| RedisTimeSeries (`TS.ADD`, part of Redis 8) | large | not in Valkey, Redis 7 or managed services without modules; fences and versions would have to be rebuilt on it |
| Timestamps as 32-bit offsets from the window start (12-byte records) | 25% | stateless, but small |
| **Asynchronous compaction of open chunks** | large | chosen |

### Encoding at append time with BITFIELD

`BITFIELD` reads and writes integers of 1–64 bits at any bit offset of a
string, in C. That is what a bit stream needs, so the prototype
(`eval_bitfield_test.go`) builds a complete Gorilla encoder in the append
script on top of it:

- **State in the chunk.** After the fragment, the chunk holds the number of
  records, the length of the bit stream in bits, the latest timestamp of any
  record (`maxT`), and the encoder state (previous timestamp, delta, value,
  leading and trailing zeros). Then follows a Prometheus XOR chunk: the
  sample count and the bit stream. The script reads the state, computes
  delta-of-delta and XOR in Lua, writes the bits at the end of the stream
  with `BITFIELD SET u<n>` (bits are accumulated into fields of up to 52
  bits, exact in a Lua number) and writes the state back. Ingesters stay
  stateless.
- **Out of order, overwrites, histograms.** Only a float sample with a
  timestamp above `maxT` extends the stream. Every other record (out of
  order, duplicate, overwrite, histogram, or beyond 65,535 samples) is
  appended raw to a second key. A timestamp enters the stream only if no
  record had it before, and every later record with that timestamp is raw,
  so reading `Normalize(stream ++ raw)` keeps "the latest write wins".
- **Fences.** The number of records follows the fragment and grows with
  every append, so a fence compares the first 20 bytes of the chunk.

Correctness:

- 180 series appended one sample per call, including random bit patterns,
  NaN, ±Inf, -0, jitter and gaps that need each of the 14, 17, 20 and
  64-bit delta-of-delta codes: the stream is **byte for byte** the
  Prometheus XOR chunk of the same samples (`TestEvalBitfieldMatchesPrometheus`).
- 300 series appended in random batches, with random order, duplicates,
  overwrites, NaNs and histograms: reads return exactly the samples and the
  record count of the raw format (`TestEvalBitfieldSemantics`).

Memory, steady state (`TestEvalBitfieldMemory`): **8.00 B/sample** for the
whole store against 29.99 with raw records; chunk values 3.83 B/sample (the
60-byte header included), plus 327 bytes per chunk for the key and the
allocation (1,445 with raw records).

Redis time per sample (`TestEvalBitfieldCost`):

| µs/sample | constant | integer counter | random float |
|---|---|---|---|
| Raw records (current append script) | 2.25 | 2.56 | 2.57 |
| State and bits through `BITFIELD` | 20.05 | 21.73 | 23.78 |
| State through `GETRANGE`/`SETRANGE`, bits through `BITFIELD` | 7.45 | 8.83 | 10.62 |
|   … without writing the bits | 6.01 | 7.70 | 8.52 |
|   … without encoding (state only) | 5.36 | 5.84 | 5.71 |
| Lean: no `STRLEN`, the state read doubles as `EXISTS` | 6.23 | 7.45 | 9.80 |

- `BITFIELD` subcommands are expensive: reading and writing ten state fields
  with 20 subcommands costs about 13 µs. Packing the state into bytes with
  `struct` and using `BITFIELD` only for the bits removes most of it.
- Writing the bits costs 1–2 µs, the encoding in Lua 0.7–2.8 µs, and the
  state handling about 3 µs more than the raw append script. Even the lean
  variant costs three to four times as much as raw records.
- An earlier cost model that built the bits as Lua strings
  (`TestEvalLuaGorillaModelCost`) measured 6.04 µs against 2.54.

Capacity of one Redis core (Lua runs on the main thread; I/O threads do not
help), with a 1 hour `chunk_range`, 15 s interval and the integer counter
timings; memory of chunk keys only:

| | µs/sample | Series per core | Memory per series | Memory at CPU saturation | Series on a 16 GB node |
|---|---|---|---|---|---|
| Raw records | 2.56 | 5.9 M | 3.4 KB | 20 GB | 4.7 M (memory) |
| `BITFIELD`, lean | 7.45 | 2.0 M | 0.8 KB | 1.6 GB | 2.0 M (CPU) |
| Compaction, 5 min | 2.87 | 5.2 M | 0.8 KB | 4.3 GB | 5.2 M (CPU) |

With encoding at append time, Redis saturates its CPU with 1.6 GB of data,
and a node holds fewer series than with raw records. Each script call also
blocks Redis three times longer (about 2 ms for 256 chunks), which delays
queries.

Two variants would change the balance; both are tracked in
[#15](https://github.com/yuuki/xtsdb/issues/15):

- **Client-side encoding.** If the ingester keeps the encoder state of the
  series it receives, Redis only has to compare a version and write the bits
  and the state: 3.63 µs/sample on the hit path
  (`TestEvalClientSideEncodingCost`), 1.4 times raw records. A miss can fall
  back to a raw record without losing correctness, but hits need every
  series routed to the same ingester (a distributor and a hash ring); with a
  load balancer that ignores series, an ingester receives the previous
  sample of a series once in N.
- **Long hot windows.** With several hours in Redis, memory dominates and
  the absence of a raw tail matters more, but compaction remains as small
  in memory at a fraction of the CPU.

### Compaction of open chunks

Layout D of the measurements, which is what was implemented:

- A chunk is two keys: `c`, the fragment followed by raw records appended
  as before, and `z`, the compacted part, which only compaction writes.
- Compaction (in the flusher) reads both keys of open chunks, merges the
  compacted samples with the raw records (`Normalize(z ++ c)`), encodes the
  result with the persistent tier's encoding, and replaces both in one
  script: `z` gets the new blob, `c` keeps its fragment and the records
  appended since it was read. The append path does not change.
- The script runs only while the window is open, by the same Redis clock as
  appends and claims, so a due window stays immutable. It compares the
  content that was read, not just lengths: after a failover to a lagging
  replica, different records of the same length could have been appended.
- A compare-and-swap with 40 raw records costs 6.11 µs per chunk, 0.15 µs
  per sample folded (`TestEvalCompactionModelCost`); compacting every
  20 samples costs about 0.3 µs per sample, 12% of an append.

## Decision

Compaction reaches about the memory of encoding at append time (6.9 against
6.5 B/sample) for less than a tenth of the extra Redis CPU, keeps the append path and
the ingesters as they are, does its encoding work outside Redis with the
existing, tested Go encoder, and works on every supported Redis and Valkey
version.

## Open questions

- All measurements were made on Redis 7.0.15. Redis 8 and Valkey may
  allocate differently; the tests run on Redis 8.10 and Valkey 9.1 in CI,
  but the memory figures were not measured there.
- The data is generated. Real data compresses better, which makes the raw
  tail a larger part of what remains.
