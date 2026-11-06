# Gorilla compression in the hot tier

This document records the evaluation of Gorilla compression (the Prometheus
XOR encoding) in the hot tier, and the reasons for the design that was
chosen. The persistent tier already stores Gorilla-compressed chunks (see
[architecture.md](architecture.md#cold-tier-cassandra)); the hot tier
stored 16-byte raw records.

- **Question.** Can the hot tier hold Gorilla-compressed samples, in
  particular from the moment they are ingested?
- **Decision.** Keep the append path as it is, and compress the float
  samples of open chunks asynchronously ("compaction"), every 10 minutes by
  default. It is implemented as described in
  [architecture.md](architecture.md#compaction).
- **Not chosen for now.** Encoding at append time inside Redis with
  `BITFIELD` works, but more than doubles the CPU time of Redis. When it
  would pay off is tracked in
  [#15](https://github.com/yuuki/xtsdb/issues/15).

## Summary

The same workload, measured the same way for every design
([Comparison under the same conditions](#comparison-under-the-same-conditions)):

| | Redis memory | CPU of the Redis main thread |
|---|---|---|
| Raw records (no compaction) | 28.9 B/sample | 5.0 µs/sample |
| Compaction every 10 minutes (the default) | 9.4 B/sample | 6.1 µs/sample (+22%) |
| Compaction every 5 minutes | 8.2 B/sample | 6.5 µs/sample (+29%) |
| Encoding at append time with `BITFIELD` | 7.5 B/sample | 10.9–12.2 µs/sample (+116% to +143%) |

Redis 7.0.15, generated data, 15 s interval, 1 hour `chunk_range`, default
10 minute grace period; memory of the whole store in steady state.

- Compressing each write batch on its own does not help: remote write and
  Graphite deliver about one sample per series per request, and a Gorilla
  block of one sample takes 17 bytes, more than a raw record.
- Gorilla needs the state of the previous sample of the series. The
  ingesters are stateless and do not read a chunk when they append to it,
  so encoding at append time has to run inside Redis, in Lua. A complete
  encoder that writes its bits with `BITFIELD` produces exactly the
  Prometheus XOR chunk and keeps every guarantee of the hot tier, but it
  more than doubles the CPU time of every append.
- Compaction folds the raw float records of an open chunk into a compressed
  part in the background. The append path is unchanged, the encoding runs
  in Go in the flusher, and Redis only lists, reads and swaps the parts of
  each chunk with a compare-and-swap. It trades CPU for memory: it lets a
  Redis core hold more series when memory is what limits it (below about
  14 GB of data per Redis main thread at these rates), and fewer when CPU
  is.
- Raw records used more memory than documented: Redis grows a string
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
- Memory: growth of `used_memory` (`INFO memory`).
- Two kinds of CPU measurements, which must not be compared with each
  other:
  - **Comparison under the same conditions** (`TestEvalCapacity`): requests
    of 2,000 series with one sample each, as remote write sends them, so
    that a script call carries about 8 chunks (one call per shard of the
    256); the CPU time of the Redis main thread (`used_cpu_*_main_thread`
    in `INFO cpu`), which runs every command and script, and as a
    cross-check the time of the commands the client sends.
  - **Microbenchmarks** of a design's parts: the time of `EVAL`/`EVALSHA`
    in `INFO commandstats` for script calls of 256 chunks with one sample
    each. They show where time goes, not what a deployment pays: a call
    carries far more chunks than a remote write request produces.

  Commands run by a script count both under their own name and in the time
  of the script, so only commands the client sends are summed. Results vary
  by about 10% between runs.
- The code is in [`internal/hot/eval_compression_test.go`](../internal/hot/eval_compression_test.go)
  and [`internal/hot/eval_bitfield_test.go`](../internal/hot/eval_bitfield_test.go),
  behind the build tag `compressioneval`. It runs against a scratch server
  and **deletes its data** (`FLUSHALL`):

  ```bash
  redis-server --port 6399 --save '' --appendonly no
  XTSDB_EVAL_REDIS=127.0.0.1:6399 go test -tags compressioneval -run Eval -v -timeout 60m ./internal/hot/
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

Memory of the chunk keys only, in several layouts, every series holding
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

Chunk keys only, chunk fill uniform in 1–240 samples and no grace period
(`TestEvalSteadyStateMemory`):

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
| Encode at append time in Lua | large | Redis CPU more than doubles, see below |
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

Memory, without grace period (`TestEvalBitfieldMemory`): 8.00 B/sample for
the whole store against 29.99 with raw records; chunk values 3.83 B/sample
(the 60-byte header included), plus 327 bytes per chunk for the key and the
allocation (1,445 with raw records).

Where the time goes (microbenchmark, `TestEvalBitfieldCost`: script time
only, 256 chunks per call, not comparable with the comparison below):

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
  `struct` and using `BITFIELD` only for the bits removes most of it; the
  comparison below uses that variant.
- Writing the bits costs 1–2 µs, the encoding in Lua 0.7–2.8 µs, and the
  state handling about 3 µs more than the raw append script.
- An earlier cost model that built the bits as Lua strings
  (`TestEvalLuaGorillaModelCost`) measured 6.04 µs against 2.54.

Two variants would change the balance; both are tracked in
[#15](https://github.com/yuuki/xtsdb/issues/15):

- **Client-side encoding.** If the ingester keeps the encoder state of the
  series it receives, Redis only has to compare a version and write the bits
  and the state: 3.63 µs/sample on the hit path in the microbenchmark
  (`TestEvalClientSideEncodingCost`), 1.4 times raw records there. A miss
  can fall back to a raw record without losing correctness, but hits need
  every series routed to the same ingester (a distributor and a hash ring);
  with a load balancer that ignores series, an ingester receives the
  previous sample of a series once in N.
- **Long hot windows.** With several hours in Redis, memory dominates and
  the absence of a raw tail matters more.

### Compaction of open chunks (model)

Layout D of the measurements, which is what was implemented:

- A chunk is two keys: `c`, the fragment followed by raw records appended
  as before, and `z`, the compacted part, which only compaction writes.
- Compaction (in the flusher) reads both keys of open chunks, merges the
  compacted samples with the raw records (`Normalize(z ++ c)`), encodes the
  result, and replaces both in one script: `z` gets the new blob, `c` keeps
  its fragment and the records appended since it was read. The append path
  does not change.
- The script runs only while the window is open, by the same Redis clock as
  appends and claims, so a due window stays immutable. It compares the
  content that was read, not just lengths: after a failover to a lagging
  replica, different records of the same length could have been appended.
- A model of the compare-and-swap alone, with 40 raw records and no
  compacted part yet, costs 6.11 µs per chunk, 0.15 µs per sample folded
  (microbenchmark, `TestEvalCompactionModelCost`). The implementation costs
  more: it also lists the open chunks and reads them (below).

### Comparison under the same conditions

`TestEvalCapacity` runs the implementation of compaction and the `BITFIELD`
prototype on the same workload: 12,000 series in 256 shards, written in
requests of 2,000 series with one sample each.

- **CPU** is measured over the life of a window: every series receives a
  whole window of 240 samples (1 hour at 15 s), and each shard is compacted
  every 40 or 20 samples (10 or 5 minutes), at a phase of its own.
- **Memory** is measured in steady state with the default 10 minute grace
  period: the fill of the current window is uniform in 1–240 samples, and
  one series in six still holds its previous, full window, compacted once
  more during the grace period (160.5 samples per series).

| | Memory | Main thread: appending | Main thread: compaction | Commands: appending | Commands: compaction |
|---|---|---|---|---|---|
| Raw records | 28.93 B/sample | 5.02 µs/sample | | 4.41 µs/sample | |
| Compaction every 40 samples | 9.35 B/sample | 5.53 µs/sample | 0.58 µs/sample | 4.90 µs/sample | 0.42 µs/sample |
| Compaction every 20 samples | 8.15 B/sample | 5.40 µs/sample | 1.09 µs/sample | 4.81 µs/sample | 0.78 µs/sample |
| `BITFIELD`, state as bytes | 7.52 B/sample | 12.22 µs/sample | | 11.66 µs/sample | |
| `BITFIELD`, lean | 7.52 B/sample | 10.85 µs/sample | | 10.29 µs/sample | |

- Compaction cuts memory by 3.1 times (every 40 samples) to 3.5 times
  (every 20), and costs 22% to 29% more CPU of the main thread. Appending
  itself costs about 10% more with compaction: after a compaction, the raw
  part is a short string again, which the next appends reallocate as it
  grows.
- `BITFIELD` takes 8% to 20% less memory than compaction, for 116% to 143%
  more CPU than raw records.
- "Commands" is the time of the commands the client sends (scripts and the
  reads of compaction). The main thread also spends time on the protocol and
  the network.

What this means for one Redis main thread (Lua and every command run on
it; I/O threads do not run scripts), at a 15 s interval and 160.5 samples
held per series:

| | CPU per sample | Series per thread, by CPU | Memory per series | Memory at CPU saturation | Series with 8 GB | Series with 16 GB |
|---|---|---|---|---|---|---|
| Raw records | 5.0 µs | 3.0 M | 4.6 KB | 13.9 GB | 1.7 M (memory) | 3.0 M (CPU) |
| Compaction every 10 minutes | 6.1 µs | 2.5 M | 1.5 KB | 3.7 GB | 2.5 M (CPU) | 2.5 M (CPU) |
| Compaction every 5 minutes | 6.5 µs | 2.3 M | 1.3 KB | 3.0 GB | 2.3 M (CPU) | 2.3 M (CPU) |
| `BITFIELD`, lean | 10.9 µs | 1.4 M | 1.2 KB | 1.7 GB | 1.4 M (CPU) | 1.4 M (CPU) |

Raw records need about 14 GB of memory per Redis main thread before CPU
limits them. With less memory per thread, as with one Redis Cluster shard
per core on machines with 4–8 GB per core, compaction lets each thread hold
more series: about 40% more with 8 GB, and the less memory, the larger the
gain. With more than about 14 GB per thread, raw records hold more series
and compaction only saves memory. Compaction every 10 minutes holds more
series than every 5 minutes whenever there are more than about 3.5 GB per
thread, which is why it is the default.

### Memory effect of one compaction

A compaction creates the compacted part of a chunk the first time: a key of
its own. Change of `used_memory` per chunk when a chunk with n raw records,
appended one at a time, is compacted (`TestEvalCompactionMemoryDelta`, the
swap without the rules of the implementation), in bytes:

| First compaction, n = | 1 | 2 | 4 | 6 | 8 | 12 | 16 | 24 | 32 |
|---|---|---|---|---|---|---|---|---|---|
| constant (`up`) | +79 | +80 | −32 | −32 | −32 | −288 | −288 | −288 | −864 |
| integer counter | +80 | +80 | −31 | −16 | −16 | −266 | −256 | −240 | −799 |
| CPU seconds (0.01) | +80 | +80 | −16 | 0 | +15 | −223 | −178 | −144 | −657 |
| random float | +80 | +87 | −12 | +2 | +17 | −208 | −176 | −112 | −592 |

Every later compaction, after one of 40 records, frees memory for every
kind of series: 71 to 80 bytes per chunk for one record, about 300 to 450
bytes for 12 to 24 records. Hence the rule that a chunk is compacted only from
12 records on (`storage.CompactMinRecords`): from there even the first
compaction frees memory, and the CPU time of a compaction is spread over
at least 12 samples.

## Adversarial review

Two independent reviews of the implementation found the following, which
were fixed before it was merged:

- **A corrupt compacted part crashed the flusher.** The Prometheus chunk
  iterators panic on some corrupt input, and a corrupt float histogram chunk
  can make them allocate many gigabytes. Decoding now recovers panics as
  `chunk.ErrCorrupt`, compacted parts may only hold XOR-encoded floats, and
  decoding stops as soon as the samples outnumber the records the part
  claims. A fuzz test covers `chunk.DecodeHot`. Undecodable chunks are
  quarantined at flush, as before.
- **Native histograms changed when compacted.** Decoding a histogram from a
  Prometheus chunk sets counter reset hints of its own (and widens bucket
  layouts), so a late sample merged before a compacted histogram could hide
  a counter reset from PromQL; gauge histograms whose buckets move grew
  instead of shrinking, quadratically in CPU. Chunks with native histograms
  are no longer compacted.
- **Compaction could grow memory**, for chunks with few records (the first
  compaction creates a key), and for incompressible data. Chunks with fewer
  than 12 raw records are left alone, and a compacted part that would not
  be smaller than what it replaces is not written (outcome `kept`).
- **Listing open chunks with `LIMIT offset` was quadratic** in the size of a
  shard. Pages now follow the due times.
- **Passes could pile up** when one took longer than the interval. A pass
  now stops after an interval; the next one goes on.
- **The quarantine key of a chunk hashed both parts without a separator**;
  it now hashes the length of the raw part too.
- The first measurements compared numbers taken under different conditions
  (script time for 256 chunks per call against every command for about 24),
  counted commands run by scripts twice, and left out the grace period. The
  comparison above replaces them.
- The tests did not catch a broken compaction schedule (every shard
  compacted on every offer, or only once) or a missing margin; tests now
  do, and check the metrics.

## Decision

Compaction reaches almost the memory of encoding at append time (9.4
against 7.5 B/sample) for a fifth of its extra CPU (+22% against +116% to
+143%), keeps the append path and the ingesters as they are, does its
encoding work outside Redis with the existing, tested Go encoder, and works
on every supported Redis and Valkey version. It is enabled by default every
10 minutes (`flusher.compaction_interval`). A deployment whose Redis is
limited by CPU rather than memory can set a longer interval or disable it
(0).

## Open questions

- All measurements were made on Redis 7.0.15. Redis 8 and Valkey may
  allocate differently; the tests run on Redis 8.10 and Valkey 9.1 in CI,
  but the memory and CPU figures were not measured there.
- The data is generated. Real data compresses better, which makes the raw
  tail a larger part of what remains.
- Native histograms stay raw in Redis. Compacting them would need an
  encoding that keeps their counter reset hints exactly and bounds the
  growth of gauge histograms.
- The CPU time of compaction has not been optimized: the `STRLEN` pass, the
  comparison and rewriting of whole compacted parts, and the reallocation of
  the raw part after a compaction could be made cheaper.
