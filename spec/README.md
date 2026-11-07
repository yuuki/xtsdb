# Formal model of the hand-off

[`XtsdbHandoff.tla`](XtsdbHandoff.tla) is a TLA+ model of how a chunk window
moves from Redis to the persistent tier: appends, background compaction,
flushers that claim, persist and complete, and a query that reads both tiers.
`make test-tla` checks it with TLC; CI runs the same target.

The model covers one series and one window, so every sample belongs to the
same chunk. It explores every interleaving of two flushers, compaction and a
reader while:

- flushers stall, give up, and finish after their lease expired;
- the Redis node clock steps back (NTP);
- the shard fails over to a lagging replica, or is restored from an older
  snapshot: its state returns to one it had earlier, possibly on a node whose
  clock is behind.

## What corresponds to what

| Model | Code |
|---|---|
| `Ingest` | `appendScript` in [`internal/hot/scripts.go`](../internal/hot/scripts.go) |
| `Claim` | `claimScript` |
| `Load`, `Persist`, `GiveUp` | the batches of [`internal/flush/flusher.go`](../internal/flush/flusher.go): `MGET`, the cold write, a failed or late batch |
| `Complete`, `Fence` | `completeScript` |
| `CompactRead`, `CompactSwap` | compaction in [`internal/hot/compact.go`](../internal/hot/compact.go) and `compactScript` |
| `ReadStart`, `ReadFirst`, `ReadSecond` | `Queryable.Samples` in [`internal/query/queryable.go`](../internal/query/queryable.go) |
| `Replicate`, `Failover` | asynchronous Redis replication, failover or snapshot restore |
| `ColdQuorum = FALSE`, `Propagate` | `cassandra.consistency` `ONE` or `LOCAL_ONE` ([`internal/config/config.go`](../internal/config/config.go)) |
| `redis.last`, `KeepShardClock` | the shard clock key `<prefix>:{N}:clock`, which a failover rewinds with the rest of the shard |

Samples are distinct, so a chunk is the sequence of samples appended to it and
a persisted version is identified by its fragment, its total and its content
(the digest). Lengths are counted in records; the compacted part's length is
its record count, which grows with every compaction.

## Properties

| Property | Meaning |
|---|---|
| `NoUnpersistedDelete` | Delete after persist: a chunk is deleted only once every sample it holds is in the persistent tier. |
| `NoAckedSampleLost` | Every acknowledged sample is in Redis or in the persistent tier, unless a failover lost it before it was flushed (Redis persistence). |
| `NoStrandedChunk` | A chunk is always due or in flight, so a flusher will claim it. |
| `ReadsComplete` | A query returns every sample acknowledged before it started, while the chunk moves between tiers. |
| `DueWindowImmutable` | By the clock of one node, a due chunk changes only by being deleted. |

## Configurations

Each `.cfg` file states its expected outcome on its first line. The variants
that must fail show that the checks would catch the defect they introduce, so
the model does not pass vacuously.

| Configuration | Expected | What it shows |
|---|---|---|
| `XtsdbHandoff.cfg` | pass | The hand-off as implemented, with clock steps and without failover. |
| `XtsdbHandoff_FailoverLengthFence.cfg` | `NoUnpersistedDelete` violated | After a failover to a lagging replica on a node whose clock is behind, `completeScript` deletes a chunk whose fragment and lengths match what was persisted, together with records that were never persisted. |
| `XtsdbHandoff_FailoverLengthFenceOneIncarnation.cfg` | `NoUnpersistedDelete` violated | The same within one incarnation, with no fragment issued twice: the gap documented under "Fenced completion" in [docs/architecture.md](../docs/architecture.md). |
| `XtsdbHandoff_FailoverContentFence.cfg` | pass | Completion fenced by content, as compaction already is, keeps every sample through the same failovers. |
| `XtsdbHandoff_ColdFirst.cfg` | `ReadsComplete` violated | Reading the persistent tier first misses a chunk persisted after the cold read and deleted before the hot read. |
| `XtsdbHandoff_ConsistencyOne.cfg` | `ReadsComplete` violated | Reads and writes that do not overlap in Cassandra miss a chunk deleted from Redis after its write. |

## Findings

- The length fence of `completeScript` loses samples after a failover to a
  lagging replica on a node whose clock is behind, in two ways. Within one
  incarnation, other records of the same length are appended, as
  docs/architecture.md already says. And because a failover rewinds the shard
  clock key with the rest of the shard, the new node can create a later
  incarnation with the fragment of the one being persisted. The model's clock
  is coarse, which makes the second way far more likely than with microsecond
  fragments, but it shows that the fence relies on fragments never repeating,
  which the hybrid clock does not guarantee across a failover.
- Comparing the content instead, as `compactScript` already does, closes both
  ways in the model: `XtsdbHandoff_FailoverContentFence.cfg` passes every
  property through the same failovers. In the code, `completeScript` would
  compare the bytes (or a digest) of both parts instead of their lengths.
- The read order of `Queryable.Samples` and an overlapping Cassandra
  consistency level are both necessary for `ReadsComplete`, as the comments in
  `internal/query/queryable.go` and `internal/config/config.go` state.

## Not modeled

- Several windows, series and shards. Scripts touch one shard's slot, and
  windows of a series are independent, so one window is the unit of the
  hand-off.
- Quarantine, the window index (`w:<series>`), the size limits, native
  histograms and import (fragment 0).
- Byte lengths: the model compares record counts, so it does not explore a
  compaction that leaves the compacted part exactly as long in bytes.
- Eviction (`maxmemory-policy` other than `noeviction`) and TiKV.
- Liveness: flushers may give up forever, so progress is not checked.

## Running

```sh
make test-tla                                       # every configuration
python3 scripts/tlc.py --java java --jar .cache/tla/tla2tools-1.7.4.jar \
  --version 1.7.4 --sha256 <digest> --only XtsdbHandoff.cfg spec/XtsdbHandoff.tla
```

The runner downloads the pinned `tla2tools.jar` release into `.cache/tla/`,
checks its SHA-256 digest, and needs Java 11 or later. Change the model
together with the scripts it mirrors: a change to append, claim, complete,
compaction or the read order should keep every configuration ending as it
states.
