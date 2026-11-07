----------------------------- MODULE XtsdbHandoff -----------------------------
(***************************************************************************)
(* The hand-off of one chunk window from the hot tier (Redis) to the       *)
(* persistent tier, with appends, background compaction, flushers and a   *)
(* reader (docs/architecture.md, "Hand-off protocol").                      *)
(*                                                                         *)
(* One series, one window: every sample belongs to it. Redis scripts are   *)
(* atomic steps that read the clock of the node serving the shard. The     *)
(* model is adversarial:                                                   *)
(*   - flushers can stall, give up, and finish after their lease expired; *)
(*   - the node clock can step back (NTP);                                 *)
(*   - the shard can fail over to a lagging replica, or be restored from   *)
(*     an older snapshot: its state goes back to one it had earlier, and   *)
(*     the clock of the new node can be behind.                            *)
(*                                                                         *)
(* Code this models (see spec/README.md for the full mapping):             *)
(*   appendScript, claimScript, completeScript, compactScript              *)
(*                                         internal/hot/scripts.go          *)
(*   flush batches (claim, MGET, persist, complete)                        *)
(*                                         internal/flush/flusher.go        *)
(*   Queryable.Samples (hot tier, then cold tier)                          *)
(*                                         internal/query/queryable.go      *)
(*                                                                         *)
(* Samples are distinct, so a chunk is the sequence of samples appended to *)
(* it, and a persisted version is identified by its fragment, its total    *)
(* and its content (the digest).                                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Flushers,       \* flusher processes
    Samples,        \* distinct samples written to the window
    Due,            \* due time of the window: start + chunk_range + grace
    Lease,          \* claim lease (flusher.lease) in clock ticks
    MaxTime,        \* bound on the node clock
    MaxStepBack,    \* how far one clock step or failover moves the clock back
    MaxClockSteps,  \* NTP steps of the clock back, without failover
    MaxFailovers,   \* failovers to a lagging replica, or snapshot restores
    CompleteFence,  \* "length" as completeScript compares, or "content"
    HotFirst,       \* TRUE as in Queryable.Samples: hot tier read first
    ColdQuorum,     \* TRUE: reads and writes overlap (QUORUM, LOCAL_QUORUM)
    Compaction,     \* TRUE: compaction of open chunks runs
    KeepShardClock  \* FALSE as in Redis: a failover also rewinds the shard
                    \* clock key; TRUE keeps it, so no fragment is issued twice

ASSUME Due \in Nat /\ Lease \in Nat \ {0} /\ MaxTime \in Nat
ASSUME MaxStepBack \in Nat /\ MaxClockSteps \in Nat /\ MaxFailovers \in Nat
ASSUME CompleteFence \in {"length", "content"}
ASSUME {HotFirst, ColdQuorum, Compaction, KeepShardClock} \subseteq BOOLEAN

\* The shard as one Redis node holds it: the chunk key (its fragment header
\* and raw records), the compacted part (z), the membership of the window in
\* the due and in-flight sets, the lease deadline, and the shard clock key.
Empty == [present |-> FALSE, frag |-> 0, raw |-> <<>>, z |-> <<>>,
          due |-> FALSE, infl |-> FALSE, deadline |-> 0, last |-> 0]

\* What a flusher or compaction read with one MGET.
Read(r) == [present |-> r.present, frag |-> r.frag, raw |-> r.raw, z |-> r.z]
NoRead  == Read(Empty)

Elems(q) == {q[i] : i \in 1..Len(q)}

\* The samples a chunk holds, in both parts.
HotSamples(r) == IF r.present THEN Elems(r.z) \cup Elems(r.raw) ELSE {}

\* A persisted version of a chunk read r: (fragment, total, digest) and parts.
Version(r) == [frag |-> r.frag, total |-> Len(r.z) + Len(r.raw), content |-> r.z \o r.raw]

ColdSamples(vs) == UNION {Elems(v.content) : v \in vs}

VARIABLES
    now,          \* clock of the node serving the shard
    redis,        \* shard state on that node
    replica,      \* a state the shard had earlier: a lagging replica or snapshot
    cold,         \* acknowledged versions in the persistent tier
    coldSeen,     \* versions that a read below quorum can see
    pending,      \* samples not sent yet
    acked,        \* samples whose append was acknowledged
    fpc,          \* flusher step
    fload,        \* what the flusher loaded
    cpc,          \* compaction step
    cread,        \* what compaction read
    clockSteps,   \* clock steps back so far
    failovers,    \* failovers and restores so far
    lostByRedis,  \* acknowledged samples a failover lost (Redis persistence)
    lostByFlush,  \* samples deleted without being persisted: must stay empty
    rpc,          \* reader step
    rWant,        \* samples acknowledged before the read started
    rGot          \* samples the read returned

vars == <<now, redis, replica, cold, coldSeen, pending, acked, fpc, fload,
          cpc, cread, clockSteps, failovers, lostByRedis, lostByFlush,
          rpc, rWant, rGot>>

flushVars   == <<fpc, fload>>
compactVars == <<cpc, cread>>
readerVars  == <<rpc, rWant, rGot>>
ghostVars   == <<clockSteps, failovers, lostByRedis, lostByFlush>>

-----------------------------------------------------------------------------

SampleSeq(q) == Len(q) <= Cardinality(Samples) /\ Elems(q) \subseteq Samples

ReadOK(r) ==
    /\ r.present \in BOOLEAN /\ r.frag \in Nat
    /\ SampleSeq(r.raw) /\ SampleSeq(r.z)

StateOK(r) ==
    /\ ReadOK(Read(r))
    /\ r.due \in BOOLEAN /\ r.infl \in BOOLEAN
    /\ r.deadline \in Nat /\ r.last \in Nat

TypeOK ==
    /\ now \in 0..MaxTime
    /\ StateOK(redis) /\ StateOK(replica)
    /\ \A v \in cold : v.frag \in Nat /\ v.total \in Nat /\ SampleSeq(v.content)
    /\ coldSeen \subseteq cold
    /\ pending \subseteq Samples /\ acked \subseteq Samples
    /\ fpc \in [Flushers -> {"idle", "claimed", "loaded", "persisted"}]
    /\ \A f \in Flushers : ReadOK(fload[f])
    /\ cpc \in {"idle", "read"} /\ ReadOK(cread)
    /\ clockSteps \in 0..MaxClockSteps /\ failovers \in 0..MaxFailovers
    /\ lostByRedis \subseteq Samples /\ lostByFlush \subseteq Samples
    /\ rpc \in {"idle", "first", "second", "done"}
    /\ rWant \subseteq Samples /\ rGot \subseteq Samples

-----------------------------------------------------------------------------

Init ==
    /\ now = 0
    /\ redis = Empty
    /\ replica = Empty
    /\ cold = {}
    /\ coldSeen = {}
    /\ pending = Samples
    /\ acked = {}
    /\ fpc = [f \in Flushers |-> "idle"]
    /\ fload = [f \in Flushers |-> NoRead]
    /\ cpc = "idle"
    /\ cread = NoRead
    /\ clockSteps = 0
    /\ failovers = 0
    /\ lostByRedis = {}
    /\ lostByFlush = {}
    /\ rpc = "idle"
    /\ rWant = {}
    /\ rGot = {}

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<redis, replica, cold, coldSeen, pending, acked, flushVars,
                   compactVars, ghostVars, readerVars>>

\* An NTP step back. The shard clock key keeps fragments increasing.
ClockStep ==
    /\ clockSteps < MaxClockSteps
    /\ \E back \in 1..MaxStepBack :
        /\ back <= now
        /\ now' = now - back
    /\ clockSteps' = clockSteps + 1
    /\ UNCHANGED <<redis, replica, cold, coldSeen, pending, acked, flushVars,
                   compactVars, failovers, lostByRedis, lostByFlush, readerVars>>

\* appendScript: refuse a due or claimed window (too_old); otherwise create
\* the chunk with a fragment from the hybrid clock, register it in the due
\* set, and append without reading the chunk.
Ingest(s) ==
    /\ s \in pending
    /\ pending' = pending \ {s}
    /\ IF now >= Due \/ redis.infl
       THEN UNCHANGED <<redis, acked>>
       ELSE /\ acked' = acked \cup {s}
            /\ redis' =
                 IF redis.present
                 THEN [redis EXCEPT !.raw = Append(@, s)]
                 ELSE LET frag == IF now > redis.last THEN now ELSE redis.last + 1 IN
                      [redis EXCEPT !.present = TRUE, !.frag = frag, !.raw = <<s>>,
                                    !.z = <<>>, !.due = TRUE, !.last = frag]
    /\ UNCHANGED <<now, replica, cold, coldSeen, flushVars, compactVars,
                   ghostVars, readerVars>>

\* claimScript: hand out an expired lease again, or move a due window from
\* the due set to the in-flight set.
Claim(f) ==
    /\ fpc[f] = "idle"
    /\ \/ (redis.infl /\ redis.deadline <= now)
       \/ (redis.due /\ Due <= now)
    /\ redis' = [redis EXCEPT !.due = FALSE, !.infl = TRUE, !.deadline = now + Lease]
    /\ fpc' = [fpc EXCEPT ![f] = "claimed"]
    /\ UNCHANGED <<now, replica, cold, coldSeen, pending, acked, fload,
                   compactVars, ghostVars, readerVars>>

\* MGET of the chunk and its compacted part.
Load(f) ==
    /\ fpc[f] = "claimed"
    /\ fload' = [fload EXCEPT ![f] = Read(redis)]
    /\ fpc' = [fpc EXCEPT ![f] = "loaded"]
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   compactVars, ghostVars, readerVars>>

\* Write every part of the loaded version. Writes are idempotent by content.
Persist(f) ==
    /\ fpc[f] = "loaded"
    /\ cold' = IF fload[f].present THEN cold \cup {Version(fload[f])} ELSE cold
    /\ fpc' = [fpc EXCEPT ![f] = "persisted"]
    /\ UNCHANGED <<now, redis, replica, coldSeen, pending, acked, fload,
                   compactVars, ghostVars, readerVars>>

\* The fence of completeScript: the chunk is deleted only if it still is
\* the incarnation that was persisted and its lengths did not change. A
\* chunk that was missing is released only if it is still missing.
Fence(l) ==
    IF ~l.present
    THEN ~redis.present
    ELSE /\ redis.present
         /\ redis.frag = l.frag
         /\ IF CompleteFence = "length"
            THEN Len(redis.raw) = Len(l.raw) /\ Len(redis.z) = Len(l.z)
            ELSE redis.raw = l.raw /\ redis.z = l.z

Complete(f) ==
    /\ fpc[f] = "persisted"
    /\ IF Fence(fload[f])
       THEN /\ redis' = [redis EXCEPT !.present = FALSE, !.frag = 0, !.raw = <<>>,
                                      !.z = <<>>, !.infl = FALSE, !.deadline = 0]
            /\ lostByFlush' = lostByFlush \cup (HotSamples(redis) \ ColdSamples(cold))
       ELSE UNCHANGED <<redis, lostByFlush>>
    /\ fpc' = [fpc EXCEPT ![f] = "idle"]
    /\ fload' = [fload EXCEPT ![f] = NoRead]
    /\ UNCHANGED <<now, replica, cold, coldSeen, pending, acked,
                   compactVars, clockSteps, failovers, lostByRedis, readerVars>>

\* A write fails, the batch runs out of its lease, or the flusher dies: the
\* chunk stays in flight and is claimed again after its lease.
GiveUp(f) ==
    /\ fpc[f] # "idle"
    /\ fpc' = [fpc EXCEPT ![f] = "idle"]
    /\ fload' = [fload EXCEPT ![f] = NoRead]
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   compactVars, ghostVars, readerVars>>

\* Compaction reads an open chunk and its compacted part with one MGET ...
CompactRead ==
    /\ Compaction
    /\ cpc = "idle"
    /\ redis.present /\ redis.due /\ now < Due /\ redis.raw # <<>>
    /\ cread' = Read(redis)
    /\ cpc' = "read"
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   flushVars, ghostVars, readerVars>>

\* ... and compactScript swaps the parts only while the window is open by the
\* node clock, the chunk still starts with exactly the records read (header
\* included) and the compacted part is the one read. Later records are kept.
CompactSwap ==
    /\ cpc = "read"
    /\ LET n  == Len(cread.raw)
           ok == /\ redis.present /\ redis.due /\ now < Due
                 /\ redis.frag = cread.frag
                 /\ n <= Len(redis.raw)
                 /\ SubSeq(redis.raw, 1, n) = cread.raw
                 /\ redis.z = cread.z
       IN redis' = IF ok
                   THEN [redis EXCEPT !.z = cread.z \o cread.raw,
                                      !.raw = SubSeq(redis.raw, n + 1, Len(redis.raw))]
                   ELSE redis
    /\ cpc' = "idle"
    /\ cread' = NoRead
    /\ UNCHANGED <<now, replica, cold, coldSeen, pending, acked, flushVars,
                   ghostVars, readerVars>>

\* Asynchronous replication: the replica catches up with the shard.
Replicate ==
    /\ replica' = redis
    /\ UNCHANGED <<now, redis, cold, coldSeen, pending, acked, flushVars,
                   compactVars, ghostVars, readerVars>>

\* A failover to the lagging replica, or a restore of an older snapshot: the
\* shard goes back to the replica's state on a node whose clock may be
\* behind. Acknowledged samples held only by the lost suffix are lost, as
\* the Redis persistence settings allow; they are not the hand-off's loss.
\* The model's clock is coarse, so a node that is behind easily reissues a
\* fragment; with microsecond fragments that needs the same microsecond.
\* KeepShardClock rules this out to look at one incarnation only.
Failover ==
    /\ failovers < MaxFailovers
    /\ \E back \in 0..MaxStepBack :
        now' = IF back <= now THEN now - back ELSE 0
    /\ redis' = IF KeepShardClock /\ redis.last > replica.last
                THEN [replica EXCEPT !.last = redis.last]
                ELSE replica
    /\ lostByRedis' = lostByRedis \cup
           ((acked \cap HotSamples(redis)) \ (HotSamples(replica) \cup ColdSamples(cold)))
    /\ failovers' = failovers + 1
    /\ UNCHANGED <<replica, cold, coldSeen, pending, acked, flushVars,
                   compactVars, clockSteps, lostByFlush, readerVars>>

\* A version reaches the replicas that a read below quorum asks.
Propagate ==
    /\ ~ColdQuorum
    /\ \E v \in cold \ coldSeen : coldSeen' = coldSeen \cup {v}
    /\ UNCHANGED <<now, redis, replica, cold, pending, acked, flushVars,
                   compactVars, ghostVars, readerVars>>

\* A query of the window: both tiers, in the order of Queryable.Samples.
HotRead  == HotSamples(redis)
ColdRead == ColdSamples(IF ColdQuorum THEN cold ELSE coldSeen)

ReadStart ==
    /\ rpc = "idle"
    /\ rpc' = "first"
    /\ rWant' = acked
    /\ rGot' = {}
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   flushVars, compactVars, ghostVars>>

ReadFirst ==
    /\ rpc = "first"
    /\ rGot' = rGot \cup (IF HotFirst THEN HotRead ELSE ColdRead)
    /\ rpc' = "second"
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   flushVars, compactVars, ghostVars, rWant>>

ReadSecond ==
    /\ rpc = "second"
    /\ rGot' = rGot \cup (IF HotFirst THEN ColdRead ELSE HotRead)
    /\ rpc' = "done"
    /\ UNCHANGED <<now, redis, replica, cold, coldSeen, pending, acked,
                   flushVars, compactVars, ghostVars, rWant>>

Next ==
    \/ Tick \/ ClockStep \/ Replicate \/ Failover \/ Propagate
    \/ CompactRead \/ CompactSwap
    \/ ReadStart \/ ReadFirst \/ ReadSecond
    \/ \E s \in pending : Ingest(s)
    \/ \E f \in Flushers :
        Claim(f) \/ Load(f) \/ Persist(f) \/ Complete(f) \/ GiveUp(f)

Spec == Init /\ [][Next]_vars

\* Samples and flushers are interchangeable.
Symmetry == Permutations(Samples) \cup Permutations(Flushers)

-----------------------------------------------------------------------------
(* Safety properties.                                                      *)

\* Delete after persist: a chunk is deleted only once every sample it holds
\* is in the persistent tier.
NoUnpersistedDelete == lostByFlush = {}

\* Every acknowledged sample is in Redis or in the persistent tier, unless a
\* failover lost it before it was flushed.
NoAckedSampleLost ==
    \A s \in acked :
        s \in lostByRedis \/ s \in HotSamples(redis) \/ s \in ColdSamples(cold)

\* No leaks the other way: a chunk is always due or in flight, so that a
\* flusher eventually claims it.
NoStrandedChunk == redis.present => (redis.due \/ redis.infl)

\* A read returns every sample acknowledged before it started, even while the
\* chunk moves between the tiers.
ReadsComplete ==
    rpc = "done" => (rWant \ (lostByRedis \cup lostByFlush)) \subseteq rGot

\* A due window is immutable: by the clock of the same node, once the window
\* is due its chunk changes only by being deleted.
DueWindowImmutable ==
    [][ (/\ redis.present /\ now >= Due
         /\ failovers' = failovers /\ clockSteps' = clockSteps)
        => (redis'.present => /\ redis'.frag = redis.frag
                              /\ redis'.raw = redis.raw
                              /\ redis'.z = redis.z) ]_vars

=============================================================================
