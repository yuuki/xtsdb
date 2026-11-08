package hot

import (
	"context"
	"math"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

func compactHistogram(i int) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Schema: 1, ZeroThreshold: 0.001, ZeroCount: float64(i), Count: float64(10 + 3*i), Sum: float64(i) * 1.5,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{float64(5 + i), float64(5 + 2*i)},
	}
}

// compactAll compacts every shard once, ignoring the lease of earlier calls.
func compactAll(t *testing.T, s *Store, client redis.UniversalClient) storage.CompactResult {
	t.Helper()
	ctx := context.Background()
	var total storage.CompactResult
	for shard := range s.Shards() {
		client.Del(ctx, s.compactLeaseKey(shard))
		res, err := s.Compact(ctx, shard, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if res.Skipped {
			t.Fatalf("shard %d skipped without a lease", shard)
		}
		total.Compacted += res.Compacted
		total.Kept += res.Kept
		total.Changed += res.Changed
		total.Failed += res.Failed
		total.BytesBefore += res.BytesBefore
		total.BytesAfter += res.BytesAfter
	}
	return total
}

// run returns n samples of value v, one millisecond apart from t: enough
// records that their compacted part is smaller than they are.
func run(t int64, n int, v float64) []model.Sample {
	out := make([]model.Sample, n)
	for i := range out {
		out[i] = model.Sample{T: t + int64(i), V: v}
	}
	return out
}

func sameSamples(t *testing.T, got, want []model.Sample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if g.T != w.T || math.Float64bits(g.V) != math.Float64bits(w.V) || (g.H == nil) != (w.H == nil) ||
			(g.H != nil && !g.H.Copy().Compact(0).Equals(w.H.Copy().Compact(0))) {
			t.Fatalf("sample %d: got %+v, want %+v", i, g, w)
		}
	}
}

// expected returns what the hot tier must return for samples appended in
// this order.
func expected(samples []model.Sample) []model.Sample {
	_, out, _, err := chunk.DecodeHot(chunk.AppendRaw([]byte("0000000000000001"), samples), nil)
	if err != nil {
		panic(err)
	}
	return out
}

func TestCompactionKeepsSamples(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	now := nowMs()
	appended := map[model.SeriesID][]model.Sample{}
	ids := []model.SeriesID{1, 2, 3, 1 << 40, 7 << 50}
	appendSome := func(n int) {
		for _, id := range ids {
			var batch []model.Sample
			for i := range n {
				ts := now - int64(r.IntN(20))*1000 // out of order, with overwrites
				smp := model.Sample{T: ts, V: []float64{float64(i), r.Float64(), math.NaN()}[r.IntN(3)]}
				batch = append(batch, smp)
			}
			if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: batch}}); err != nil {
				t.Fatal(err)
			}
			appended[id] = append(appended[id], batch...)
		}
	}
	check := func() {
		t.Helper()
		got, err := s.Read(ctx, ids, now-time.Hour.Milliseconds(), now+1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			sameSamples(t, got[id], chunk.Filter(expected(appended[id]), now-time.Hour.Milliseconds(), now+1000))
		}
	}

	appendSome(50)
	res := compactAll(t, s, client)
	if res.Compacted == 0 || res.Changed != 0 || res.Failed != 0 || res.BytesAfter >= res.BytesBefore {
		t.Fatalf("unexpected first compaction %+v", res)
	}
	check()
	// The raw parts only hold their header now.
	keys, _ := client.Keys(ctx, s.prefix+":{*}:c:*").Result()
	for _, k := range keys {
		if n := client.StrLen(ctx, k).Val(); n != chunk.HeaderLen {
			t.Fatalf("%s holds %d bytes after compaction", k, n)
		}
	}

	// New samples, overwriting compacted ones too, are merged at the next
	// compaction; the latest write still wins.
	appendSome(30)
	check()
	res = compactAll(t, s, client)
	if res.Compacted == 0 || res.Changed != 0 {
		t.Fatalf("unexpected second compaction %+v", res)
	}
	check()
	// Nothing left to do.
	if res := compactAll(t, s, client); res.Compacted != 0 {
		t.Fatalf("chunks without raw records compacted: %+v", res)
	}
	check()
}

func TestCompactionLease(t *testing.T) {
	layout := model.Layout{Shards: 2, ChunkRange: time.Hour.Milliseconds(), Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	if res, err := s.Compact(ctx, 0, time.Hour); err != nil || res.Skipped {
		t.Fatalf("first compaction: %+v %v", res, err)
	}
	// Another flusher on the same store waits for the interval.
	other, err := New(ctx, client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := other.Compact(ctx, 0, time.Hour); err != nil || !res.Skipped {
		t.Fatalf("compaction within the interval must be skipped: %+v %v", res, err)
	}
	if res, err := other.Compact(ctx, 1, time.Hour); err != nil || res.Skipped {
		t.Fatalf("other shards are not affected: %+v %v", res, err)
	}
	if ttl := client.PTTL(ctx, s.compactLeaseKey(0)).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("lease must expire after the interval, got %v", ttl)
	}
}

// A compacted chunk goes through claim, load and complete like any other,
// with the same records, and complete removes both parts.
func TestCompactedChunkIsFlushed(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 1500, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMargin = 0
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(42)
	start := layout.WindowStart(id, atWindowStart(layout, id))
	var all []model.Sample
	for i := range 6 {
		smp := model.Sample{T: start + int64(i%4), V: float64(i)} // two overwrites
		all = append(all, smp)
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{smp}}}); err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			if res := compactAll(t, s, client); res.Compacted != 1 {
				t.Fatalf("unexpected compaction %+v", res)
			}
		}
	}
	m := member(id, start)
	if client.Exists(ctx, s.compactedKey(0, m)).Val() != 1 {
		t.Fatal("no compacted part")
	}
	refs := waitClaim(t, s, 0)
	chunks, missing, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 || len(missing) != 0 {
		t.Fatalf("load: %+v %v %v", chunks, missing, err)
	}
	c := chunks[0]
	if c.Records != 6 || c.CompactedSize == 0 || c.Size != chunk.HeaderLen+2*chunk.RecordLen {
		t.Fatalf("records %d, size %d, compacted size %d", c.Records, c.Size, c.CompactedSize)
	}
	sameSamples(t, c.Samples, expected(all))
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if n := client.Exists(ctx, s.chunkKey(0, m), s.compactedKey(0, m), s.windowsKey(0, id)).Val(); n != 0 {
		t.Fatalf("%d keys left after completion", n)
	}
}

// Records appended while a compaction is in progress are kept.
func TestCompactionKeepsConcurrentAppends(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(7)
	now := nowMs()
	all := run(now, 4, 1)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: all}}); err != nil {
		t.Fatal(err)
	}
	late := []model.Sample{{T: now + 1, V: 20}, {T: now + 4, V: 3}}
	s.beforeCompact = func() {
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: late}}); err != nil {
			t.Error(err)
		}
	}
	if res := compactAll(t, s, client); res.Compacted != 1 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	all = append(all, late...)
	key := s.chunkKey(0, member(id, layout.WindowStart(id, now)))
	if n := client.StrLen(ctx, key).Val(); n != chunk.HeaderLen+2*chunk.RecordLen {
		t.Fatalf("raw part holds %d bytes, want the header and the two late records", n)
	}
	got, err := s.Read(ctx, []model.SeriesID{id}, now-1000, now+1000)
	if err != nil {
		t.Fatal(err)
	}
	sameSamples(t, got[id], expected(all))
}

// A compaction does nothing if the chunk changed after it was read in any
// way other than an append.
func TestCompactionIsFenced(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(7)
	now := nowMs()
	m := member(id, layout.WindowStart(id, now))
	key, zkey := s.chunkKey(0, m), s.compactedKey(0, m)
	base := run(now, 4, 1)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: base}}); err != nil {
		t.Fatal(err)
	}
	header := client.GetRange(ctx, key, 0, 15).Val()

	cases := []struct {
		name   string
		change func()
	}{
		// Different records of the same length, as after a failover to a
		// lagging replica that received other appends.
		{"diverged records", func() {
			client.Set(ctx, key, string(chunk.AppendRaw([]byte(header), run(now, 4, 9))), 0)
		}},
		// A newer incarnation of the window.
		{"new incarnation", func() {
			client.Set(ctx, key, string(chunk.AppendRaw([]byte("9999999999999999"), base)), 0)
		}},
		// Another compaction in between.
		{"concurrent compaction", func() {
			s.beforeCompact = nil
			if res := compactAll(t, s, client); res.Compacted != 1 {
				t.Errorf("inner compaction: %+v", res)
			}
			// The same records arrive again (a retried remote write), so the
			// raw part looks as it did when it was read.
			if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: base}}); err != nil {
				t.Error(err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client.Del(ctx, zkey)
			client.Set(ctx, key, string(chunk.AppendRaw([]byte(header), base)), 0)
			s.beforeCompact = c.change
			client.Del(ctx, s.compactLeaseKey(0))
			res, err := s.Compact(ctx, 0, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if res.Compacted != 0 || res.Changed != 1 {
				t.Fatalf("a changed chunk must not be compacted: %+v", res)
			}
		})
	}
	s.beforeCompact = nil
	// The state left by the last case still reads correctly.
	got, err := s.Read(ctx, []model.SeriesID{id}, now-1000, now+1000)
	if err != nil {
		t.Fatal(err)
	}
	sameSamples(t, got[id], base)
}

// Chunks that closed, or were claimed, are never compacted: a due window
// stays immutable.
func TestCompactionLeavesClosedChunks(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 600, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMargin = 0
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(3)
	now := atWindowStart(layout, id)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: run(now, 4, 1)}}); err != nil {
		t.Fatal(err)
	}
	due := layout.Due(layout.WindowStart(id, now))
	// The window closes between the read and the swap.
	s.beforeCompact = func() { time.Sleep(time.Until(time.UnixMilli(due + 50))) }
	res := compactAll(t, s, client)
	if res.Compacted != 0 || res.Changed != 1 {
		t.Fatalf("a closed window must not be compacted: %+v", res)
	}
	s.beforeCompact = nil
	m := member(id, layout.WindowStart(id, now))
	if client.Exists(ctx, s.compactedKey(0, m)).Val() != 0 {
		t.Fatal("a closed window was compacted")
	}
	// Claimed: not even listed.
	refs := waitClaim(t, s, 0)
	if res := compactAll(t, s, client); res.Compacted+res.Changed != 0 {
		t.Fatalf("a claimed chunk was considered: %+v", res)
	}
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 || chunks[0].CompactedSize != 0 {
		t.Fatalf("load: %+v %v", chunks, err)
	}
}

// Complete is fenced on the compacted part too.
func TestCompleteIsFencedOnCompactedPart(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 1500, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMargin = 0
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(5)
	now := atWindowStart(layout, id)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: run(now, 4, 1)}}); err != nil {
		t.Fatal(err)
	}
	if res := compactAll(t, s, client); res.Compacted != 1 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	refs := waitClaim(t, s, 0)
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("load: %v %v", chunks, err)
	}
	m := member(id, refs[0].Start)
	zkey := s.compactedKey(0, m)
	compacted := client.Get(ctx, zkey).Val()
	client.Append(ctx, zkey, "x")
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, s.chunkKey(0, m)).Val() != 1 {
		t.Fatal("a chunk whose compacted part changed was deleted")
	}
	// A compacted part of the same length with other content, as a
	// compaction on a diverged replica can write.
	other := []byte(compacted)
	other[len(other)-1] ^= 0xff
	client.Set(ctx, zkey, other, 0)
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, s.chunkKey(0, m)).Val() != 1 {
		t.Fatal("a chunk whose compacted part has other content was deleted")
	}
}

func TestQuarantineMovesCompactedPart(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 1500, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMargin = 0
	s.compactMinRaw = chunk.RecordLen
	s.quarantineTTL = time.Hour
	ctx := context.Background()
	id := model.SeriesID(5)
	now := atWindowStart(layout, id)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: run(now, 4, 1)}}); err != nil {
		t.Fatal(err)
	}
	if res := compactAll(t, s, client); res.Compacted != 1 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	refs := waitClaim(t, s, 0)
	m := member(id, refs[0].Start)
	client.Append(ctx, s.chunkKey(0, m), "x") // break record alignment
	raw := client.Get(ctx, s.chunkKey(0, m)).Val()
	compacted := client.Get(ctx, s.compactedKey(0, m)).Val()
	res, err := s.Load(ctx, refs)
	if err != nil || res.Quarantined != 1 {
		t.Fatalf("load: %+v %v", res, err)
	}
	if n := client.Exists(ctx, s.chunkKey(0, m), s.compactedKey(0, m)).Val(); n != 0 {
		t.Fatalf("%d parts left behind", n)
	}
	list, err := s.ListQuarantined(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	q := list[0]
	if q.Bytes != int64(len(raw)+len(compacted)) || q.Header != raw[:16] {
		t.Fatalf("unexpected listing %+v", q)
	}
	if client.Get(ctx, q.Key).Val() != raw || client.Get(ctx, quarantinedCompactedKey(q.Key)).Val() != compacted {
		t.Fatal("parts were not kept")
	}
	if ttl := client.PTTL(ctx, quarantinedCompactedKey(q.Key)).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("compacted part must expire with the chunk, got %v", ttl)
	}
	if n, err := s.DeleteQuarantined(ctx, []string{q.Key}); err != nil || n != 1 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if client.Exists(ctx, quarantinedCompactedKey(q.Key)).Val() != 0 {
		t.Fatal("compacted part not deleted")
	}
}

// A compacted part without its chunk (deleted outside of xtsdb) is removed
// when the chunk is released.
func TestReleaseRemovesLeftoverCompactedPart(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	appendAccepted(t, s, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: atWindowStart(layout, 5), V: 1}}}})
	refs := waitClaim(t, s, 0)
	m := member(refs[0].Series, refs[0].Start)
	client.Set(ctx, s.compactedKey(0, m), "leftover", 0)
	client.Del(ctx, s.chunkKey(0, m))
	_, missing, err := load(ctx, s, refs)
	if err != nil || len(missing) != 1 {
		t.Fatalf("load: %v %v", missing, err)
	}
	if err := s.Release(ctx, missing); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, s.compactedKey(0, m)).Val() != 0 {
		t.Fatal("leftover compacted part not removed")
	}
}

// Compaction stops once a chunk holds as many records as its raw part may;
// later records stay raw, up to the size limit.
func TestCompactionRecordLimit(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	s2, err := New(ctx, client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute, MaxChunkBytes: 16 + 4*16})
	if err != nil {
		t.Fatal(err)
	}
	s2.compactMinRaw = chunk.RecordLen
	now := nowMs()
	var all []model.Sample
	add := func(n int) {
		var batch []model.Sample
		for range n {
			batch = append(batch, model.Sample{T: now + int64(len(all)+len(batch)), V: 1})
		}
		res, err := s2.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: batch}})
		if err != nil || res.Accepted != n {
			t.Fatalf("append: %+v %v", res, err)
		}
		all = append(all, batch...)
	}
	add(3)
	if res := compactAll(t, s2, client); res.Compacted != 1 {
		t.Fatalf("first compaction: %+v", res)
	}
	add(3) // 6 records, more than the 4 the raw part may hold
	if res := compactAll(t, s2, client); res.Compacted != 0 {
		t.Fatalf("compaction beyond the limit: %+v", res)
	}
	got, err := s2.Read(ctx, []model.SeriesID{1}, now, now+100)
	if err != nil {
		t.Fatal(err)
	}
	sameSamples(t, got[1], all)
}

// TestCompactionWorksAtMaxmemory needs a throwaway server whose maxmemory
// the test may change: XTSDB_TEST_REDIS_OOM_ADDR.
func TestCompactionWorksAtMaxmemory(t *testing.T) {
	addr := os.Getenv("XTSDB_TEST_REDIS_OOM_ADDR")
	if addr == "" {
		t.Skip("XTSDB_TEST_REDIS_OOM_ADDR not set")
	}
	t.Setenv("XTSDB_TEST_REDIS_ADDRS", addr)
	t.Setenv("XTSDB_TEST_REDIS_CLUSTER", "")
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	t.Cleanup(func() { client.ConfigSet(context.Background(), "maxmemory", "0") })
	client.ConfigSet(ctx, "maxmemory-policy", "noeviction")
	now := nowMs()
	for i := range 200 {
		samples := make([]model.Sample, 20)
		for k := range samples {
			samples[k] = model.Sample{T: now - int64(k)*1000, V: 1}
		}
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: model.SeriesID(i), Samples: samples}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.ConfigSet(ctx, "maxmemory", "100kb").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 999, Samples: []model.Sample{{T: now, V: 1}}}}); err == nil ||
		!strings.Contains(err.Error(), "OOM") {
		t.Fatalf("precondition: append must be refused at maxmemory, got %v", err)
	}
	before := client.MemoryUsage(ctx, s.chunkKey(0, member(0, layout.WindowStart(0, now)))).Val()
	if res := compactAll(t, s, client); res.Compacted < 200 || res.Changed+res.Failed != 0 {
		t.Fatalf("compaction must work at maxmemory: %+v", res)
	}
	if after := client.MemoryUsage(ctx, s.chunkKey(0, member(0, layout.WindowStart(0, now)))).Val(); after >= before {
		t.Fatalf("raw part still takes %d bytes (%d before)", after, before)
	}
}

// Undecodable chunks are counted and left for the flusher to quarantine.
func TestCompactionSkipsUndecodableChunks(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	now := nowMs()
	for _, id := range []model.SeriesID{1, 2} {
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: run(now, 4, 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	bad := s.chunkKey(0, member(1, layout.WindowStart(1, now)))
	client.Append(ctx, bad, "x")
	content := client.Get(ctx, bad).Val()
	res := compactAll(t, s, client)
	if res.Compacted != 1 || res.Failed != 1 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	if client.Get(ctx, bad).Val() != content {
		t.Fatal("an undecodable chunk was changed")
	}
}

// A corrupt compacted part must not crash compaction, reads or the
// flusher: it is reported, skipped, and quarantined when flushed.
func TestCorruptCompactedPart(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 1500, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMargin = 0
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	id := model.SeriesID(5)
	now := atWindowStart(layout, id)
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now, V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	m := member(id, layout.WindowStart(id, now))
	header := client.GetRange(ctx, s.chunkKey(0, m), 0, 15).Val()
	// One record, XOR encoding, a segment of one byte: chunkenc would panic.
	client.Set(ctx, s.compactedKey(0, m), header+"\x01\x01\x01\x00", 0)
	if res := compactAll(t, s, client); res.Failed != 1 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	if got, err := s.Read(ctx, []model.SeriesID{id}, now-1000, now+1000); err != nil || len(got[id]) != 0 {
		t.Fatalf("read: %v %v", got, err)
	}
	refs := waitClaim(t, s, 0)
	res, err := s.Load(ctx, refs)
	if err != nil || res.Quarantined != 1 {
		t.Fatalf("load: %+v %v", res, err)
	}
}

// Chunks with fewer than storage.CompactMinRecords raw records are left
// alone: creating their compacted part could take more memory than it
// frees.
func TestCompactionWaitsForRecords(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	now := nowMs()
	add := func(id model.SeriesID, n int) {
		var samples []model.Sample
		for i := range n {
			samples = append(samples, model.Sample{T: now + int64(i), V: 1})
		}
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: samples}}); err != nil {
			t.Fatal(err)
		}
	}
	add(1, storage.CompactMinRecords-1)
	add(2, storage.CompactMinRecords)
	if res := compactAll(t, s, client); res.Compacted != 1 || res.Kept+res.Changed+res.Failed != 0 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	if client.Exists(ctx, s.compactedKey(0, member(1, layout.WindowStart(1, now)))).Val() != 0 {
		t.Fatal("a chunk with too few records was compacted")
	}
}

// Chunks with native histograms, and chunks that would not get smaller,
// stay raw.
func TestCompactionKeepsWhatItCannotShrink(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 24 * time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	r := rand.New(rand.NewPCG(3, 4))
	now := nowMs()
	var hist, noise []model.Sample
	start := layout.WindowStart(2, now)
	ts := start
	for i := range storage.CompactMinRecords {
		hist = append(hist, model.Sample{T: now + int64(i), V: 1})
		// Random gaps of up to 90 minutes and random bits: the XOR encoding
		// takes more than 16 bytes per sample.
		ts += 1 + r.Int64N(90*time.Minute.Milliseconds())
		noise = append(noise, model.Sample{T: ts, V: math.Float64frombits(r.Uint64() &^ (0x7ff << 52))})
	}
	hist[3] = model.Sample{T: hist[3].T, H: compactHistogram(3)}
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: hist}, {ID: 2, Samples: noise}}); err != nil {
		t.Fatal(err)
	}
	res := compactAll(t, s, client)
	if res.Compacted != 0 || res.Kept != 2 {
		t.Fatalf("unexpected compaction %+v", res)
	}
	if n, _ := client.Keys(ctx, s.prefix+":{*}:z:*").Result(); len(n) != 0 {
		t.Fatalf("compacted parts written: %v", n)
	}
}

// Chunks that close within storage.CompactMargin are left to the flusher.
func TestCompactionLeavesChunksClosingSoon(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 30 * time.Second.Milliseconds(), Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	if res := compactAll(t, s, client); res.Compacted+res.Changed+res.Kept != 0 {
		t.Fatalf("a chunk closing within the margin was considered: %+v", res)
	}
}

// Open chunks are listed page by page, following their due times, also
// when many share one.
func TestCompactionPages(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	s.compactPage = 3
	ctx := context.Background()
	now := nowMs()
	var batch []storage.SeriesSamples
	for k := range 10 { // the same window offset: the same due time
		batch = append(batch, storage.SeriesSamples{ID: model.SeriesID(1 + int64(k)*layout.ChunkRange), Samples: run(now, 4, 1)})
	}
	for k := range 5 {
		batch = append(batch, storage.SeriesSamples{ID: model.SeriesID(2 + k), Samples: run(now, 4, 1)})
	}
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if res := compactAll(t, s, client); res.Compacted != len(batch) {
		t.Fatalf("compacted %d chunks, want %d: %+v", res.Compacted, len(batch), res)
	}
}

// A pass stops after an interval; the next one goes on.
func TestCompactionPassIsBounded(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	s.compactMinRaw = chunk.RecordLen
	s.compactPage = 3
	ctx := context.Background()
	now := nowMs()
	var batch []storage.SeriesSamples
	for k := range 9 {
		batch = append(batch, storage.SeriesSamples{ID: model.SeriesID(2 + k), Samples: run(now, 4, 1)})
	}
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	s.beforeCompact = func() { time.Sleep(60 * time.Millisecond) }
	res, err := s.Compact(ctx, 0, 50*time.Millisecond)
	if err != nil || res.Compacted != 3 {
		t.Fatalf("first pass: %+v %v", res, err)
	}
	s.beforeCompact = nil
	if total := res.Compacted + compactAll(t, s, client).Compacted; total != 9 {
		t.Fatalf("compacted %d chunks in two passes, want 9", total)
	}
}
