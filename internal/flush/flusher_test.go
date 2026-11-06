package flush

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time           { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration)      { c.mu.Lock(); defer c.mu.Unlock(); c.t = c.t.Add(d) }
func newClock() *clock                    { return &clock{t: time.Unix(1_700_000_000, 0)} }
func (c *clock) ms(d time.Duration) int64 { return c.Now().Add(d).UnixMilli() }

func setup(t *testing.T) (*Flusher, *storagetest.Hot, *storagetest.Cold, *clock) {
	t.Helper()
	layout := model.Layout{Shards: 4, ChunkRange: time.Minute.Milliseconds(), Grace: 10 * time.Second.Milliseconds()}
	c := newClock()
	hot := storagetest.NewHot(layout, 30*time.Second)
	hot.Now = c.Now
	cold := storagetest.NewCold(layout.ChunkRange)
	f := New(hot, cold, Options{PollInterval: 10 * time.Millisecond, BatchSize: 2, Concurrency: 2, Lease: 30 * time.Second})
	return f, hot, cold, c
}

func TestFlushMovesClosedChunks(t *testing.T) {
	f, hot, cold, c := setup(t)
	ctx := context.Background()
	var batch []storage.SeriesSamples
	for i := range 5 {
		batch = append(batch, storage.SeriesSamples{
			ID:      model.SeriesID(i * 7919),
			Samples: []model.Sample{{T: c.ms(0), V: 1}, {T: c.ms(time.Second), V: 2}},
		})
	}
	if _, err := hot.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}
	f.FlushOnce(ctx)
	if cold.Len() != 0 {
		t.Fatal("open chunks must not be flushed")
	}
	c.Add(2 * time.Minute)
	f.FlushOnce(ctx)
	if cold.Len() != 5 || hot.Len() != 0 {
		t.Fatalf("cold=%d hot=%d, want 5 and 0", cold.Len(), hot.Len())
	}
	got, err := cold.Read(ctx, []model.SeriesID{7919}, c.ms(-time.Hour), c.ms(0))
	if err != nil || len(got[7919]) != 2 {
		t.Fatalf("cold read: %v %v", got, err)
	}
}

func TestFailedChunksAreRetriedAfterLease(t *testing.T) {
	f, hot, cold, c := setup(t)
	ctx := context.Background()
	if _, err := hot.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: c.ms(0), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	c.Add(2 * time.Minute)
	cold.FailWrites = errors.New("cassandra down")
	f.FlushOnce(ctx)
	if hot.Len() != 1 {
		t.Fatal("chunk must stay in the hot tier when persisting fails")
	}
	cold.FailWrites = nil
	f.FlushOnce(ctx)
	if cold.Len() != 0 {
		t.Fatal("leased chunk must not be retried before the lease expires")
	}
	c.Add(time.Minute)
	f.FlushOnce(ctx)
	if cold.Len() != 1 || hot.Len() != 0 {
		t.Fatalf("cold=%d hot=%d after retry", cold.Len(), hot.Len())
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	f, hot, cold, c := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := hot.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: c.ms(0), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	c.Add(2 * time.Minute)
	done := make(chan error)
	go func() { done <- f.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for cold.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	if cold.Len() != 1 {
		t.Fatal("Run did not flush")
	}
}

// slowCold delays every write, to simulate a struggling Cassandra.
type slowCold struct {
	*storagetest.Cold
	delay time.Duration
}

func (s *slowCold) WriteChunk(ctx context.Context, c storage.ColdChunk) error {
	time.Sleep(s.delay)
	return s.Cold.WriteChunk(ctx, c)
}

func TestSlowColdTierStillMakesProgress(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Minute.Milliseconds()}
	c := newClock()
	hot := storagetest.NewHot(layout, 150*time.Millisecond)
	hot.Now = c.Now
	cold := &slowCold{Cold: storagetest.NewCold(layout.ChunkRange), delay: 60 * time.Millisecond}
	// 64 chunks at 60ms with 16 parallel writes need ~240ms: longer than the lease.
	f := New(hot, cold, Options{BatchSize: 64, Concurrency: 1, Lease: 150 * time.Millisecond})
	var batch []storage.SeriesSamples
	for i := range 64 {
		batch = append(batch, storage.SeriesSamples{ID: model.SeriesID(i * 7919), Samples: []model.Sample{{T: c.ms(0), V: 1}}})
	}
	if _, err := hot.Append(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	c.Add(2 * time.Minute)
	for round := 0; hot.Len() > 0; round++ {
		before := hot.Len()
		done := make(chan struct{})
		go func() { f.FlushOnce(context.Background()); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("FlushOnce did not return")
		}
		if hot.Len() >= before {
			t.Fatalf("round %d made no progress (%d chunks left)", round, before)
		}
		c.Add(time.Second) // expire the leases of skipped chunks
	}
	if cold.Len() != 64 {
		t.Fatalf("cold chunks = %d", cold.Len())
	}
}

func TestLargeChunksAreSplitIntoParts(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds()}
	c := newClock()
	hot := storagetest.NewHot(layout, time.Minute)
	hot.Now = c.Now
	cold := storagetest.NewCold(layout.ChunkRange)
	f := New(hot, cold, Options{BatchSize: 10, Concurrency: 1, Lease: time.Minute})
	ctx := context.Background()
	const n = 250_000
	start := layout.WindowStart(3, c.ms(0))
	samples := make([]model.Sample, n)
	for i := range samples {
		samples[i] = model.Sample{T: start + int64(i), V: float64(i)}
	}
	if _, err := hot.Append(ctx, []storage.SeriesSamples{{ID: 3, Samples: samples}}); err != nil {
		t.Fatal(err)
	}
	c.Add(2 * time.Hour)
	f.FlushOnce(ctx)
	if cold.Len() != 3 {
		t.Fatalf("expected 3 parts, got %d rows", cold.Len())
	}
	got, err := cold.Read(ctx, []model.SeriesID{3}, start, start+time.Hour.Milliseconds())
	if err != nil {
		t.Fatal(err)
	}
	if len(got[3]) != n || got[3][n-1].V != n-1 {
		t.Fatalf("read back %d samples", len(got[3]))
	}
}

func TestMissingChunksAreReleased(t *testing.T) {
	f, hot, _, c := setup(t)
	ctx := context.Background()
	if _, err := hot.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: c.ms(0), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	c.Add(2 * time.Minute)
	refs, _ := hot.Claim(ctx, hot.Layout.Shard(1), 10)
	hot.Delete(refs[0])
	c.Add(time.Minute) // the lease expires; the flusher re-claims and finds nothing
	f.FlushOnce(ctx)
	if hot.Inflight() != 0 {
		t.Fatalf("in-flight entry of a missing chunk leaked")
	}
}

func TestStaleVersionDoesNotOverwriteCompleteOne(t *testing.T) {
	f, _, cold, c := setup(t)
	ctx := context.Background()
	ref := storage.ChunkRef{Series: 4, Start: c.ms(-time.Minute)}
	full := storage.HotChunk{Ref: ref, Fragment: 42, Samples: []model.Sample{{T: c.ms(-50 * time.Second), V: 1}, {T: c.ms(-40 * time.Second), V: 2}}}
	// Redis restored from a snapshot taken before the second sample: same
	// incarnation (fragment), less data.
	stale := storage.HotChunk{Ref: ref, Fragment: 42, Samples: full.Samples[:1]}
	if err := f.persist(ctx, full); err != nil {
		t.Fatal(err)
	}
	if err := f.persist(ctx, stale); err != nil {
		t.Fatal(err)
	}
	got, err := cold.Read(ctx, []model.SeriesID{4}, c.ms(-time.Hour), c.ms(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(got[4]) != 2 {
		t.Fatalf("persisting a stale version lost data: %v", got[4])
	}
}

// A window rewritten in Redis and persisted twice from the same incarnation
// (as after a restore from an older snapshot) keeps the latest value: the
// version with more appended records wins, even when both versions have the
// same number of distinct timestamps.
func TestLaterStateOfIncarnationWins(t *testing.T) {
	ctx := context.Background()
	for _, order := range []string{"stale first", "complete first"} {
		f, _, cold, c := setup(t)
		t0 := c.ms(0)
		ref := storage.ChunkRef{Series: 1, Start: t0}
		stale := storage.HotChunk{Ref: ref, Fragment: 7, Records: 2,
			Samples: []model.Sample{{T: t0, V: 1}, {T: t0 + 1, V: 2}}}
		// t0+1 rewritten. With these values the digest of the stale version
		// is the larger one, so the digest alone would pick the wrong value.
		complete := storage.HotChunk{Ref: ref, Fragment: 7, Records: 3,
			Samples: []model.Sample{{T: t0, V: 1}, {T: t0 + 1, V: 11}}}
		chunks := []storage.HotChunk{stale, complete}
		if order == "complete first" {
			chunks = []storage.HotChunk{complete, stale}
		}
		for _, hc := range chunks {
			if err := f.persist(ctx, hc); err != nil {
				t.Fatal(err)
			}
		}
		got, err := cold.Read(ctx, []model.SeriesID{1}, t0, t0+10)
		if err != nil || len(got[1]) != 2 || got[1][1].V != 11 {
			t.Fatalf("%s: got %v %v, want the rewritten value 11", order, got[1], err)
		}
	}
}

// Compaction must not change what is flushed: the same versions, totals,
// digests and data reach the cold tier with or without it.
func TestCompactionDoesNotChangeFlushedData(t *testing.T) {
	ctx := context.Background()
	layout := model.Layout{Shards: 2, ChunkRange: time.Hour.Milliseconds(), Grace: 10 * time.Minute.Milliseconds()}
	c := newClock()
	newTier := func() (*Flusher, *storagetest.Hot, *storagetest.Cold) {
		hot := storagetest.NewHot(layout, time.Minute)
		hot.Now = c.Now
		hot.CompactMinRecords = 1
		cold := storagetest.NewCold(layout.ChunkRange)
		return New(hot, cold, Options{BatchSize: 16, Concurrency: 2, Lease: time.Minute, CompactionInterval: time.Second}), hot, cold
	}
	plainF, plainHot, plainCold := newTier()
	compF, compHot, compCold := newTier()
	for round := range 3 {
		var batch []storage.SeriesSamples
		for i := range 5 {
			batch = append(batch, storage.SeriesSamples{ID: spreadID(i), Samples: []model.Sample{
				{T: c.ms(0), V: float64(round)},
				{T: c.ms(-time.Duration(round) * time.Second), V: float64(10 * round)}, // overwrites an earlier sample
				{T: c.ms(time.Millisecond), V: math.NaN()},
			}})
		}
		for _, hot := range []*storagetest.Hot{plainHot, compHot} {
			if _, err := hot.Append(ctx, batch); err != nil {
				t.Fatal(err)
			}
		}
		compF.CompactOnce(ctx)
		c.Add(time.Second)
	}
	if compHot.CompactionsDone() == 0 {
		t.Fatal("nothing was compacted")
	}
	c.Add(2 * time.Hour)
	plainF.FlushOnce(ctx)
	compF.FlushOnce(ctx)
	if plainHot.Len() != 0 || compHot.Len() != 0 {
		t.Fatalf("chunks left: %d and %d", plainHot.Len(), compHot.Len())
	}
	want, got := plainCold.Chunks(), compCold.Chunks()
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("cold chunks differ with compaction:\n got %+v\nwant %+v", got, want)
	}
}

// spreadID returns series IDs that fall into different shards (the shard
// comes from the high 32 bits of the ID).
func spreadID(i int) model.SeriesID { return model.SeriesID(uint64(i)<<32 | uint64(i)*7919) }

// countingHot records the compactions the flusher asks for.
type countingHot struct {
	*storagetest.Hot
	mu      sync.Mutex
	offered map[int]int
	ran     map[int]int
}

func (h *countingHot) Compact(ctx context.Context, shard int, interval time.Duration) (storage.CompactResult, error) {
	res, err := h.Hot.Compact(ctx, shard, interval)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.offered[shard]++
	if !res.Skipped {
		h.ran[shard]++
	}
	return res, err
}

// Run compacts every shard, about once per interval: shards are offered
// often, and the lease of the store lets one compaction per interval run.
func TestRunCompactsOncePerInterval(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: 10 * time.Minute.Milliseconds()}
	hot := &countingHot{Hot: storagetest.NewHot(layout, time.Minute), offered: map[int]int{}, ran: map[int]int{}}
	hot.CompactMinRecords = 1
	const interval = 300 * time.Millisecond
	f := New(hot, storagetest.NewCold(layout.ChunkRange), Options{
		PollInterval: time.Hour, // does not slow down compaction
		BatchSize:    2, Concurrency: 2, Lease: time.Minute, CompactionInterval: interval,
	})
	var batch []storage.SeriesSamples
	for i := range 8 {
		batch = append(batch, storage.SeriesSamples{ID: spreadID(i), Samples: []model.Sample{{T: time.Now().UnixMilli(), V: 1}}})
	}
	if _, err := hot.Append(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	start := time.Now()
	go func() { done <- f.Run(ctx) }()
	time.Sleep(1050 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if n := hot.CompactionsDone(); n != 8 {
		t.Fatalf("%d chunks compacted, want 8", n)
	}
	hot.mu.Lock()
	defer hot.mu.Unlock()
	for shard := range layout.Shards {
		// The lease allows one compaction at the start and one per interval
		// after it.
		most := 1 + int(elapsed/interval)
		if r := hot.ran[shard]; r < 2 || r > most {
			t.Errorf("shard %d compacted %d times in %v, want 2 to %d", shard, r, elapsed, most)
		}
		if o := hot.offered[shard]; o < 3*most {
			t.Errorf("shard %d offered %d times, want about ten per interval", shard, o)
		}
	}
}

func metricValue(t *testing.T, m prometheus.Metric) *dto.Metric {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func TestCompactionMetrics(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: 10 * time.Minute.Milliseconds()}
	c := newClock()
	hot := storagetest.NewHot(layout, time.Minute)
	hot.Now = c.Now
	hot.CompactMinRecords = 1
	reg := prometheus.NewRegistry()
	f := New(hot, storagetest.NewCold(layout.ChunkRange), Options{
		BatchSize: 2, Concurrency: 2, Lease: time.Minute, CompactionInterval: time.Minute, Registerer: reg,
	})
	var batch []storage.SeriesSamples
	for i := range 6 {
		batch = append(batch, storage.SeriesSamples{ID: spreadID(i), Samples: []model.Sample{{T: c.ms(0), V: 1}, {T: c.ms(time.Second), V: 2}}})
	}
	if _, err := hot.Append(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	f.CompactOnce(context.Background())
	f.CompactOnce(context.Background()) // within the interval: skipped
	if got := metricValue(t, f.compactions.WithLabelValues("compacted")).GetCounter().GetValue(); got != 6 {
		t.Fatalf("compacted chunks = %v, want 6", got)
	}
	in := metricValue(t, f.compactInBytes).GetCounter().GetValue()
	out := metricValue(t, f.compactOutBytes).GetCounter().GetValue()
	if in != 6*2*16 || out <= 0 {
		t.Fatalf("bytes in %v, out %v", in, out)
	}
	// One observation per shard that was compacted, not for skipped ones.
	if n := metricValue(t, f.compactShardTime).GetHistogram().GetSampleCount(); n != uint64(layout.Shards) {
		t.Fatalf("%d duration observations, want %d", n, layout.Shards)
	}
}
