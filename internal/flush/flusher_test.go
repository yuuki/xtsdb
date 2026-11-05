package flush

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
