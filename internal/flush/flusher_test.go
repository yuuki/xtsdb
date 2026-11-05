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
