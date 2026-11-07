package flush

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

var errInjected = errors.New("injected fault")

// fault is what happens to one call of a store.
type fault uint8

const (
	noFault    fault = iota
	failBefore       // the call fails without taking effect
	failAfter        // the call takes effect, but its reply is lost
)

// faults decides the fault of every call from a schedule. The decision
// depends only on the schedule, the call and how often it was made before,
// not on the order in which concurrent calls arrive, so that a schedule
// always injects the same faults.
type faults struct {
	mu       sync.Mutex
	schedule []byte
	on       bool
	calls    map[string]int
}

func newFaults(schedule []byte) *faults {
	return &faults{schedule: schedule, on: len(schedule) > 0, calls: make(map[string]int)}
}

func (f *faults) decide(call string) fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.on {
		return noFault
	}
	n := f.calls[call]
	f.calls[call]++
	h := xxhash.Sum64String(fmt.Sprintf("%s#%d", call, n))
	return fault(f.schedule[h%uint64(len(f.schedule))] % 3)
}

func (f *faults) stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.on = false
}

// faultyHot injects faults into the hand-off calls of a hot store.
type faultyHot struct {
	*storagetest.Hot
	f *faults
}

func refsKey(refs []storage.ChunkRef) string {
	if len(refs) == 0 {
		return ""
	}
	return fmt.Sprintf("%d/%d/%d", refs[0].Series, refs[0].Start, len(refs))
}

func (h *faultyHot) Claim(ctx context.Context, shard, limit int) ([]storage.ChunkRef, error) {
	switch h.f.decide(fmt.Sprintf("claim/%d", shard)) {
	case failBefore:
		return nil, errInjected
	case failAfter:
		_, _ = h.Hot.Claim(ctx, shard, limit)
		return nil, errInjected
	}
	return h.Hot.Claim(ctx, shard, limit)
}

func (h *faultyHot) Load(ctx context.Context, refs []storage.ChunkRef) (storage.LoadResult, error) {
	if h.f.decide("load/"+refsKey(refs)) != noFault {
		return storage.LoadResult{}, errInjected
	}
	return h.Hot.Load(ctx, refs)
}

func (h *faultyHot) Complete(ctx context.Context, chunks []storage.HotChunk) error {
	refs := make([]storage.ChunkRef, len(chunks))
	for i, c := range chunks {
		refs[i] = c.Ref
	}
	switch h.f.decide("complete/" + refsKey(refs)) {
	case failBefore:
		return errInjected
	case failAfter:
		_ = h.Hot.Complete(ctx, chunks)
		return errInjected
	}
	return h.Hot.Complete(ctx, chunks)
}

func (h *faultyHot) Release(ctx context.Context, refs []storage.ChunkRef) error {
	switch h.f.decide("release/" + refsKey(refs)) {
	case failBefore:
		return errInjected
	case failAfter:
		_ = h.Hot.Release(ctx, refs)
		return errInjected
	}
	return h.Hot.Release(ctx, refs)
}

// faultyCold injects faults into the writes of a cold store.
type faultyCold struct {
	*storagetest.Cold
	f *faults
}

func (c *faultyCold) WriteChunk(ctx context.Context, ch storage.ColdChunk) error {
	switch c.f.decide(fmt.Sprintf("write/%d/%d/%d", ch.Series, ch.Start, ch.Part)) {
	case failBefore:
		return errInjected
	case failAfter:
		_ = c.Cold.WriteChunk(ctx, ch)
		return errInjected
	}
	return c.Cold.WriteChunk(ctx, ch)
}

// Whatever calls of the hand-off fail, before or after taking effect, no
// sample is lost or changed, no row is stored twice, and once the faults stop
// every chunk reaches the cold tier and leaves the hot tier.
func FuzzFlushUnderFaults(f *testing.F) {
	f.Add([]byte{}, uint8(1))
	f.Add([]byte{1}, uint8(3))       // every call fails
	f.Add([]byte{2}, uint8(3))       // every reply is lost
	f.Add([]byte{0, 1, 2}, uint8(6)) // a third of each
	f.Add([]byte{0, 0, 0, 2, 0, 1, 0, 0, 2}, uint8(4))
	f.Fuzz(func(t *testing.T, schedule []byte, rounds uint8) {
		layout := model.Layout{Shards: 2, ChunkRange: time.Minute.Milliseconds(), Grace: 10 * time.Second.Milliseconds()}
		const lease = 30 * time.Second
		c := newClock()
		hot := storagetest.NewHot(layout, lease)
		hot.Now = c.Now
		cold := storagetest.NewCold(layout.ChunkRange)
		fl := newFaults(schedule)
		flusher := New(&faultyHot{Hot: hot, f: fl}, &faultyCold{Cold: cold, f: fl},
			Options{BatchSize: 3, Concurrency: 1, Lease: lease})
		ctx := context.Background()

		// Eight series in both shards, each with samples in two windows.
		written := make(map[model.SeriesID][]model.Sample)
		var batch []storage.SeriesSamples
		for i := range 8 {
			id := spreadID(i)
			s := []model.Sample{{T: c.ms(0), V: float64(i)}, {T: c.ms(time.Second), V: 1}, {T: c.ms(time.Minute), V: 2}}
			written[id] = chunk.Normalize(append([]model.Sample(nil), s...))
			batch = append(batch, storage.SeriesSamples{ID: id, Samples: s})
		}
		if res, err := hot.Append(ctx, batch); err != nil || res.Accepted != 24 {
			t.Fatalf("append: %+v %v", res, err)
		}
		chunks := hot.Len()
		c.Add(3 * time.Minute) // every window is closed

		for range rounds % 8 {
			flusher.FlushOnce(ctx)
			c.Add(lease + time.Second)
		}
		fl.stop()
		for round := 0; hot.Len() > 0 || hot.Inflight() > 0; round++ {
			if round == 10 {
				t.Fatalf("%d chunks and %d in-flight entries left after the faults stopped", hot.Len(), hot.Inflight())
			}
			flusher.FlushOnce(ctx)
			c.Add(lease + time.Second)
		}

		if cold.Len() != chunks {
			t.Fatalf("%d rows in the cold tier for %d chunks of one part", cold.Len(), chunks)
		}
		ids := make([]model.SeriesID, 0, len(written))
		for id := range written {
			ids = append(ids, id)
		}
		got, err := cold.Read(ctx, ids, c.ms(-time.Hour), c.ms(0))
		if err != nil {
			t.Fatal(err)
		}
		for id, want := range written {
			if fmt.Sprint(got[id]) != fmt.Sprint(want) {
				t.Fatalf("series %v: cold tier holds %v, want %v", id, got[id], want)
			}
		}
	})
}

// partFailingCold fails the first write of one part of every chunk.
type partFailingCold struct {
	*storagetest.Cold
	part   int
	mu     sync.Mutex
	failed map[[2]int64]bool
}

func (c *partFailingCold) WriteChunk(ctx context.Context, ch storage.ColdChunk) error {
	c.mu.Lock()
	key := [2]int64{int64(ch.Series), ch.Start}
	fail := ch.Part == c.part && !c.failed[key]
	if fail {
		c.failed[key] = true
	}
	c.mu.Unlock()
	if fail {
		return errInjected
	}
	return c.Cold.WriteChunk(ctx, ch)
}

// A chunk whose write fails after some of its parts were stored stays in
// the hot tier, and the retry stores every part once.
func TestPartialWriteIsRetried(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds()}
	c := newClock()
	hot := storagetest.NewHot(layout, time.Minute)
	hot.Now = c.Now
	cold := &partFailingCold{Cold: storagetest.NewCold(layout.ChunkRange), part: 1, failed: make(map[[2]int64]bool)}
	f := New(hot, cold, Options{BatchSize: 10, Concurrency: 1, Lease: time.Minute})
	ctx := context.Background()
	const n = samplesPerPart + 50_000 // two parts
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
	if hot.Len() != 1 || cold.Len() != 1 {
		t.Fatalf("after the failed write: %d hot chunks and %d cold rows, want 1 and 1", hot.Len(), cold.Len())
	}
	c.Add(2 * time.Minute) // the lease expires
	f.FlushOnce(ctx)
	if hot.Len() != 0 || cold.Len() != 2 {
		t.Fatalf("after the retry: %d hot chunks and %d cold rows, want 0 and 2", hot.Len(), cold.Len())
	}
	got, err := cold.Read(ctx, []model.SeriesID{3}, start, start+time.Hour.Milliseconds())
	if err != nil || len(got[3]) != n || got[3][n-1].V != n-1 {
		t.Fatalf("read back %d samples, %v", len(got[3]), err)
	}
}
