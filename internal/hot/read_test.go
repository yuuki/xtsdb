package hot

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// countHook counts the commands sent to Redis, including pipelined ones.
type countHook struct{ n atomic.Int64 }

func (h *countHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *countHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}

func (h *countHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.n.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// appendSeries writes one sample per series at t and returns the IDs.
func appendSeries(tb testing.TB, s *Store, n int, t int64) []model.SeriesID {
	tb.Helper()
	ids := make([]model.SeriesID, n)
	batch := make([]storage.SeriesSamples, n)
	for i := range ids {
		ids[i] = model.SeriesID(uint64(i)*0x9e3779b97f4a7c15 + 1)
		batch[i] = storage.SeriesSamples{ID: ids[i], Samples: []model.Sample{{T: t, V: float64(i)}}}
	}
	if res, err := s.Append(context.Background(), batch); err != nil || res.Accepted != n {
		tb.Fatalf("append: %+v %v", res, err)
	}
	return ids
}

// A read issues one command per series and one per existing chunk, however
// long the range and however short the chunk range.
func TestReadCommandsFollowExistingChunks(t *testing.T) {
	layout := model.Layout{Shards: 16, ChunkRange: time.Minute.Milliseconds(), Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	now := nowMs()
	const n = 100
	ids := appendSeries(t, s, n, now)
	// Half of the series get a second chunk in the next window.
	var batch []storage.SeriesSamples
	for _, id := range ids[:n/2] {
		batch = append(batch, storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: now + layout.ChunkRange, V: 1}}})
	}
	if _, err := s.Append(ctx, batch); err != nil {
		t.Fatal(err)
	}

	hook := &countHook{}
	client.AddHook(hook)
	got, err := s.Read(ctx, ids, now-6*time.Hour.Milliseconds(), now+2*layout.ChunkRange)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n || len(got[ids[0]]) != 2 || len(got[ids[n-1]]) != 1 {
		t.Fatalf("read %d series (%v, %v)", len(got), got[ids[0]], got[ids[n-1]])
	}
	// Before the window index, this read issued 363 GETs per series.
	if cmds, want := hook.n.Load(), int64(n+n+n/2); cmds != want {
		t.Fatalf("read issued %d commands, want %d (one per series and one per chunk)", cmds, want)
	}
}

// The window index lists exactly the windows a series has in Redis.
func TestWindowIndexIsMaintained(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	id := model.SeriesID(5)
	windows := func() []string {
		return client.ZRange(ctx, s.windowsKey(0, id), 0, -1).Val()
	}
	now := nowMs()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now, V: 1}, {T: now + 1, V: 2}}}}); err != nil {
		t.Fatal(err)
	}
	start := layout.WindowStart(id, now)
	if got := windows(); len(got) != 1 || got[0] != strconv.FormatInt(start, 10) {
		t.Fatalf("window index %v, want [%d]", got, start)
	}
	refs := waitClaim(t, s, 0)
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("load: %v %v", chunks, err)
	}
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if got := windows(); len(got) != 0 || client.Exists(ctx, s.windowsKey(0, id)).Val() != 0 {
		t.Fatalf("completed window still indexed: %v", got)
	}

	// Quarantined and released chunks leave the index too.
	for _, corrupt := range []bool{true, false} {
		now := nowMs()
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now, V: 1}}}}); err != nil {
			t.Fatal(err)
		}
		refs := waitClaim(t, s, 0)
		key := s.chunkKey(0, member(refs[0].Series, refs[0].Start))
		if corrupt {
			client.Append(ctx, key, "x")
		} else {
			client.Del(ctx, key)
		}
		res, err := s.Load(ctx, refs)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Release(ctx, res.Missing); err != nil {
			t.Fatal(err)
		}
		if got := windows(); len(got) != 0 {
			t.Fatalf("corrupt=%v: window still indexed: %v", corrupt, got)
		}
	}
}

// Chunks written before the window index existed are registered once when
// a store is opened.
func TestWindowIndexIsBuiltForExistingChunks(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	now := nowMs()
	ids := appendSeries(t, s, 20, now)
	// Forget the index, as with chunks written by an earlier version.
	for _, id := range ids {
		client.Del(ctx, s.windowsKey(layout.Shard(id), id))
	}
	client.HDel(ctx, s.prefix+":meta", windowIndexField)
	if got, _ := s.Read(ctx, ids, now-1, now+1); len(got) != 0 {
		t.Fatalf("precondition: unindexed chunks are not read, got %d series", len(got))
	}
	s2, err := New(ctx, client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s2.Read(ctx, ids, now-1, now+1); err != nil || len(got) != len(ids) {
		t.Fatalf("read %d series after the index was built, want %d (%v)", len(got), len(ids), err)
	}
}

// BenchmarkRead reads 1000 series with one chunk each over a six hour range,
// the default querier.hot_lookback.
func BenchmarkRead(b *testing.B) {
	for _, chunkRange := range []time.Duration{time.Minute, time.Hour} {
		b.Run(fmt.Sprintf("chunk_range=%s", chunkRange), func(b *testing.B) {
			layout := model.Layout{Shards: 256, ChunkRange: chunkRange.Milliseconds(), Grace: 0}
			s, client := newTestStore(b, layout, time.Minute)
			now := nowMs()
			ids := appendSeries(b, s, 1000, now)
			hook := &countHook{}
			client.AddHook(hook)
			ctx := context.Background()
			b.ResetTimer()
			for b.Loop() {
				got, err := s.Read(ctx, ids, now-6*time.Hour.Milliseconds(), now)
				if err != nil || len(got) != len(ids) {
					b.Fatalf("read %d series: %v", len(got), err)
				}
			}
			b.ReportMetric(float64(hook.n.Load())/float64(b.N), "cmds/op")
		})
	}
}
