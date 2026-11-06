package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// The in-memory stores are the reference of the conformance tests.
func TestColdConformance(t *testing.T) {
	RunColdStoreTests(t, func(_ *testing.T, retention time.Duration, chunkRange int64) storage.ColdStore {
		c := NewCold(chunkRange)
		c.Retention = retention
		return c
	})
}

func TestIndexConformance(t *testing.T) {
	RunIndexTests(t, func(_ *testing.T, retention time.Duration) IndexStore {
		x := NewIndex()
		x.Retention = retention
		return x
	})
}

// Hot.Compact follows the rules of the Redis store: once per interval per
// shard, never for chunks that close within storage.CompactMargin, and not
// beyond the record limit; the samples do not change.
func TestHotCompact(t *testing.T) {
	ctx := context.Background()
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: 0}
	now := time.UnixMilli(1_700_000_000_000)
	h := NewHot(layout, time.Minute)
	h.Now = func() time.Time { return now }
	h.MaxChunk = 4
	h.CompactMinRecords = 1
	// Series 1 closes in more than the margin, series 2 within it.
	open, closing := model.SeriesID(1), model.SeriesID(2)
	for layout.Due(layout.WindowStart(open, now.UnixMilli())) <= now.Add(storage.CompactMargin).UnixMilli() {
		open += 2
	}
	for layout.Due(layout.WindowStart(closing, now.UnixMilli())) > now.Add(storage.CompactMargin).UnixMilli() {
		closing += 2
	}
	t0 := now.UnixMilli()
	samples := []model.Sample{{T: t0, V: 1}, {T: t0 + 1, V: 2}, {T: t0, V: 3}}
	if _, err := h.Append(ctx, []storage.SeriesSamples{{ID: open, Samples: samples}, {ID: closing, Samples: samples}}); err != nil {
		t.Fatal(err)
	}
	res, err := h.Compact(ctx, 0, time.Minute)
	if err != nil || res.Skipped || res.Compacted != 1 {
		t.Fatalf("compaction: %+v %v", res, err)
	}
	if res, _ := h.Compact(ctx, 0, time.Minute); !res.Skipped {
		t.Fatalf("a second compaction within the interval must be skipped: %+v", res)
	}
	got, _ := h.Read(ctx, []model.SeriesID{open}, t0, t0+1)
	if len(got[open]) != 2 || got[open][0].V != 3 || got[open][1].V != 2 {
		t.Fatalf("samples changed: %v", got[open])
	}
	// Two more records make five, more than MaxChunk: they stay raw.
	if _, err := h.Append(ctx, []storage.SeriesSamples{{ID: open, Samples: samples[:2]}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if res, _ := h.Compact(ctx, 0, time.Minute); res.Skipped || res.Compacted != 0 {
		t.Fatalf("compaction beyond the record limit: %+v", res)
	}
	if n := h.CompactionsDone(); n != 1 {
		t.Fatalf("%d compactions, want 1", n)
	}
}

// Chunks with too few records or with native histograms are not compacted.
func TestHotCompactLeavesSmallAndHistogramChunks(t *testing.T) {
	ctx := context.Background()
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds(), Grace: time.Hour.Milliseconds()}
	h := NewHot(layout, time.Minute)
	t0 := time.Now().UnixMilli()
	var small, full, hist []model.Sample
	for i := range storage.CompactMinRecords {
		full = append(full, model.Sample{T: t0 + int64(i), V: 1})
	}
	small = full[:storage.CompactMinRecords-1]
	hist = append(append([]model.Sample{}, full...), model.Sample{T: t0 - 1, H: &histogram.FloatHistogram{Count: 1, ZeroCount: 1}})
	if _, err := h.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: small}, {ID: 2, Samples: full}, {ID: 3, Samples: hist}}); err != nil {
		t.Fatal(err)
	}
	if res, err := h.Compact(ctx, 0, time.Minute); err != nil || res.Compacted != 1 {
		t.Fatalf("compaction: %+v %v", res, err)
	}
}
