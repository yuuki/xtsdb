package storagetest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// This file holds the conformance tests of the persistent tier and the
// index: the semantics every backend must provide, whatever its data model.
// Backends run them from their own tests with a factory for empty stores,
// and add tests of their storage format (TTL values, partitioning) next to
// them.

// ColdFactory returns an empty cold store that shares no data with the
// stores of other calls. chunkRange is the width of a chunk window in
// milliseconds; retention 0 keeps chunks forever.
type ColdFactory func(t *testing.T, retention time.Duration, chunkRange int64) storage.ColdStore

// IndexStore is both sides of the series index.
type IndexStore = storage.Index

// IndexFactory returns an empty index that shares no data with the indexes
// of other calls. retention 0 keeps entries forever.
type IndexFactory func(t *testing.T, retention time.Duration) IndexStore

const hour = int64(time.Hour / time.Millisecond)

// RunColdStoreTests runs the conformance tests of storage.ColdStore.
func RunColdStoreTests(t *testing.T, newStore ColdFactory) {
	t.Run("ReadWrite", func(t *testing.T) { testColdReadWrite(t, newStore) })
	t.Run("VersionsAndParts", func(t *testing.T) { testColdVersionsAndParts(t, newStore) })
	t.Run("VersionPrecedence", func(t *testing.T) { testColdVersionPrecedence(t, newStore) })
	t.Run("ManyWindows", func(t *testing.T) { testColdManyWindows(t, newStore) })
	t.Run("LargePart", func(t *testing.T) { testColdLargePart(t, newStore) })
	t.Run("Histograms", func(t *testing.T) { testColdHistograms(t, newStore) })
	t.Run("Retention", func(t *testing.T) { testColdRetention(t, newStore) })
}

// RunIndexTests runs the conformance tests of storage.IndexWriter and
// storage.IndexReader.
func RunIndexTests(t *testing.T, newIndex IndexFactory) {
	t.Run("ReadWrite", func(t *testing.T) { testIndexReadWrite(t, newIndex) })
	t.Run("ManySeries", func(t *testing.T) { testIndexManySeries(t, newIndex) })
	t.Run("Strings", func(t *testing.T) { testIndexStrings(t, newIndex) })
	t.Run("Collisions", func(t *testing.T) { testIndexCollisions(t, newIndex) })
	t.Run("Retention", func(t *testing.T) { testIndexRetention(t, newIndex) })
}

// writePart writes samples as one part of a version.
func writePart(t *testing.T, store storage.ColdStore, c storage.ColdChunk, samples []model.Sample) {
	t.Helper()
	enc, data, err := chunk.Encode(samples)
	if err != nil {
		t.Fatal(err)
	}
	c.MaxT, c.Count, c.Encoding, c.Data = samples[len(samples)-1].T, len(samples), enc, data
	if err := store.WriteChunk(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, store storage.ColdStore, ids []model.SeriesID, mint, maxt int64) map[model.SeriesID][]model.Sample {
	t.Helper()
	got, err := store.Read(context.Background(), ids, mint, maxt)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// equalSamples compares samples, histograms included.
func equalSamples(a, b []model.Sample) bool {
	return slices.EqualFunc(a, b, func(x, y model.Sample) bool {
		if x.T != y.T || math.Float64bits(x.V) != math.Float64bits(y.V) || (x.H == nil) != (y.H == nil) {
			return false
		}
		return x.H == nil || x.H.Equals(y.H)
	})
}

func expectSamples(t *testing.T, what string, got, want []model.Sample) {
	t.Helper()
	if !equalSamples(got, want) {
		if len(got) > 20 || len(want) > 20 {
			t.Fatalf("%s: got %d samples, want %d", what, len(got), len(want))
		}
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func testColdReadWrite(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 0, hour)
	ctx := context.Background()
	id := model.SeriesID(0x8000000000000001) // the high bit set
	// The window straddles a day boundary: it starts on day N and ends on N+1.
	start := 20_000*model.DayMillis - hour/2
	first := []model.Sample{{T: start + 1000, V: 1}, {T: start + 2000, V: 2}, {T: start + hour - 1, V: 3}}
	writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: 100, Total: 3}, first)
	// A retried write must not duplicate data.
	writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: 100, Total: 3}, first)
	// A later fragment of the same window overrides overlapping samples.
	writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: 200, Total: 2},
		[]model.Sample{{T: start + 2000, V: 20}, {T: start + 2500, V: 25}})
	// A series whose ID differs only in the low byte.
	other := model.SeriesID(0x8000000000000002)
	writePart(t, store, storage.ColdChunk{Series: other, Start: start, Fragment: 100, Total: 1},
		[]model.Sample{{T: start + 1000, V: 7}})

	// The query starts after the chunk start: the store must look back.
	got := read(t, store, []model.SeriesID{id, 1, other}, start+1500, start+hour)
	expectSamples(t, "read", got[id], []model.Sample{{T: start + 2000, V: 20}, {T: start + 2500, V: 25}, {T: start + hour - 1, V: 3}})
	if _, ok := got[1]; ok {
		t.Fatal("an unknown series was returned")
	}
	if _, ok := got[other]; ok {
		t.Fatalf("a series without samples in the range was returned: %v", got[other])
	}
	got = read(t, store, []model.SeriesID{id, other}, start, start+1000)
	expectSamples(t, "range start", got[id], []model.Sample{{T: start + 1000, V: 1}})
	expectSamples(t, "other series", got[other], []model.Sample{{T: start + 1000, V: 7}})
	if got := read(t, store, []model.SeriesID{id}, start+hour, start+2*hour); len(got[id]) != 0 {
		t.Fatalf("expected no samples after the window, got %v", got[id])
	}
	if got := read(t, store, []model.SeriesID{id}, start-hour, start+999); len(got[id]) != 0 {
		t.Fatalf("expected no samples before the first one, got %v", got[id])
	}
	if got := read(t, store, nil, start, start+hour); len(got) != 0 {
		t.Fatalf("reading no series returned %v", got)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func testColdVersionsAndParts(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 0, hour)
	id := model.SeriesID(77)
	start := int64(20_000) * model.DayMillis
	c := func(total int, digest uint64, part int) storage.ColdChunk {
		return storage.ColdChunk{Series: id, Start: start, Fragment: 5, Total: total, Digest: digest, Part: part}
	}
	// A complete version in two parts...
	writePart(t, store, c(3, 0xaaaa, 0), []model.Sample{{T: start + 1, V: 1}, {T: start + 2, V: 2}})
	writePart(t, store, c(3, 0xaaaa, 1), []model.Sample{{T: start + 3, V: 3}})
	// ...and a stale version of the same incarnation with less data.
	writePart(t, store, c(1, 0xbbbb, 0), []model.Sample{{T: start + 1, V: 1}})
	got := read(t, store, []model.SeriesID{id}, start, start+hour)
	expectSamples(t, "read", got[id], []model.Sample{{T: start + 1, V: 1}, {T: start + 2, V: 2}, {T: start + 3, V: 3}})
}

// Versions of a window take precedence in (fragment, total, digest) order,
// with the digest compared as a signed 64-bit value.
func testColdVersionPrecedence(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 0, hour)
	start := int64(20_000) * model.DayMillis
	write := func(id model.SeriesID, frag int64, total int, digest uint64, v float64) {
		t.Helper()
		writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: frag, Total: total, Digest: digest},
			[]model.Sample{{T: start + 1, V: v}})
	}
	value := func(id model.SeriesID) float64 {
		t.Helper()
		got := read(t, store, []model.SeriesID{id}, start, start+hour)
		if len(got[id]) != 1 {
			t.Fatalf("read %v", got)
		}
		return got[id][0].V
	}
	// A later state of the same incarnation (more records) wins over an
	// earlier one, whatever the digests.
	write(1, 5, 3, 0x7fff_ffff_ffff_ffff, 1)
	write(1, 5, 4, 1, 2)
	if v := value(1); v != 2 {
		t.Fatalf("version with more records must win, got %v", v)
	}
	// A later incarnation wins over a longer earlier one.
	write(2, 5, 9, 1, 1)
	write(2, 6, 1, 1, 2)
	if v := value(2); v != 2 {
		t.Fatalf("later fragment must win, got %v", v)
	}
	// Equal (fragment, total): the digest as a signed value decides.
	write(3, 5, 3, 0x8000_0000_0000_0000, 1) // negative
	write(3, 5, 3, 1, 2)
	if v := value(3); v != 2 {
		t.Fatalf("larger signed digest must win, got %v", v)
	}
	// Imported versions (fragment 0) lose to flushed ones.
	write(4, 1_700_000_000_000_000, 1, 1, 2)
	write(4, 0, 5, 1, 1)
	if v := value(4); v != 2 {
		t.Fatalf("fragment 0 must lose, got %v", v)
	}
}

// A long range covers hundreds of windows, which backends may have to read
// in several requests.
func testColdManyWindows(t *testing.T, newStore ColdFactory) {
	const (
		minute  = int64(time.Minute / time.Millisecond)
		windows = 300
	)
	store := newStore(t, 0, minute)
	ctx := context.Background()
	base := 20_000*model.DayMillis - windows/2*minute // crosses a day boundary
	ids := []model.SeriesID{5, 6}
	want := make(map[model.SeriesID][]model.Sample)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, id := range ids {
		for w := range windows {
			start := base + int64(w)*minute
			samples := []model.Sample{{T: start + 1, V: float64(w)}, {T: start + minute/2, V: float64(id)*1000 + float64(w)}}
			want[id] = append(want[id], samples...)
			g.Go(func() error {
				enc, data, err := chunk.Encode(samples)
				if err != nil {
					return err
				}
				return store.WriteChunk(gctx, storage.ColdChunk{Series: id, Start: start, Fragment: 1, Total: 2,
					MaxT: samples[1].T, Count: 2, Encoding: enc, Data: data})
			})
		}
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	got := read(t, store, ids, base, base+windows*minute)
	for _, id := range ids {
		expectSamples(t, fmt.Sprintf("series %d", id), got[id], want[id])
	}
	mint, maxt := base+100*minute+minute/4, base+200*minute
	got = read(t, store, ids[:1], mint, maxt)
	expectSamples(t, "sub-range", got[ids[0]], chunk.Filter(want[ids[0]], mint, maxt))
}

// A part can be several hundred KiB, for example with large native
// histograms or incompressible values.
func testColdLargePart(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 0, hour)
	id := model.SeriesID(9)
	start := int64(20_000) * model.DayMillis
	rng := rand.New(rand.NewPCG(1, 2))
	samples := make([]model.Sample, 40_000)
	for i := range samples {
		samples[i] = model.Sample{T: start + int64(i)*50, V: rng.NormFloat64()}
	}
	enc, data, err := chunk.Encode(samples)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 256<<10 {
		t.Fatalf("the test needs a large part, got %d bytes", len(data))
	}
	c := storage.ColdChunk{Series: id, Start: start, Fragment: 1, Total: len(samples), Digest: 42,
		MaxT: samples[len(samples)-1].T, Count: len(samples), Encoding: enc, Data: data}
	for range 2 { // and idempotently
		if err := store.WriteChunk(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	// A small second part after it.
	writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: 1, Total: len(samples) + 1, Digest: 43},
		[]model.Sample{{T: start + hour - 1, V: 1}})
	got := read(t, store, []model.SeriesID{id}, start, start+hour)
	expectSamples(t, "large part", got[id], append(slices.Clone(samples), model.Sample{T: start + hour - 1, V: 1}))
}

func testColdHistograms(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 0, hour)
	id := model.SeriesID(10)
	start := int64(20_000) * model.DayMillis
	var samples []model.Sample
	for i := range 5 {
		samples = append(samples, model.Sample{T: start + int64(i)*1000, H: &histogram.FloatHistogram{
			Schema: 1, ZeroThreshold: 0.001, ZeroCount: float64(i), Count: float64(10 + 3*i), Sum: float64(i) * 1.5,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{float64(5 + i), float64(5 + 2*i)},
		}})
	}
	samples = append(samples, model.Sample{T: start + 10_000, V: 3}) // a float after the histograms
	writePart(t, store, storage.ColdChunk{Series: id, Start: start, Fragment: 1, Total: len(samples)}, samples)
	got := read(t, store, []model.SeriesID{id}, start, start+hour)
	expectSamples(t, "histograms", got[id], samples)
}

// Chunks are kept for the retention after their last sample; chunks that
// are already older than that are not stored.
func testColdRetention(t *testing.T, newStore ColdFactory) {
	store := newStore(t, 24*time.Hour, hour)
	now := time.Now()
	write := func(id model.SeriesID, maxT int64) {
		t.Helper()
		writePart(t, store, storage.ColdChunk{Series: id, Start: maxT - 1000, Fragment: 1, Total: 1},
			[]model.Sample{{T: maxT, V: 1}})
	}
	recent, expired := now.Add(-23*time.Hour).UnixMilli(), now.Add(-25*time.Hour).UnixMilli()
	write(1, recent)
	write(2, expired)
	got := read(t, store, []model.SeriesID{1, 2}, expired-hour, now.UnixMilli())
	expectSamples(t, "recent chunk", got[1], []model.Sample{{T: recent, V: 1}})
	if len(got[2]) != 0 {
		t.Fatalf("a chunk older than the retention was stored: %v", got[2])
	}
}

// writeIndex records a series as the ingester does.
func writeIndex(ctx context.Context, x IndexStore, day int32, lset labels.Labels) error {
	if err := x.WriteSeries(ctx, day, model.ID(lset), lset); err != nil {
		return err
	}
	var err error
	lset.Range(func(l labels.Label) {
		if err == nil {
			err = x.WriteLabelValue(ctx, day, l.Name, l.Value)
		}
		if err == nil {
			err = x.WriteLabelName(ctx, day, l.Name)
		}
	})
	return err
}

func postings(t *testing.T, x IndexStore, day int32, name, value string) []model.SeriesID {
	t.Helper()
	ids, err := x.Postings(context.Background(), day, name, value)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

func sortedIDs(ids ...model.SeriesID) []model.SeriesID {
	slices.Sort(ids)
	return ids
}

func testIndexReadWrite(t *testing.T, newIndex IndexFactory) {
	x := newIndex(t, 0)
	ctx := context.Background()
	const day = int32(20_000)
	a := labels.FromStrings("__name__", "up", "job", "api", "instance", "a:9090")
	b := labels.FromStrings("__name__", "up", "job", "db", "instance", "b:9090")
	for _, lset := range []labels.Labels{a, b, a} { // writes are idempotent
		if err := writeIndex(ctx, x, day, lset); err != nil {
			t.Fatal(err)
		}
	}
	if ids := postings(t, x, day, "__name__", "up"); !slices.Equal(ids, sortedIDs(model.ID(a), model.ID(b))) {
		t.Fatalf("postings = %v", ids)
	}
	if ids := postings(t, x, day, "job", "db"); !slices.Equal(ids, []model.SeriesID{model.ID(b)}) {
		t.Fatalf("postings of job=db = %v", ids)
	}
	if ids := postings(t, x, day, "job", "web"); len(ids) != 0 {
		t.Fatalf("postings of an unknown value = %v", ids)
	}
	if ids := postings(t, x, day+1, "__name__", "up"); len(ids) != 0 {
		t.Fatalf("postings must be scoped by day, got %v", ids)
	}
	vals, err := x.LabelValues(ctx, day, "job")
	if err != nil || fmt.Sprint(vals) != "[api db]" {
		t.Fatalf("label values = %v, %v", vals, err)
	}
	if vals, err := x.LabelValues(ctx, day-1, "job"); err != nil || len(vals) != 0 {
		t.Fatalf("label values must be scoped by day, got %v, %v", vals, err)
	}
	names, err := x.LabelNames(ctx, day)
	if err != nil || fmt.Sprint(names) != "[__name__ instance job]" {
		t.Fatalf("label names = %v, %v", names, err)
	}
	if names, err := x.LabelNames(ctx, day+1); err != nil || len(names) != 0 {
		t.Fatalf("label names must be scoped by day, got %v, %v", names, err)
	}
	ls, err := x.SeriesLabels(ctx, []model.SeriesID{model.ID(a), 12345})
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 || !labels.Equal(ls[model.ID(a)], a) {
		t.Fatalf("series labels = %v", ls)
	}
	if ls, err := x.SeriesLabels(ctx, nil); err != nil || len(ls) != 0 {
		t.Fatalf("labels of no series = %v, %v", ls, err)
	}
}

// Many series can share a label pair, and a label can have many values;
// backends may have to read them in several requests.
func testIndexManySeries(t *testing.T, newIndex IndexFactory) {
	x := newIndex(t, 0)
	ctx := context.Background()
	const (
		day    = int32(20_000)
		series = 300
	)
	var (
		ids  []model.SeriesID
		want = make(map[model.SeriesID]labels.Labels)
		vals []string
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for i := range series {
		v := fmt.Sprintf("host-%03d", i)
		lset := labels.FromStrings("__name__", "up", "instance", v)
		ids = append(ids, model.ID(lset))
		want[model.ID(lset)] = lset
		vals = append(vals, v)
		g.Go(func() error { return writeIndex(gctx, x, day, lset) })
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := postings(t, x, day, "__name__", "up"); !slices.Equal(got, sortedIDs(ids...)) {
		t.Fatalf("postings: got %d series, want %d", len(got), len(ids))
	}
	got, err := x.LabelValues(ctx, day, "instance")
	if err != nil || !slices.Equal(got, vals) {
		t.Fatalf("label values: got %d (%v), want %d sorted", len(got), err, len(vals))
	}
	ls, err := x.SeriesLabels(ctx, append(slices.Clone(ids), 1, 2, 3))
	if err != nil || len(ls) != series {
		t.Fatalf("series labels: got %d (%v), want %d", len(ls), err, series)
	}
	for id, lset := range want {
		if !labels.Equal(ls[id], lset) {
			t.Fatalf("labels of %s = %v, want %v", id, ls[id], lset)
		}
	}
}

// Label names and values are arbitrary UTF-8, NUL included. A lookup must
// match them exactly, not by prefix or by concatenation.
func testIndexStrings(t *testing.T, newIndex IndexFactory) {
	x := newIndex(t, 0)
	ctx := context.Background()
	const day = int32(20_000)
	sets := map[string]labels.Labels{
		"a":        labels.FromStrings("__name__", "m", "job", "a"),
		"ab":       labels.FromStrings("__name__", "m", "job", "ab"),
		"a\\0b":    labels.FromStrings("__name__", "m", "job", "a\x00b"),
		"a\\0":     labels.FromStrings("__name__", "m", "job", "a\x00"),
		"unicode":  labels.FromStrings("__name__", "m", "job", "日本 \x01ÿ"),
		"job_x=a":  labels.FromStrings("__name__", "m", "job_x", "a"),
		"jo=ba":    labels.FromStrings("__name__", "m", "jo", "ba"),
		"jo\\0b=a": labels.FromStrings("__name__", "m", "jo\x00b", "a"),
	}
	for _, lset := range sets {
		if err := writeIndex(ctx, x, day, lset); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name, value, want string
	}{
		{"job", "a", "a"},
		{"job", "ab", "ab"},
		{"job", "a\x00b", "a\\0b"},
		{"job", "a\x00", "a\\0"},
		{"job", "日本 \x01ÿ", "unicode"},
		{"job_x", "a", "job_x=a"},
		{"jo", "ba", "jo=ba"},
		{"jo\x00b", "a", "jo\\0b=a"},
	} {
		if got := postings(t, x, day, c.name, c.value); !slices.Equal(got, []model.SeriesID{model.ID(sets[c.want])}) {
			t.Errorf("postings of %q=%q = %v, want only the series %q", c.name, c.value, got, c.want)
		}
	}
	for _, c := range [][2]string{{"job", ""}, {"job", "a\x00\x00"}, {"jo", "b"}, {"jo", ""}, {"job\x00", "a"}} {
		if got := postings(t, x, day, c[0], c[1]); len(got) != 0 {
			t.Errorf("postings of %q=%q = %v, want none", c[0], c[1], got)
		}
	}
	vals, err := x.LabelValues(ctx, day, "job")
	if want := []string{"a", "a\x00", "a\x00b", "ab", "日本 \x01ÿ"}; err != nil || !slices.Equal(vals, want) {
		t.Errorf("label values = %q (%v), want %q", vals, err, want)
	}
	if vals, err := x.LabelValues(ctx, day, "jo"); err != nil || !slices.Equal(vals, []string{"ba"}) {
		t.Errorf("label values of jo = %q (%v)", vals, err)
	}
	names, err := x.LabelNames(ctx, day)
	if want := []string{"__name__", "jo", "jo\x00b", "job", "job_x"}; err != nil || !slices.Equal(names, want) {
		t.Errorf("label names = %q (%v), want %q", names, err, want)
	}
	id := model.ID(sets["jo\\0b=a"])
	if ls, err := x.SeriesLabels(ctx, []model.SeriesID{id}); err != nil || !labels.Equal(ls[id], sets["jo\\0b=a"]) {
		t.Errorf("series labels = %v (%v)", ls, err)
	}
}

func testIndexCollisions(t *testing.T, newIndex IndexFactory) {
	x := newIndex(t, 24*time.Hour)
	ctx := context.Background()
	day := model.Day(time.Now().UnixMilli())
	id := model.SeriesID(0x8000000000000042)
	a := labels.FromStrings("__name__", "up", "job", "api")
	b := labels.FromStrings("__name__", "up", "job", "db")
	if err := x.WriteSeries(ctx, day, id, a); err != nil {
		t.Fatal(err)
	}
	if err := x.WriteSeries(ctx, day, id, a); err != nil {
		t.Fatalf("the same series must be recorded again: %v", err)
	}
	if err := x.WriteSeries(ctx, day-1, id, a); err != nil {
		t.Fatalf("the same series must be recorded on another day: %v", err)
	}
	for _, d := range []int32{day, day + 1} {
		err := x.WriteSeries(ctx, d, id, b)
		var coll *storage.SeriesCollisionError
		if !errors.As(err, &coll) || !errors.Is(err, storage.ErrSeriesCollision) {
			t.Fatalf("expected a collision, got %v", err)
		}
		if coll.ID != id || !labels.Equal(coll.Existing, a) || !labels.Equal(coll.Labels, b) {
			t.Fatalf("unexpected collision %+v", coll)
		}
	}
	if ids := postings(t, x, day, "job", "db"); len(ids) != 0 {
		t.Fatalf("postings of the refused series were written: %v", ids)
	}
	if ids := postings(t, x, day-1, "job", "api"); !slices.Equal(ids, []model.SeriesID{id}) {
		t.Fatalf("postings of the owner = %v", ids)
	}
	ls, err := x.SeriesLabels(ctx, []model.SeriesID{id})
	if err != nil || !labels.Equal(ls[id], a) {
		t.Fatalf("the owner's labels must be kept: %v %v", ls, err)
	}
	// Concurrent writers of two label sets with the same ID: exactly one
	// owns it.
	id2 := model.SeriesID(0x43)
	errs := make([]error, 8)
	var g errgroup.Group
	for i := range errs {
		lset := a
		if i%2 == 1 {
			lset = b
		}
		g.Go(func() error {
			errs[i] = x.WriteSeries(ctx, day, id2, lset)
			return nil
		})
	}
	_ = g.Wait()
	ls, err = x.SeriesLabels(ctx, []model.SeriesID{id2})
	if err != nil || len(ls) != 1 {
		t.Fatalf("series labels after a race: %v %v", ls, err)
	}
	for i, err := range errs {
		lset := a
		if i%2 == 1 {
			lset = b
		}
		if owner := labels.Equal(ls[id2], lset); owner != (err == nil) {
			t.Fatalf("writer %d of %v: %v, but the owner is %v", i, lset, err, ls[id2])
		}
	}
}

// Index entries of a day are kept two days longer than its samples; entries
// of days whose samples have already expired are not stored.
func testIndexRetention(t *testing.T, newIndex IndexFactory) {
	x := newIndex(t, 24*time.Hour)
	ctx := context.Background()
	today := model.Day(time.Now().UnixMilli())
	for _, day := range []int32{today, today - 3, today - 4} {
		lset := labels.FromStrings("__name__", "up", "day", fmt.Sprint(today-day))
		if err := writeIndex(ctx, x, day, lset); err != nil {
			t.Fatal(err)
		}
	}
	for day, want := range map[int32]bool{today: true, today - 3: true, today - 4: false} {
		names, err := x.LabelNames(ctx, day)
		if err != nil || (len(names) == 2) != want {
			t.Errorf("day -%d: names %v (%v), want present=%v", today-day, names, err, want)
		}
		vals, err := x.LabelValues(ctx, day, "day")
		if err != nil || (len(vals) == 1) != want {
			t.Errorf("day -%d: values %v (%v), want present=%v", today-day, vals, err, want)
		}
		if ids := postings(t, x, day, "__name__", "up"); (len(ids) == 1) != want {
			t.Errorf("day -%d: postings %v, want present=%v", today-day, ids, want)
		}
	}
	old := labels.FromStrings("__name__", "up", "day", "4")
	if ls, err := x.SeriesLabels(ctx, []model.SeriesID{model.ID(old)}); err != nil || len(ls) != 0 {
		t.Errorf("a series written only for an expired day was stored: %v %v", ls, err)
	}
}
