//go:build !notikv && (!darwin || cgo)

package tikv

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

// newMemDB returns a DB on an in-memory RawKV.
func newMemDB(t *testing.T) (*DB, *memKV) {
	t.Helper()
	kv := newMemKV()
	return newDB(kv, "xt", time.Second), kv
}

// newTestDB returns a DB in a fresh namespace of the TiKV cluster in
// XTSDB_TEST_TIKV_PD (comma separated PD addresses), with the API version in
// XTSDB_TEST_TIKV_API_VERSION (v2 by default).
func newTestDB(t *testing.T) *DB {
	t.Helper()
	pd := os.Getenv("XTSDB_TEST_TIKV_PD")
	if pd == "" {
		t.Skip("XTSDB_TEST_TIKV_PD not set")
	}
	cfg := config.Default().TiKV
	cfg.PDEndpoints = strings.Split(pd, ",")
	if v := os.Getenv("XTSDB_TEST_TIKV_API_VERSION"); v != "" {
		cfg.APIVersion = v
	}
	prefix := fmt.Sprintf("xttest%08x", rand.Uint32())
	db, err := Open(context.Background(), cfg, prefix, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.DeleteAll(context.Background()); err != nil {
			t.Logf("delete test data: %v", err)
		}
		_ = db.Close()
	})
	return db
}

func runConformance(t *testing.T, newDB func(t *testing.T) *DB) {
	t.Run("Cold", func(t *testing.T) {
		storagetest.RunColdStoreTests(t, func(t *testing.T, retention time.Duration, chunkRange int64) storage.ColdStore {
			return NewChunkStore(newDB(t), retention, chunkRange, 8, nil)
		})
	})
	t.Run("Index", func(t *testing.T) {
		storagetest.RunIndexTests(t, func(t *testing.T, retention time.Duration) storagetest.IndexStore {
			return NewIndex(newDB(t), retention, 8)
		})
	})
}

func TestConformance(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		runConformance(t, func(t *testing.T) *DB {
			db, _ := newMemDB(t)
			return db
		})
	})
	// With a tiny read budget, every read takes one entry at a time.
	t.Run("MemoryPaged", func(t *testing.T) {
		runConformance(t, func(t *testing.T) *DB {
			db, _ := newMemDB(t)
			db.budget = 1
			return db
		})
	})
	t.Run("TiKV", func(t *testing.T) { runConformance(t, newTestDB) })
}

// The key encoding is part of the storage format.
func TestKeysGolden(t *testing.T) {
	k := newKeys("xt")
	for _, c := range []struct {
		got  []byte
		want string
	}{
		{k.meta("format"), "78743a6d666f726d6174"},
		{k.chunk(0x8000000000000001, segmentKey{start: 1, fragment: -1, total: 3, digest: math.MinInt64, part: 0, seg: 2}),
			"78743a63" + "8000000000000001" + "8000000000000001" + "7fffffffffffffff" + "8000000000000003" +
				"0000000000000000" + "8000000000000000" + "00000002"},
		{k.series(0xf28c8afcd224e55f), "78743a73f28c8afcd224e55f"},
		{k.posting(-1, "job", "a\x00", 42), "78743a70" + "4964" + "7fffffff" + "036a6f62" + "026100" + "000000000000002a"},
		{k.labelValue(20_000, "job", "api"), "78743a76" + "9f11" + "80004e20" + "036a6f62" + "03617069"},
		{k.labelName(20_000, "job"), "78743a6e" + "80004e20" + "036a6f62"},
	} {
		if got := fmt.Sprintf("%x", c.got); got != c.want {
			t.Errorf("key changed:\n got %s\nwant %s", got, c.want)
		}
	}
	lset := labels.FromStrings("__name__", "up", "job", "a\x00b")
	if got := fmt.Sprintf("%x", encodeSeries(lset, 1700000000000)); got != "01"+"0000018bcfe56800"+"085f5f6e616d655f5f027570036a6f6203610062" {
		t.Errorf("series encoding changed: %s", got)
	}
}

// Chunk keys sort like their fields, with the digest as a signed value: the
// order of precedence of versions (see storage.ColdChunk).
func TestChunkKeyOrder(t *testing.T) {
	k := newKeys("xt")
	rng := rand.New(rand.NewPCG(3, 4))
	pick := func(vals ...int64) int64 { return vals[rng.IntN(len(vals))] }
	ext := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	segs := make([]segmentKey, 500)
	for i := range segs {
		segs[i] = segmentKey{
			start: pick(append(ext, -5000, 5000)...), fragment: pick(ext...), total: pick(0, 1, 2, 1<<40),
			digest: pick(ext...), part: pick(0, 1, 7), seg: uint32(pick(0, 1, 1<<20)),
		}
	}
	byKey := slices.Clone(segs)
	slices.SortStableFunc(byKey, func(a, b segmentKey) int { return bytes.Compare(k.chunk(1, a), k.chunk(1, b)) })
	byFields := slices.Clone(segs)
	slices.SortStableFunc(byFields, func(a, b segmentKey) int {
		return cmp.Or(cmp.Compare(a.start, b.start), cmp.Compare(a.fragment, b.fragment), cmp.Compare(a.total, b.total),
			cmp.Compare(a.digest, b.digest), cmp.Compare(a.part, b.part), cmp.Compare(a.seg, b.seg))
	})
	if !slices.Equal(byKey, byFields) {
		t.Fatal("chunk keys do not sort like their fields")
	}
	for _, s := range segs {
		got, err := parseChunk(k.chunk(77, s), len(k.chunkSeries(77)))
		if err != nil || got != s {
			t.Fatalf("parse %+v: %+v %v", s, got, err)
		}
	}
	// The read range of a series covers exactly the windows it asks for.
	if bytes.Compare(k.chunkFrom(1, math.MinInt64), k.chunkSeries(1)) < 0 ||
		bytes.Compare(k.chunkUntil(1, math.MaxInt64), k.chunkSeries(2)) > 0 {
		t.Fatal("the read range of a series leaks into its neighbours")
	}
}

func TestLabelsEncoding(t *testing.T) {
	for _, lset := range []labels.Labels{
		labels.EmptyLabels(),
		labels.FromStrings("__name__", "up"),
		labels.FromStrings("__name__", "m", "a\x00", "\x00b", "日本", strings.Repeat("x", model.MaxLabelLength)),
	} {
		got, expires, err := decodeSeries(encodeSeries(lset, -5))
		if err != nil || !labels.Equal(got, lset) || expires != -5 {
			t.Fatalf("round trip of %v: %v %d %v", lset, got, expires, err)
		}
	}
	header := []byte{formatVersion, 0, 0, 0, 0, 0, 0, 0, 0}
	for _, bad := range [][]byte{nil, {0}, header[:5], append(header, 5, 'a'), append(header, 1, 'a')} {
		if _, _, err := decodeSeries(bad); err == nil {
			t.Fatalf("decoded malformed labels %x", bad)
		}
	}
}

// The largest index keys stay below TiKV's 8 KiB key limit.
func TestLargestKeysFit(t *testing.T) {
	db, kv := newMemDB(t)
	db.keys = newKeys(strings.Repeat("p", 64))
	x := NewIndex(db, 0, 8)
	ctx := context.Background()
	long := strings.Repeat("\x00", model.MaxLabelLength)
	b := labels.NewScratchBuilder(model.MaxLabelsPerSeries)
	b.Add("__name__", long)
	for i := range model.MaxLabelsPerSeries - 1 {
		b.Add(fmt.Sprintf("%0*d", model.MaxLabelLength, i), long)
	}
	b.Sort()
	lset := b.Labels()
	if err := x.WriteSeries(ctx, 20_000, model.ID(lset), lset); err != nil {
		t.Fatal(err)
	}
	if err := x.WriteLabelValue(ctx, 20_000, long, long); err != nil {
		t.Fatal(err)
	}
	if err := x.WriteLabelName(ctx, 20_000, long); err != nil {
		t.Fatal(err)
	}
	if ls, err := x.SeriesLabels(ctx, []model.SeriesID{model.ID(lset)}); err != nil || !labels.Equal(ls[model.ID(lset)], lset) {
		t.Fatalf("series labels: %v", err)
	}
	if ids, err := x.Postings(ctx, 20_000, "__name__", long); err != nil || len(ids) != 1 {
		t.Fatalf("postings: %v %v", ids, err)
	}
	// The series, its postings, the value and the name.
	if len(kv.Keys(nil)) != 3+model.MaxLabelsPerSeries {
		t.Fatalf("unexpected entries: %d", len(kv.Keys(nil)))
	}
}

// Reads never ask for more than TiKV sends in one response, however large
// the parts are.
func TestReadsFitInResponses(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		db, kv := newMemDB(t)
		testReadsFitInResponses(t, db)
		if kv.Scans < 2 {
			t.Fatalf("14 MiB were read in %d scans", kv.Scans)
		}
	})
	t.Run("TiKV", func(t *testing.T) { testReadsFitInResponses(t, newTestDB(t)) })
}

func testReadsFitInResponses(t *testing.T, db *DB) {
	const minute = int64(time.Minute / time.Millisecond)
	store := NewChunkStore(db, 0, minute, 8, nil)
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(5, 6))
	start := int64(20_000) * model.DayMillis
	var want []model.Sample
	const windows = 200 // about 14 MiB in total
	for w := range windows {
		ws := start + int64(w)*minute
		samples := make([]model.Sample, 9000) // incompressible: several segments
		for i := range samples {
			samples[i] = model.Sample{T: ws + int64(i), V: rng.NormFloat64()}
		}
		enc, b, err := chunk.Encode(samples)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) <= segmentSize {
			t.Fatalf("the test needs parts larger than a segment, got %d bytes", len(b))
		}
		if err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: ws, Fragment: 1, Total: len(samples),
			MaxT: samples[len(samples)-1].T, Count: len(samples), Encoding: enc, Data: b}); err != nil {
			t.Fatal(err)
		}
		want = append(want, samples...)
	}
	got, err := store.Read(ctx, []model.SeriesID{1}, start, start+windows*minute)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.EqualFunc(got[1], want, func(a, b model.Sample) bool { return a.T == b.T && a.V == b.V }) {
		t.Fatalf("got %d samples, want %d", len(got[1]), len(want))
	}
}

// A part larger than a segment is split, and a part that is missing a
// segment, for example after a write that failed half way, is skipped.
func TestSegments(t *testing.T) {
	db, kv := newMemDB(t)
	store := NewChunkStore(db, 0, int64(time.Hour/time.Millisecond), 8, nil)
	ctx := context.Background()
	start := int64(20_000) * model.DayMillis
	rng := rand.New(rand.NewPCG(7, 8))
	big := make([]model.Sample, 30_000)
	for i := range big {
		big[i] = model.Sample{T: start + int64(i), V: rng.NormFloat64()}
	}
	write := func(part int, samples []model.Sample) {
		t.Helper()
		enc, data, err := chunk.Encode(samples)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: start, Fragment: 1, Total: 30_001, Part: part,
			MaxT: samples[len(samples)-1].T, Count: len(samples), Encoding: enc, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	write(0, big)
	tail := []model.Sample{{T: start + 50_000, V: 1}}
	write(1, tail)
	keys := kv.Keys(db.keys.chunkSeries(1))
	if len(keys) < 4 {
		t.Fatalf("a large part must be split, got %d keys", len(keys))
	}
	read := func() []model.Sample {
		t.Helper()
		got, err := store.Read(ctx, []model.SeriesID{1}, start, start+60_000)
		if err != nil {
			t.Fatal(err)
		}
		return got[1]
	}
	if got := read(); len(got) != len(big)+1 {
		t.Fatalf("got %d samples, want %d", len(got), len(big)+1)
	}
	kv.Delete(keys[1])
	if got := read(); !slices.EqualFunc(got, tail, func(a, b model.Sample) bool { return a.T == b.T && a.V == b.V }) {
		t.Fatalf("an incomplete part must be skipped, got %d samples", len(got))
	}
	kv.Delete(keys[len(keys)-2]) // the last segment of part 0
	kv.Delete(keys[0])
	if got := read(); len(got) != 1 {
		t.Fatalf("an incomplete part must be skipped, got %d samples", len(got))
	}
}

// Entries expire storage.retention after the time of their data, not after
// they were written, so that imported history expires like recent data.
func TestTTLs(t *testing.T) {
	db, kv := newMemDB(t)
	ctx := context.Background()
	const hour = int64(time.Hour / time.Millisecond)
	now := time.Now()
	store := NewChunkStore(db, 24*time.Hour, hour, 8, nil)
	maxT := now.Add(-23 * time.Hour).UnixMilli()
	if err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: maxT - 1000, MaxT: maxT, Count: 1,
		Encoding: chunk.EncodingXOR, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	keys := kv.Keys(db.keys.chunkSeries(1))
	if ttl, ok := kv.TTL(keys[0]); len(keys) != 1 || !ok || ttl <= 0 || ttl > time.Hour {
		t.Fatalf("a chunk 23h old with 24h retention must expire within an hour, TTL %v", ttl)
	}

	x := NewIndex(db, 24*time.Hour, 8)
	today := model.Day(now.UnixMilli())
	if err := x.WriteLabelName(ctx, today-3, "job"); err != nil {
		t.Fatal(err)
	}
	if ttl, _ := kv.TTL(db.keys.labelName(today-3, "job")); ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("the entries of three days ago must expire within a day, TTL %v", ttl)
	}
	lset := labels.FromStrings("__name__", "up")
	if err := x.WriteSeries(ctx, today, model.ID(lset), lset); err != nil {
		t.Fatal(err)
	}
	if ttl, _ := kv.TTL(db.keys.series(model.ID(lset))); ttl < 72*time.Hour-time.Minute || ttl > 72*time.Hour {
		t.Fatalf("a series must expire the retention plus two days after it is written, TTL %v", ttl)
	}
	if ttl, _ := kv.TTL(db.keys.posting(today, "__name__", "up", model.ID(lset))); ttl <= 72*time.Hour-2*time.Second || ttl > 96*time.Hour {
		t.Fatalf("postings of today must expire the retention plus two days after the day, TTL %v", ttl)
	}

	// Without a retention, nothing expires.
	forever := NewIndex(db, 0, 8)
	if err := forever.WriteLabelName(ctx, 1, "job"); err != nil {
		t.Fatal(err)
	}
	if ttl, ok := kv.TTL(db.keys.labelName(1, "job")); !ok || ttl != 0 {
		t.Fatalf("entries must not expire without a retention, TTL %v %v", ttl, ok)
	}
}

func testCheckLayout(t *testing.T, db *DB) {
	ctx := context.Background()
	if err := db.CheckLayout(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckLayout(ctx, time.Hour); err != nil {
		t.Fatalf("same layout: %v", err)
	}
	if err := db.CheckLayout(ctx, time.Minute); err == nil || !strings.Contains(err.Error(), "chunk_range") {
		t.Fatalf("expected a chunk_range mismatch, got %v", err)
	}
	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteAll(t *testing.T) {
	db, kv := newMemDB(t)
	other := newDB(kv, "xt2", 0)
	ctx := context.Background()
	for _, d := range []*DB{db, other} {
		if err := NewIndex(d, 0, 1).WriteLabelName(ctx, 1, "job"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteAll(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := kv.Keys(nil); len(keys) != 1 || !bytes.HasPrefix(keys[0], []byte("xt2:")) {
		t.Fatalf("only the namespace must be deleted: %q", keys)
	}
}

func TestCheckLayout(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		db, _ := newMemDB(t)
		testCheckLayout(t, db)
	})
	t.Run("TiKV", func(t *testing.T) { testCheckLayout(t, newTestDB(t)) })
}

// A series entry that cannot be decoded is reported, not taken for a
// collision.
func TestMalformedSeriesEntry(t *testing.T) {
	db, kv := newMemDB(t)
	x := NewIndex(db, 0, 8)
	ctx := context.Background()
	lset := labels.FromStrings("__name__", "up")
	if err := kv.PutWithTTL(ctx, db.keys.series(model.ID(lset)), []byte{9, 9}, 0); err != nil {
		t.Fatal(err)
	}
	if err := x.WriteSeries(ctx, 20_000, model.ID(lset), lset); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("expected an error about the malformed entry, got %v", err)
	}
	if _, err := x.SeriesLabels(ctx, []model.SeriesID{model.ID(lset)}); err == nil {
		t.Fatal("expected an error reading a malformed entry")
	}
}

// Large lookups are read in batches that fit in a response.
func TestSeriesLabelsBatches(t *testing.T) {
	db, kv := newMemDB(t)
	x := NewIndex(db, 0, 4)
	ctx := context.Background()
	var ids []model.SeriesID
	for i := range 1000 {
		lset := labels.FromStrings("__name__", "up", "i", fmt.Sprint(i))
		ids = append(ids, model.ID(lset), model.ID(lset)) // duplicates are read once
		if err := x.WriteSeries(ctx, 20_000, model.ID(lset), lset); err != nil {
			t.Fatal(err)
		}
	}
	ls, err := x.SeriesLabels(ctx, ids)
	if err != nil || len(ls) != 1000 {
		t.Fatalf("got %d series (%v)", len(ls), err)
	}
	batch := db.scanLimit(len(db.keys.series(0)), maxSeriesValue)
	if batch < 16 || batch*maxSeriesValue > scanBudget {
		t.Fatalf("batch of %d series", batch)
	}
	if want := (1000 + batch - 1) / batch; kv.BatchGets != want {
		t.Fatalf("%d batches, want %d", kv.BatchGets, want)
	}
}

func TestTTLSeconds(t *testing.T) {
	now := time.Now()
	if ttl, ok := ttlSeconds(0, now.Add(-time.Hour), now); !ok || ttl != 0 {
		t.Fatal("retention 0 must keep entries forever")
	}
	if _, ok := ttlSeconds(time.Hour, now.Add(time.Second/2), now); ok {
		t.Fatal("an entry expiring within a second must not be written")
	}
	if ttl, ok := ttlSeconds(time.Hour, now.Add(90*time.Second), now); !ok || ttl != 90 {
		t.Fatalf("TTL %d", ttl)
	}
}

func TestOpenFails(t *testing.T) {
	cfg := config.Default().TiKV
	cfg.PDEndpoints = []string{"127.0.0.1:1"}
	cfg.ConnectTimeout = config.Duration(500 * time.Millisecond)
	start := time.Now()
	if _, err := Open(context.Background(), cfg, "xt", nil); err == nil {
		t.Fatal("expected an error")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("connecting took %v despite a connect timeout of 500ms", d)
	}
	cfg.APIVersion = "v3"
	if _, err := Open(context.Background(), cfg, "xt", nil); err == nil || !strings.Contains(err.Error(), "api_version") {
		t.Fatalf("expected an api_version error, got %v", err)
	}
}

// The clients' logs go to slog, with info demoted to debug.
func TestLogRedirect(t *testing.T) {
	var buf bytes.Buffer
	core := &slogCore{logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	l := zap.New(core).With(zap.String("component", "pd"))
	l.Info("region cache updated", zap.Int("regions", 3))
	l.Warn("pd leader changed", zap.String("leader", "pd-1"))
	l.Debug("dropped")
	out := buf.String()
	for _, want := range []string{
		`level=DEBUG msg="region cache updated" component=pd regions=3`,
		`level=WARN msg="pd leader changed" component=pd leader=pd-1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "dropped") {
		t.Errorf("zap debug logs must be dropped at slog debug level:\n%s", out)
	}
	quiet := &slogCore{logger: slog.New(slog.NewTextHandler(&buf, nil))}
	if quiet.Enabled(zapcore.InfoLevel) || !quiet.Enabled(zapcore.WarnLevel) {
		t.Error("info logs must be enabled only when slog debug logs are")
	}
}

// A part written again in fewer segments, by a build that encodes it
// differently, leaves the old trailing segments behind: they are ignored.
// Segments of different writes are never assembled together.
func TestPartRewrittenInFewerSegments(t *testing.T) {
	db, kv := newMemDB(t)
	store := NewChunkStore(db, 0, int64(time.Hour/time.Millisecond), 8, nil)
	ctx := context.Background()
	start := int64(20_000) * model.DayMillis
	rng := rand.New(rand.NewPCG(9, 10))
	write := func(samples []model.Sample) {
		t.Helper()
		enc, data, err := chunk.Encode(samples)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: start, Fragment: 1, Total: 7, Digest: 7,
			MaxT: samples[len(samples)-1].T, Count: len(samples), Encoding: enc, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	read := func() []model.Sample {
		t.Helper()
		got, err := store.Read(ctx, []model.SeriesID{1}, start, start+60_000)
		if err != nil {
			t.Fatal(err)
		}
		return got[1]
	}
	large := make([]model.Sample, 6000)
	for i := range large {
		large[i] = model.Sample{T: start + int64(i), V: rng.NormFloat64()}
	}
	write(large)
	before := len(kv.Keys(db.keys.chunkSeries(1)))
	small := []model.Sample{{T: start + 1, V: 1}, {T: start + 2, V: 2}}
	write(small)
	keys := kv.Keys(db.keys.chunkSeries(1))
	if before < 3 || len(keys) != before {
		t.Fatalf("the test needs leftover segments: %d keys before, %d after", before, len(keys))
	}
	if got := read(); len(got) != 2 || got[1].V != 2 {
		t.Fatalf("the rewritten part must be read, got %d samples", len(got))
	}

	// Write the large part again, then replace its second segment with one
	// that has the same header but other data, as a write by another build
	// that failed half way could leave: the checksum rejects the part.
	write(large)
	v, err := kv.Get(ctx, keys[1])
	if err != nil {
		t.Fatal(err)
	}
	v[len(v)-1] ^= 0xff
	if err := kv.PutWithTTL(ctx, keys[1], v, 0); err != nil {
		t.Fatal(err)
	}
	if got := read(); len(got) != 0 {
		t.Fatalf("a part with mixed segments must be skipped, got %d samples", len(got))
	}
}

// Reads look back a whole chunk range, also near the smallest timestamp.
func TestLookBackAtTheEdges(t *testing.T) {
	db, _ := newMemDB(t)
	const r = 1000
	store := NewChunkStore(db, 0, r, 8, nil)
	ctx := context.Background()
	for _, c := range []struct {
		name         string
		start, t     int64
		mint, maxint int64
	}{
		{"min", math.MinInt64, math.MinInt64 + 5, math.MinInt64, math.MinInt64 + 10},
		{"min+r-1", math.MinInt64 + 10, math.MinInt64 + 1005, math.MinInt64 + r - 1, math.MinInt64 + 2000},
		{"min+r", math.MinInt64 + 10, math.MinInt64 + 1005, math.MinInt64 + r, math.MinInt64 + 2000},
		{"max", math.MaxInt64 - 500, math.MaxInt64 - 1, math.MaxInt64 - 10, math.MaxInt64},
	} {
		id := model.SeriesID(rand.Uint64())
		samples := []model.Sample{{T: c.t, V: 1}}
		enc, data, err := chunk.Encode(samples)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.WriteChunk(ctx, storage.ColdChunk{Series: id, Start: c.start, Fragment: 1, Total: 1,
			MaxT: c.t, Count: 1, Encoding: enc, Data: data}); err != nil {
			t.Fatal(err)
		}
		got, err := store.Read(ctx, []model.SeriesID{id}, c.mint, c.maxint)
		if err != nil || len(got[id]) != 1 || got[id][0].T != c.t {
			t.Errorf("%s: got %v (%v)", c.name, got[id], err)
		}
	}
}

// failingPuts fails the next PutWithTTL calls, after a compare-and-swap
// succeeded, as a timeout or a crash between the two would.
type failingPuts struct {
	*memKV
	fail int
}

func (f *failingPuts) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, opts ...rawkv.RawOption) error {
	if f.fail > 0 {
		f.fail--
		return errors.New("injected failure")
	}
	return f.memKV.PutWithTTL(ctx, key, value, ttl, opts...)
}

// Ownership of a series ID goes by the expiry time recorded in the entry,
// so an entry whose TTL could not be set still expires, and an expired
// entry can be claimed by other labels.
func TestSeriesEntryExpiry(t *testing.T) {
	kv := &failingPuts{memKV: newMemKV(), fail: 1}
	db := newDB(kv, "xt", time.Second)
	now := time.Now()
	x := NewIndex(db, 24*time.Hour, 8)
	x.now = func() time.Time { return now }
	ctx := context.Background()
	id := model.SeriesID(0x77)
	a := labels.FromStrings("__name__", "up", "job", "a")
	b := labels.FromStrings("__name__", "up", "job", "b")
	day := model.Day(now.UnixMilli())
	if err := x.WriteSeries(ctx, day, id, a); err == nil {
		t.Fatal("the injected failure must be reported")
	}
	if ttl, ok := kv.TTL(db.keys.series(id)); !ok || ttl != 0 {
		t.Fatalf("the test needs an entry without TTL: %v %v", ttl, ok)
	}
	if err := x.WriteSeries(ctx, day, id, b); !errors.Is(err, storage.ErrSeriesCollision) {
		t.Fatalf("a live entry must be kept: %v", err)
	}

	// Three days later the entry has expired, although it has no TTL.
	now = now.Add(72*time.Hour + time.Minute)
	day = model.Day(now.UnixMilli())
	if ls, err := x.SeriesLabels(ctx, []model.SeriesID{id}); err != nil || len(ls) != 0 {
		t.Fatalf("an expired entry must not be returned: %v %v", ls, err)
	}
	if err := x.WriteSeries(ctx, day, id, b); err != nil {
		t.Fatalf("the ID of an expired series must be free: %v", err)
	}
	if ls, _ := x.SeriesLabels(ctx, []model.SeriesID{id}); !labels.Equal(ls[id], b) {
		t.Fatalf("the new owner must be recorded: %v", ls)
	}
	if ttl, _ := kv.TTL(db.keys.series(id)); ttl < 71*time.Hour {
		t.Fatalf("the new entry must carry a TTL: %v", ttl)
	}
	if err := x.WriteSeries(ctx, day, id, a); !errors.Is(err, storage.ErrSeriesCollision) {
		t.Fatalf("the former owner must now collide: %v", err)
	}
}

// An entry about to expire is refreshed with a compare-and-swap, so that it
// cannot be claimed by other labels in between; one that is not is
// refreshed with a plain write.
func TestSeriesEntryRefresh(t *testing.T) {
	db, kv := newMemDB(t)
	now := time.Now()
	x := NewIndex(db, 24*time.Hour, 8)
	x.now = func() time.Time { return now }
	ctx := context.Background()
	id := model.SeriesID(0x78)
	a := labels.FromStrings("__name__", "up", "job", "a")
	expires := func() int64 {
		t.Helper()
		v, err := kv.Get(ctx, db.keys.series(id))
		if err != nil {
			t.Fatal(err)
		}
		_, e, err := decodeSeries(v)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	for _, step := range []time.Duration{0, 24 * time.Hour, 72*time.Hour - 30*time.Minute} {
		now = now.Add(step)
		if err := x.WriteSeries(ctx, model.Day(now.UnixMilli()), id, a); err != nil {
			t.Fatal(err)
		}
		if got, want := expires(), now.Add(72*time.Hour).UnixMilli(); got != want {
			t.Fatalf("after %v: expiry %d, want %d", step, got, want)
		}
	}
}

// The postings and label values of a label on different days are adjacent
// keys: the hash in front of the day does not depend on it.
func TestIndexKeysSpread(t *testing.T) {
	k := newKeys("xt")
	table := len(k.table(tagPosting))
	a1, a2 := k.labelPair(1, "job", "api"), k.labelPair(2, "job", "api")
	b1 := k.labelPair(1, "job", "db")
	if !bytes.Equal(a1[:table+2], a2[:table+2]) || bytes.Equal(a1[:table+2], b1[:table+2]) {
		t.Fatalf("unexpected spread: %x %x %x", a1, a2, b1)
	}
	if !bytes.Equal(k.labelValues(1, "job")[:table+2], k.labelValues(2, "job")[:table+2]) {
		t.Fatal("the values of a label must share their hash on every day")
	}
}

// The TTL semantics the stores rely on, on a real cluster: entries expire,
// expired entries are absent for compare-and-swap, and CheckTTL passes.
func TestTiKVTTLSemantics(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	if err := db.CheckTTL(ctx); err != nil {
		t.Fatal(err)
	}
	key := db.keys.meta("ttl_probe")
	if err := db.put(ctx, key, []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	if v, err := db.get(ctx, key); err != nil || string(v) != "old" {
		t.Fatalf("get before expiry: %q %v", v, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		v, err := db.get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if v == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("an entry with a TTL of 1s did not expire")
		}
		time.Sleep(200 * time.Millisecond)
	}
	prev, swapped, err := db.compareAndSwap(ctx, key, nil, []byte("new"))
	if err != nil || !swapped || prev != nil {
		t.Fatalf("an expired entry must be absent for compare-and-swap: %q %v %v", prev, swapped, err)
	}
	prev, swapped, err = db.compareAndSwap(ctx, key, nil, []byte("other"))
	if err != nil || swapped || string(prev) != "new" {
		t.Fatalf("compare-and-swap of a live entry: %q %v %v", prev, swapped, err)
	}
}

// stackError prints a stack trace with %+v, like the errors of the TiKV
// client.
type stackError struct{}

func (stackError) Error() string { return "region unavailable" }
func (e stackError) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, e.Error())
	if f.Flag('+') {
		_, _ = io.WriteString(f, "\ngoroutine 1 [running]:\n...")
	}
}

// Errors of the client reach the logs, which format them with %+v, without
// their stack traces, and keep their chain.
func TestErrorsWithoutStacks(t *testing.T) {
	logged := func(err error) string {
		var b bytes.Buffer
		slog.New(slog.NewTextHandler(&b, nil)).Error("failed", "err", err)
		return b.String()
	}
	if !strings.Contains(logged(stackError{}), "goroutine") {
		t.Fatal("the test needs an error that logs its stack")
	}
	// Ping returns the error of a read as it is, and readiness checks log it.
	err := plain(stackError{})
	if l := logged(err); strings.Contains(l, "goroutine") || !strings.Contains(l, "region unavailable") {
		t.Fatalf("unexpected log line: %s", l)
	}
	if !errors.As(err, new(stackError)) {
		t.Fatal("the error chain must be kept")
	}
	if plain(nil) != nil {
		t.Fatal("plain(nil) must be nil")
	}
}
