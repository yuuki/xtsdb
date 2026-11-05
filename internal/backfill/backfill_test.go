package backfill

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"

	"github.com/yuuki/xtsdb/internal/flush"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

var testLayout = model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: 10 * time.Minute.Milliseconds()}

func newImporter(t *testing.T) (*Importer, *storagetest.Cold, *storagetest.Index) {
	t.Helper()
	cold := storagetest.NewCold(testLayout.ChunkRange)
	index := storagetest.NewIndex()
	im, err := New(cold, index, Options{Layout: testLayout, Retention: 30 * 24 * time.Hour, MaxFuture: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return im, cold, index
}

// series returns one sample per minute over [from, to).
func series(from, to time.Time) []model.Sample {
	var out []model.Sample
	for t := from; t.Before(to); t = t.Add(time.Minute) {
		out = append(out, model.Sample{T: t.UnixMilli(), V: float64(t.Unix())})
	}
	return out
}

func TestImportSeries(t *testing.T) {
	im, cold, index := newImporter(t)
	ctx := context.Background()
	now := time.Now()
	from := now.Add(-50 * time.Hour).Truncate(time.Hour)
	samples := series(from, now.Add(-time.Hour))
	lset := labels.FromStrings("__name__", "up", "job", "old")
	id := model.ID(lset)
	shuffled := slices.Clone(samples)
	slices.Reverse(shuffled) // order does not matter
	if err := im.Import(ctx, lset, shuffled); err != nil {
		t.Fatal(err)
	}
	if im.Stats.Samples.Load() != int64(len(samples)) || im.Stats.Series.Load() != 1 {
		t.Fatalf("stats: %s", im.Stats.String())
	}
	got, err := cold.Read(ctx, []model.SeriesID{id}, from.UnixMilli(), now.UnixMilli())
	if err != nil || len(got[id]) != len(samples) {
		t.Fatalf("read %d samples, want %d (%v)", len(got[id]), len(samples), err)
	}
	if fmt.Sprint(got[id][:3]) != fmt.Sprint(samples[:3]) {
		t.Fatalf("got %v, want %v", got[id][:3], samples[:3])
	}
	// The series is indexed on every day it has samples on.
	for _, day := range model.Days(samples[0].T, samples[len(samples)-1].T) {
		ids, _ := index.Postings(ctx, day, "job", "old")
		names, _ := index.LabelNames(ctx, day)
		if len(ids) != 1 || ids[0] != id || len(names) != 2 {
			t.Fatalf("day %d: postings %v, names %v", day, ids, names)
		}
	}
	// Importing again writes the same rows.
	chunks := cold.Len()
	if err := im.Import(ctx, lset, slices.Clone(samples)); err != nil {
		t.Fatal(err)
	}
	if cold.Len() != chunks {
		t.Fatalf("a second import wrote %d new chunks", cold.Len()-chunks)
	}
}

// Ingested samples take precedence over imported ones at the same
// timestamp, whichever is written first.
func TestImportDoesNotOverrideIngestedSamples(t *testing.T) {
	im, cold, _ := newImporter(t)
	ctx := context.Background()
	lset := labels.FromStrings("__name__", "temperature")
	id := model.ID(lset)
	t0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour).UnixMilli()
	start := testLayout.WindowStart(id, t0)
	ingested := []model.Sample{{T: t0, V: 1}, {T: t0 + 1000, V: 2}}
	if _, err := flush.WriteCold(ctx, cold, id, start, time.Now().UnixMicro(), 2, ingested); err != nil {
		t.Fatal(err)
	}
	if err := im.Import(ctx, lset, []model.Sample{{T: t0 + 1000, V: 20}, {T: t0 + 2000, V: 30}}); err != nil {
		t.Fatal(err)
	}
	got, err := cold.Read(ctx, []model.SeriesID{id}, t0, t0+10_000)
	want := []model.Sample{{T: t0, V: 1}, {T: t0 + 1000, V: 2}, {T: t0 + 2000, V: 30}}
	if err != nil || fmt.Sprint(got[id]) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v (%v)", got[id], want, err)
	}
}

func TestImportSkips(t *testing.T) {
	im, cold, index := newImporter(t)
	ctx := context.Background()
	now := time.Now()
	lset := labels.FromStrings("__name__", "m")
	err := im.Import(ctx, lset, []model.Sample{
		{T: now.Add(-31 * 24 * time.Hour).UnixMilli(), V: 1}, // beyond the retention
		{T: now.Add(time.Hour).UnixMilli(), V: 1},            // too far in the future
		{T: now.Add(-time.Hour).UnixMilli(), V: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if im.Stats.TooOld.Load() != 1 || im.Stats.TooNew.Load() != 1 || im.Stats.Samples.Load() != 1 {
		t.Fatalf("stats: %s", im.Stats.String())
	}
	if err := im.Import(ctx, labels.FromStrings("job", "no name"), []model.Sample{{T: now.UnixMilli(), V: 1}}); err != nil {
		t.Fatal(err)
	}
	if im.Stats.Invalid.Load() != 1 {
		t.Fatalf("invalid series not counted: %s", im.Stats.String())
	}
	// A series whose ID already belongs to other labels is not imported.
	other := labels.FromStrings("__name__", "collides")
	day := model.Day(now.Add(-time.Hour).UnixMilli())
	if err := index.WriteSeries(ctx, day, model.ID(other), labels.FromStrings("__name__", "owner")); err != nil {
		t.Fatal(err)
	}
	chunks := cold.Len()
	if err := im.Import(ctx, other, []model.Sample{{T: now.Add(-time.Hour).UnixMilli(), V: 1}}); err != nil {
		t.Fatal(err)
	}
	if im.Stats.Collisions.Load() != 1 || cold.Len() != chunks {
		t.Fatalf("colliding series was imported: %s", im.Stats.String())
	}
}

func TestImportOpenMetrics(t *testing.T) {
	now := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	var b strings.Builder
	b.WriteString("# TYPE http_requests counter\n")
	for i := range 120 {
		ts := float64(now.Add(time.Duration(i)*time.Minute).UnixMilli()) / 1000
		fmt.Fprintf(&b, "http_requests_total{code=\"200\"} %d %.3f\n", i, ts)
		fmt.Fprintf(&b, "http_requests_total{code=\"500\"} %d %.3f\n", i/10, ts)
		fmt.Fprintf(&b, "http_requests_created{code=\"200\"} %d %.3f\n", now.Unix(), ts)
	}
	b.WriteString("# EOF\n")
	data := []byte(b.String())

	read := func(im *Importer, cold *storagetest.Cold, code string) []model.Sample {
		t.Helper()
		id := model.ID(labels.FromStrings("__name__", "http_requests_total", "code", code))
		got, err := cold.Read(context.Background(), []model.SeriesID{id}, now.UnixMilli(), now.Add(3*time.Hour).UnixMilli())
		if err != nil {
			t.Fatal(err)
		}
		return got[id]
	}
	// Importing in one go, and in slices of 7 samples, gives the same data.
	for _, maxBuffered := range []int{0, 7} {
		im, cold, _ := newImporter(t)
		if err := im.ImportOpenMetrics(context.Background(), data, 4, maxBuffered); err != nil {
			t.Fatal(err)
		}
		if got := read(im, cold, "200"); len(got) != 120 || got[119].V != 119 {
			t.Fatalf("maxBuffered=%d: imported %d samples", maxBuffered, len(got))
		}
		if got := read(im, cold, "500"); len(got) != 120 {
			t.Fatalf("maxBuffered=%d: imported %d samples", maxBuffered, len(got))
		}
		if im.Stats.Series.Load() < 2 || im.Stats.Samples.Load() != 240 {
			t.Fatalf("maxBuffered=%d: %s (the _created series must be skipped)", maxBuffered, im.Stats.String())
		}
	}
	im, _, _ := newImporter(t)
	err := im.ImportOpenMetrics(context.Background(), []byte("up 1\n# EOF\n"), 1, 0)
	if err == nil || !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("samples without a timestamp must be refused, got %v", err)
	}
}

// writeBlock writes a Prometheus TSDB block with the low-level writers.
func writeBlock(t *testing.T, dir string, series map[string][]model.Sample, deleted map[string]tombstones.Interval) string {
	t.Helper()
	const ulid = "01JC0000000000000000000000"
	block := filepath.Join(dir, ulid)
	if err := os.MkdirAll(block, 0o755); err != nil {
		t.Fatal(err)
	}
	cw, err := chunks.NewWriter(filepath.Join(block, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	type entry struct {
		lset  labels.Labels
		metas []chunks.Meta
	}
	var entries []entry
	symbols := map[string]struct{}{}
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	for name, samples := range series {
		lset := labels.FromStrings("__name__", name, "job", "prom")
		lset.Range(func(l labels.Label) { symbols[l.Name], symbols[l.Value] = struct{}{}, struct{}{} })
		var metas []chunks.Meta
		for i := 0; i < len(samples); i += 120 {
			part := samples[i:min(i+120, len(samples))]
			c := chunkenc.NewXORChunk()
			app, err := c.Appender()
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range part {
				app.Append(0, s.T, s.V)
			}
			metas = append(metas, chunks.Meta{Chunk: c, MinTime: part[0].T, MaxTime: part[len(part)-1].T})
			mint, maxt = min(mint, part[0].T), max(maxt, part[len(part)-1].T)
		}
		if err := cw.WriteChunks(metas...); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry{lset, metas})
	}
	if err := cw.Close(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(entries, func(a, b entry) int { return labels.Compare(a.lset, b.lset) })
	iw, err := index.NewWriter(context.Background(), filepath.Join(block, "index"))
	if err != nil {
		t.Fatal(err)
	}
	syms := make([]string, 0, len(symbols))
	for s := range symbols {
		syms = append(syms, s)
	}
	slices.Sort(syms)
	for _, s := range syms {
		if err := iw.AddSymbol(s); err != nil {
			t.Fatal(err)
		}
	}
	for i, e := range entries {
		if err := iw.AddSeries(promstorage.SeriesRef(i+1), e.lset, e.metas...); err != nil {
			t.Fatal(err)
		}
	}
	if err := iw.Close(); err != nil {
		t.Fatal(err)
	}
	// Tombstones refer to series by their reference in the index file.
	ir, err := index.NewFileReader(filepath.Join(block, "index"), index.DecodePostingsRaw)
	if err != nil {
		t.Fatal(err)
	}
	defer ir.Close()
	allName, allValue := index.AllPostingsKey()
	p, err := ir.Postings(context.Background(), allName, allValue)
	if err != nil {
		t.Fatal(err)
	}
	tomb := tombstones.NewMemTombstones()
	var builder labels.ScratchBuilder
	for p.Next() {
		var metas []chunks.Meta
		if err := ir.Series(p.At(), &builder, &metas); err != nil {
			t.Fatal(err)
		}
		if d, ok := deleted[builder.Labels().Get("__name__")]; ok {
			tomb.AddInterval(p.At(), d)
		}
	}
	if _, err := tombstones.WriteFile(nil, block, tomb); err != nil {
		t.Fatal(err)
	}
	meta := fmt.Sprintf(`{"ulid":%q,"minTime":%d,"maxTime":%d,"version":1}`, ulid, mint, maxt+1)
	if err := os.WriteFile(filepath.Join(block, "meta.json"), []byte(meta), 0o644); err != nil {
		t.Fatal(err)
	}
	return block
}

func TestImportBlock(t *testing.T) {
	im, cold, index := newImporter(t)
	ctx := context.Background()
	now := time.Now()
	from := now.Add(-26 * time.Hour).Truncate(time.Hour)
	cpu := series(from, now.Add(-2*time.Hour))
	mem := series(from, from.Add(10*time.Minute))
	deletedFrom := cpu[100].T
	dir := t.TempDir()
	writeBlock(t, dir, map[string][]model.Sample{"cpu": cpu, "mem": mem},
		map[string]tombstones.Interval{"cpu": {Mint: deletedFrom, Maxt: deletedFrom + 9*60_000}})

	blocks, err := Blocks(dir)
	if err != nil || len(blocks) != 1 {
		t.Fatalf("blocks: %v %v", blocks, err)
	}
	if id, err := BlockID(blocks[0]); err != nil || id != "01JC0000000000000000000000" {
		t.Fatalf("block ID %q %v", id, err)
	}
	if err := im.ImportBlock(ctx, blocks[0], 4); err != nil {
		t.Fatal(err)
	}
	cpuID := model.ID(labels.FromStrings("__name__", "cpu", "job", "prom"))
	got, err := cold.Read(ctx, []model.SeriesID{cpuID}, from.UnixMilli(), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if len(got[cpuID]) != len(cpu)-10 {
		t.Fatalf("imported %d samples of cpu, want %d without the 10 deleted ones", len(got[cpuID]), len(cpu)-10)
	}
	if slices.ContainsFunc(got[cpuID], func(s model.Sample) bool { return s.T >= deletedFrom && s.T < deletedFrom+600_000 }) {
		t.Fatal("deleted samples were imported")
	}
	if ids, _ := index.Postings(ctx, model.Day(from.UnixMilli()), "__name__", "mem"); len(ids) != 1 {
		t.Fatalf("mem is not indexed: %v", ids)
	}
	if _, err := Blocks(t.TempDir()); err == nil {
		t.Fatal("a directory without blocks must be an error")
	}
	if HasHead(dir) {
		t.Fatal("no head yet")
	}
	if err := os.Mkdir(filepath.Join(dir, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if !HasHead(dir) {
		t.Fatal("a data directory with wal/ has a head that is not imported")
	}
}

func TestState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Done("block a") {
		t.Fatal("nothing is done yet")
	}
	if err := s.MarkDone("block a"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.Done("block a") || s.Done("block b") {
		t.Fatal("state not persisted")
	}
	f := filepath.Join(t.TempDir(), "data.om")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	a, _ := FileID(f)
	if err := os.WriteFile(f, []byte("xy"), 0o644); err != nil {
		t.Fatal(err)
	}
	if b, _ := FileID(f); a == b || !strings.HasPrefix(a, "file /") {
		t.Fatalf("file IDs must change with the content: %q %q", a, b)
	}
}
