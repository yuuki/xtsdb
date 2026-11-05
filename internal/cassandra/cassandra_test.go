package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Integration tests need XTSDB_TEST_CASSANDRA_HOSTS (comma separated).
func newTestSession(t *testing.T) *gocql.Session {
	t.Helper()
	hosts := os.Getenv("XTSDB_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("XTSDB_TEST_CASSANDRA_HOSTS not set")
	}
	cfg := config.Default().Cassandra
	cfg.Hosts = strings.Split(hosts, ",")
	cfg.Keyspace = fmt.Sprintf("xttest_%d", rand.Uint32())
	cfg.Consistency = "ONE"
	ctx := context.Background()
	if err := Migrate(ctx, cfg, nil); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, cfg, nil); err != nil { // idempotent
		t.Fatalf("second migration: %v", err)
	}
	s, err := NewSession(cfg, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Query("DROP KEYSPACE " + cfg.Keyspace).Exec(); err != nil {
			t.Logf("drop keyspace: %v", err)
		}
		s.Close()
	})
	return s
}

func encode(t *testing.T, samples []model.Sample) []byte {
	t.Helper()
	b, err := chunk.EncodeXOR(samples)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestChunkStore(t *testing.T) {
	s := newTestSession(t)
	const hour = int64(time.Hour / time.Millisecond)
	store := NewChunkStore(s, 0, hour, 8, nil)
	ctx := context.Background()

	id := model.SeriesID(0x8000000000000001) // exercises negative bigint
	// The window straddles a day boundary: it starts on day N and ends on N+1.
	start := 20_000*model.DayMillis - hour/2
	first := []model.Sample{{T: start + 1000, V: 1}, {T: start + 2000, V: 2}, {T: start + hour - 1, V: 3}}
	write := func(frag int64, samples []model.Sample) {
		t.Helper()
		err := store.WriteChunk(ctx, storage.ColdChunk{
			Series: id, Start: start, Fragment: frag, MaxT: samples[len(samples)-1].T,
			Count: len(samples), Encoding: chunk.EncodingXOR, Data: encode(t, samples),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	write(100, first)
	write(100, first) // retried write must not duplicate data
	// A later fragment of the same window overrides overlapping samples.
	write(200, []model.Sample{{T: start + 2000, V: 20}, {T: start + 2500, V: 25}})

	// The query starts after the chunk start: the store must look back.
	got, err := store.Read(ctx, []model.SeriesID{id, 1}, start+1500, start+hour)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Sample{{T: start + 2000, V: 20}, {T: start + 2500, V: 25}, {T: start + hour - 1, V: 3}}
	if fmt.Sprint(got[id]) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got[id], want)
	}
	if _, ok := got[1]; ok {
		t.Fatal("unknown series returned")
	}
	if got, _ := store.Read(ctx, []model.SeriesID{id}, start+hour, start+2*hour); len(got[id]) != 0 {
		t.Fatalf("expected no samples after the window, got %v", got[id])
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestChunkVersionsAndParts(t *testing.T) {
	s := newTestSession(t)
	const hour = int64(time.Hour / time.Millisecond)
	store := NewChunkStore(s, 0, hour, 8, nil)
	ctx := context.Background()
	id := model.SeriesID(77)
	start := int64(20_000) * model.DayMillis
	write := func(total int, digest uint64, part int, samples []model.Sample) {
		t.Helper()
		err := store.WriteChunk(ctx, storage.ColdChunk{
			Series: id, Start: start, Fragment: 5, Total: total, Digest: digest, Part: part,
			MaxT: samples[len(samples)-1].T, Count: len(samples), Encoding: chunk.EncodingXOR, Data: encode(t, samples),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// A complete version in two parts...
	write(3, 0xaaaa, 0, []model.Sample{{T: start + 1, V: 1}, {T: start + 2, V: 2}})
	write(3, 0xaaaa, 1, []model.Sample{{T: start + 3, V: 3}})
	// ...and a stale version of the same incarnation with less data.
	write(1, 0xbbbb, 0, []model.Sample{{T: start + 1, V: 1}})
	got, err := store.Read(ctx, []model.SeriesID{id}, start, start+hour)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Sample{{T: start + 1, V: 1}, {T: start + 2, V: 2}, {T: start + 3, V: 3}}
	if fmt.Sprint(got[id]) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got[id], want)
	}
}

// Versions of a window take precedence in (fragment, total, digest) order,
// with the digest compared as a signed 64-bit value.
func TestChunkVersionPrecedence(t *testing.T) {
	s := newTestSession(t)
	const hour = int64(time.Hour / time.Millisecond)
	store := NewChunkStore(s, 0, hour, 8, nil)
	ctx := context.Background()
	start := int64(20_000) * model.DayMillis
	write := func(id model.SeriesID, frag int64, total int, digest uint64, v float64) {
		t.Helper()
		samples := []model.Sample{{T: start + 1, V: v}}
		err := store.WriteChunk(ctx, storage.ColdChunk{
			Series: id, Start: start, Fragment: frag, Total: total, Digest: digest,
			MaxT: start + 1, Count: 1, Encoding: chunk.EncodingXOR, Data: encode(t, samples),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	read := func(id model.SeriesID) float64 {
		t.Helper()
		got, err := store.Read(ctx, []model.SeriesID{id}, start, start+hour)
		if err != nil || len(got[id]) != 1 {
			t.Fatalf("read: %v %v", got, err)
		}
		return got[id][0].V
	}
	// A later state of the same incarnation (more records) wins over an
	// earlier one, whatever the digests.
	write(1, 5, 3, 0x7fff_ffff_ffff_ffff, 1)
	write(1, 5, 4, 1, 2)
	if v := read(1); v != 2 {
		t.Fatalf("version with more records must win, got %v", v)
	}
	// A later incarnation wins over a longer earlier one.
	write(2, 5, 9, 1, 1)
	write(2, 6, 1, 1, 2)
	if v := read(2); v != 2 {
		t.Fatalf("later fragment must win, got %v", v)
	}
	// Equal (fragment, total): the digest as a signed value decides.
	write(3, 5, 3, 0x8000_0000_0000_0000, 1) // negative
	write(3, 5, 3, 1, 2)
	if v := read(3); v != 2 {
		t.Fatalf("larger signed digest must win, got %v", v)
	}
}

func TestIndex(t *testing.T) {
	s := newTestSession(t)
	x := NewIndex(s, 0, 8, 4)
	ctx := context.Background()
	const day = int32(20_000)

	a := labels.FromStrings("__name__", "up", "job", "api", "instance", "a:9090")
	b := labels.FromStrings("__name__", "up", "job", "db", "instance", "b:9090")
	for _, lset := range []labels.Labels{a, b} {
		if err := x.WriteSeries(ctx, day, model.ID(lset), lset); err != nil {
			t.Fatal(err)
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
		if err != nil {
			t.Fatal(err)
		}
	}

	ids, err := x.Postings(ctx, day, "__name__", "up")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || !slices.Contains(ids, model.ID(a)) || !slices.Contains(ids, model.ID(b)) {
		t.Fatalf("postings = %v", ids)
	}
	if ids, _ := x.Postings(ctx, day+1, "__name__", "up"); len(ids) != 0 {
		t.Fatalf("postings must be scoped by day, got %v", ids)
	}
	vals, err := x.LabelValues(ctx, day, "job")
	if err != nil || fmt.Sprint(vals) != "[api db]" {
		t.Fatalf("label values = %v, %v", vals, err)
	}
	names, err := x.LabelNames(ctx, day)
	if err != nil || fmt.Sprint(names) != "[__name__ instance job]" {
		t.Fatalf("label names = %v, %v", names, err)
	}
	ls, err := x.SeriesLabels(ctx, []model.SeriesID{model.ID(a), 12345})
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 || !labels.Equal(ls[model.ID(a)], a) {
		t.Fatalf("series labels = %v", ls)
	}
}

// The bucket functions are part of the storage format.
func TestBucketsGolden(t *testing.T) {
	if got := postingsBucket(0xf28c8afcd224e55f, 16); got != 15 {
		t.Fatalf("postings bucket changed: %d", got)
	}
	if got := valueBucket("prometheus", 16); got != 2 {
		t.Fatalf("value bucket changed: %d", got)
	}
	if postingsBucket(0xffffffffffffffff, 256) < 0 || valueBucket("x", 1) != 0 {
		t.Fatal("buckets must be in [0, n)")
	}
}

// Series sharing a label pair, and values of a label, are spread over the
// buckets and read back from all of them.
func TestIndexBuckets(t *testing.T) {
	s := newTestSession(t)
	const buckets = 4
	x := NewIndex(s, 0, 8, buckets)
	ctx := context.Background()
	const day = int32(20_000)
	var want []model.SeriesID
	for i := range 64 {
		lset := labels.FromStrings("__name__", "up", "instance", fmt.Sprintf("host-%02d", i))
		id := model.ID(lset)
		want = append(want, id)
		if err := x.WriteSeries(ctx, day, id, lset); err != nil {
			t.Fatal(err)
		}
		if err := x.WriteLabelValue(ctx, day, "instance", fmt.Sprintf("host-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := x.Postings(ctx, day, "__name__", "up")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("postings: got %d series, want %d", len(got), len(want))
	}
	vals, err := x.LabelValues(ctx, day, "instance")
	if err != nil || len(vals) != 64 || !slices.IsSorted(vals) {
		t.Fatalf("label values: %d %v", len(vals), err)
	}
	for b := range buckets {
		var n int
		if err := s.Query(`SELECT COUNT(*) FROM postings WHERE day = ? AND name = ? AND value = ? AND bucket = ?`,
			day, "__name__", "up", b).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 || n == 64 {
			t.Fatalf("bucket %d holds %d of 64 series: postings are not spread", b, n)
		}
	}
}

func TestCheckLayout(t *testing.T) {
	s := newTestSession(t)
	ctx := context.Background()
	if err := CheckLayout(ctx, s, 16, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := CheckLayout(ctx, s, 16, time.Hour); err != nil {
		t.Fatalf("same layout: %v", err)
	}
	if err := CheckLayout(ctx, s, 8, time.Hour); err == nil || !strings.Contains(err.Error(), "index_buckets") {
		t.Fatalf("expected an index_buckets mismatch, got %v", err)
	}
	if err := CheckLayout(ctx, s, 16, time.Minute); err == nil || !strings.Contains(err.Error(), "chunk_range") {
		t.Fatalf("expected a chunk_range mismatch, got %v", err)
	}
}

// Tables created by earlier builds without buckets cannot be migrated in
// place; the migration must say so instead of leaving them broken.
func TestMigrateRefusesUnbucketedTables(t *testing.T) {
	hosts := os.Getenv("XTSDB_TEST_CASSANDRA_HOSTS")
	if hosts == "" {
		t.Skip("XTSDB_TEST_CASSANDRA_HOSTS not set")
	}
	cfg := config.Default().Cassandra
	cfg.Hosts = strings.Split(hosts, ",")
	cfg.Keyspace = fmt.Sprintf("xttest_%d", rand.Uint32())
	cfg.Consistency = "ONE"
	s, err := NewSession(cfg, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t.Cleanup(func() { _ = s.Query("DROP KEYSPACE IF EXISTS " + cfg.Keyspace).Exec() })
	for _, stmt := range []string{
		"CREATE KEYSPACE " + cfg.Keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		"CREATE TABLE " + cfg.Keyspace + ".postings (day int, name text, value text, series_id bigint, PRIMARY KEY ((day, name, value), series_id))",
	} {
		if err := s.Query(stmt).Exec(); err != nil {
			t.Fatal(err)
		}
	}
	err = Migrate(context.Background(), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "without index buckets") {
		t.Fatalf("expected the migration to refuse the old postings table, got %v", err)
	}
}

// TestPostingsStress indexes a million series that share one label pair,
// as with __name__ of a large metric. Set XTSDB_TEST_STRESS=1 to run it.
func TestPostingsStress(t *testing.T) {
	if os.Getenv("XTSDB_TEST_STRESS") == "" {
		t.Skip("XTSDB_TEST_STRESS not set")
	}
	s := newTestSession(t)
	const (
		series  = 1_000_000
		buckets = 16
	)
	x := NewIndex(s, 0, 64, buckets)
	ctx := context.Background()
	const day = int32(20_000)
	lset := labels.FromStrings("__name__", "container_cpu_usage_seconds_total")
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(512)
	for i := range series {
		g.Go(func() error {
			return x.writePostings(gctx, day, model.SeriesID(rand.Uint64()|uint64(i)<<44), lset, 3600)
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d postings in %v", series, time.Since(start))

	start = time.Now()
	ids, err := x.Postings(ctx, day, "__name__", "container_cpu_usage_seconds_total")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("read %d postings in %v", len(ids), time.Since(start))
	if len(ids) < series*999/1000 { // random IDs may repeat, very rarely
		t.Fatalf("read %d postings, want about %d", len(ids), series)
	}
	var largest int
	for b := range buckets {
		var n int
		if err := s.Query(`SELECT COUNT(*) FROM postings WHERE day = ? AND name = '__name__' AND value = ? AND bucket = ?`,
			day, lset.Get("__name__"), b).Scan(&n); err != nil {
			t.Fatal(err)
		}
		largest = max(largest, n)
	}
	t.Logf("largest partition: %d rows (%d per bucket on average)", largest, series/buckets)
	if largest > series/buckets*11/10 {
		t.Fatalf("largest partition holds %d rows, want about %d", largest, series/buckets)
	}
}

func TestIndexDetectsCollisions(t *testing.T) {
	s := newTestSession(t)
	x := NewIndex(s, 24*time.Hour, 8, 4)
	ctx := context.Background()
	day := model.Day(time.Now().UnixMilli())
	id := model.SeriesID(0x8000000000000042)
	a := labels.FromStrings("__name__", "up", "job", "api")
	b := labels.FromStrings("__name__", "up", "job", "db")
	if err := x.WriteSeries(ctx, day, id, a); err != nil {
		t.Fatal(err)
	}
	if err := x.WriteSeries(ctx, day+1, id, a); err != nil {
		t.Fatalf("the same series must be recorded again: %v", err)
	}
	err := x.WriteSeries(ctx, day, id, b)
	var coll *storage.SeriesCollisionError
	if !errors.As(err, &coll) || !errors.Is(err, storage.ErrSeriesCollision) {
		t.Fatalf("expected a collision, got %v", err)
	}
	if coll.ID != id || !labels.Equal(coll.Existing, a) || !labels.Equal(coll.Labels, b) {
		t.Fatalf("unexpected collision %+v", coll)
	}
	if ids, _ := x.Postings(ctx, day, "job", "db"); len(ids) != 0 {
		t.Fatalf("postings of the refused series were written: %v", ids)
	}
	ls, err := x.SeriesLabels(ctx, []model.SeriesID{id})
	if err != nil || !labels.Equal(ls[id], a) {
		t.Fatalf("the owner's labels must be kept: %v %v", ls, err)
	}
	var ttl int
	if err := s.Query(`SELECT TTL(labels) FROM series WHERE series_id = ?`, int64(id)).Scan(&ttl); err != nil || ttl <= 0 {
		t.Fatalf("series row must carry a TTL: %d %v", ttl, err)
	}
}

// Rows expire storage.retention after the time of their data, not after
// they were written, so that imported history expires like recent data.
func TestTTLFollowsDataTime(t *testing.T) {
	s := newTestSession(t)
	ctx := context.Background()
	const hour = int64(time.Hour / time.Millisecond)
	now := time.Now()
	store := NewChunkStore(s, 24*time.Hour, hour, 8, nil)
	write := func(id model.SeriesID, maxT int64) {
		t.Helper()
		start := maxT - 1000
		err := store.WriteChunk(ctx, storage.ColdChunk{Series: id, Start: start, MaxT: maxT, Count: 1,
			Encoding: chunk.EncodingXOR, Data: encode(t, []model.Sample{{T: maxT, V: 1}})})
		if err != nil {
			t.Fatal(err)
		}
	}
	write(1, now.Add(-23*time.Hour).UnixMilli())
	write(2, now.Add(-25*time.Hour).UnixMilli())
	var ttl int
	if err := s.Query(`SELECT TTL(data) FROM chunks WHERE series_id = 1 ALLOW FILTERING`).Scan(&ttl); err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 3600 {
		t.Fatalf("a chunk 23h old with 24h retention must expire within an hour, TTL %d", ttl)
	}
	var n int
	if err := s.Query(`SELECT COUNT(*) FROM chunks WHERE series_id = 2 ALLOW FILTERING`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("a chunk older than the retention must not be written: %d %v", n, err)
	}

	x := NewIndex(s, 24*time.Hour, 8, 1)
	today := model.Day(now.UnixMilli())
	for _, day := range []int32{today, today - 3, today - 4} {
		if err := x.WriteLabelName(ctx, day, "job"); err != nil {
			t.Fatal(err)
		}
	}
	for day, want := range map[int32]bool{today: true, today - 3: true, today - 4: false} {
		names, err := x.LabelNames(ctx, day)
		if err != nil || (len(names) == 1) != want {
			t.Errorf("day %d: names %v (%v), want present=%v", today-day, names, err, want)
		}
	}
}

func TestTTLSeconds(t *testing.T) {
	if TTLSeconds(0) != 0 || TTLSeconds(time.Hour) != 3600 {
		t.Fatal("unexpected TTL")
	}
	if TTLSeconds(100*365*24*time.Hour) != int(maxTTL/time.Second) {
		t.Fatal("TTL must be capped")
	}
}

func TestReplication(t *testing.T) {
	cfg := config.Default().Cassandra
	cfg.ReplicationClass = "NetworkTopologyStrategy"
	if _, err := replication(cfg); err == nil {
		t.Fatal("datacenters must be required")
	}
	cfg.Datacenters = map[string]int{"dc2": 2, "dc1": 3}
	got, err := replication(cfg)
	if err != nil || got != "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}" {
		t.Fatalf("got %q, %v", got, err)
	}
	if validIdentifier("1abc") || validIdentifier("a-b") || !validIdentifier("xtsdb_1") {
		t.Fatal("identifier validation is wrong")
	}
}
