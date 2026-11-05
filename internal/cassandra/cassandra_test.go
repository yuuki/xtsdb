package cassandra

import (
	"context"
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
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
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

// The conformance tests of the persistent tier and the index.
func TestColdConformance(t *testing.T) {
	storagetest.RunColdStoreTests(t, func(t *testing.T, retention time.Duration, chunkRange int64) storage.ColdStore {
		return NewChunkStore(newTestSession(t), retention, chunkRange, 8, nil)
	})
}

func TestIndexConformance(t *testing.T) {
	storagetest.RunIndexTests(t, func(t *testing.T, retention time.Duration) storagetest.IndexStore {
		return NewIndex(newTestSession(t), retention, 8, 4)
	})
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

// Series rows carry a TTL, refreshed every day the series is written.
func TestSeriesTTL(t *testing.T) {
	s := newTestSession(t)
	x := NewIndex(s, 24*time.Hour, 8, 4)
	ctx := context.Background()
	day := model.Day(time.Now().UnixMilli())
	id := model.SeriesID(0x8000000000000042)
	if err := x.WriteSeries(ctx, day, id, labels.FromStrings("__name__", "up", "job", "api")); err != nil {
		t.Fatal(err)
	}
	var ttl int
	if err := s.Query(`SELECT TTL(labels) FROM series WHERE series_id = ?`, int64(id)).Scan(&ttl); err != nil ||
		ttl <= 0 || ttl > int((24*time.Hour+storage.IndexSlack)/time.Second) {
		t.Fatalf("series row must carry a TTL of the retention plus the index slack: %d %v", ttl, err)
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
	maxT := now.Add(-23 * time.Hour).UnixMilli()
	err := store.WriteChunk(ctx, storage.ColdChunk{Series: 1, Start: maxT - 1000, MaxT: maxT, Count: 1,
		Encoding: chunk.EncodingXOR, Data: encode(t, []model.Sample{{T: maxT, V: 1}})})
	if err != nil {
		t.Fatal(err)
	}
	var ttl int
	if err := s.Query(`SELECT TTL(data) FROM chunks WHERE series_id = 1 ALLOW FILTERING`).Scan(&ttl); err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > 3600 {
		t.Fatalf("a chunk 23h old with 24h retention must expire within an hour, TTL %d", ttl)
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
