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
	store := NewChunkStore(s, 24*time.Hour, hour, 8, nil)
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

func TestIndex(t *testing.T) {
	s := newTestSession(t)
	x := NewIndex(s, 24*time.Hour, 8)
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
