package hot

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Integration tests run against a real server. Set XTSDB_TEST_REDIS_ADDRS
// to a comma separated list of addresses (several addresses or
// XTSDB_TEST_REDIS_CLUSTER=1 select cluster mode).
func newTestStore(t testing.TB, layout model.Layout, lease time.Duration) (*Store, redis.UniversalClient) {
	t.Helper()
	addrs := os.Getenv("XTSDB_TEST_REDIS_ADDRS")
	if addrs == "" {
		t.Skip("XTSDB_TEST_REDIS_ADDRS not set")
	}
	client, err := NewClient(config.RedisConfig{
		Addrs:       strings.Split(addrs, ","),
		ClusterMode: os.Getenv("XTSDB_TEST_REDIS_CLUSTER") == "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("xttest%d", rand.Uint64())
	ctx := context.Background()
	s, err := New(ctx, client, Options{Prefix: prefix, Layout: layout, Lease: lease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deleteKeys(t, client, prefix)
		_ = client.Close()
	})
	return s, client
}

func deleteKeys(t testing.TB, client redis.UniversalClient, prefix string) {
	ctx := context.Background()
	del := func(ctx context.Context, c *redis.Client) error {
		iter := c.Scan(ctx, 0, prefix+":*", 1000).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
		return iter.Err()
	}
	var err error
	if cc, ok := client.(*redis.ClusterClient); ok {
		err = cc.ForEachMaster(ctx, del)
	} else {
		err = del(ctx, client.(*redis.Client))
	}
	if err != nil {
		t.Logf("cleanup: %v", err)
	}
}

func nowMs() int64 { return time.Now().UnixMilli() }

// load returns the loaded and missing chunks.
func load(ctx context.Context, s *Store, refs []storage.ChunkRef) ([]storage.HotChunk, []storage.ChunkRef, error) {
	res, err := s.Load(ctx, refs)
	return res.Chunks, res.Missing, err
}

func TestAppendAndRead(t *testing.T) {
	layout := model.Layout{Shards: 8, ChunkRange: time.Hour.Milliseconds(), Grace: time.Minute.Milliseconds()}
	s, _ := newTestStore(t, layout, time.Minute)
	ctx := context.Background()

	now := nowMs()
	a, b := model.SeriesID(1), model.SeriesID(0xdeadbeef00000002)
	res, err := s.Append(ctx, []storage.SeriesSamples{
		{ID: a, Samples: []model.Sample{{T: now - 2000, V: 1}, {T: now - 1000, V: 2}}},
		{ID: b, Samples: []model.Sample{{T: now - 1000, V: 10}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 3 || res.TooOld != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
	// Out-of-order write and an overwrite of an existing timestamp.
	if _, err := s.Append(ctx, []storage.SeriesSamples{
		{ID: a, Samples: []model.Sample{{T: now - 3000, V: 0}, {T: now - 1000, V: 2.5}}},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Read(ctx, []model.SeriesID{a, b, 42}, now-10_000, now)
	if err != nil {
		t.Fatal(err)
	}
	wantA := []model.Sample{{T: now - 3000, V: 0}, {T: now - 2000, V: 1}, {T: now - 1000, V: 2.5}}
	if fmt.Sprint(got[a]) != fmt.Sprint(wantA) {
		t.Fatalf("series a: got %v, want %v", got[a], wantA)
	}
	if len(got[b]) != 1 || got[b][0].V != 10 {
		t.Fatalf("series b: got %v", got[b])
	}
	if _, ok := got[42]; ok {
		t.Fatal("unknown series must not be returned")
	}
	// Range filtering.
	got, err = s.Read(ctx, []model.SeriesID{a}, now-2500, now-1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[a]) != 1 || got[a][0].T != now-2000 {
		t.Fatalf("filtered read: got %v", got[a])
	}
}

func TestRejectsClosedWindows(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 1000, Grace: 0}
	s, _ := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	now := nowMs()
	res, err := s.Append(ctx, []storage.SeriesSamples{
		{ID: 7, Samples: []model.Sample{{T: now - 10_000, V: 1}, {T: now - 9_000, V: 1}, {T: now + 1000, V: 2}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.TooOld != 2 || res.Accepted != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func waitClaim(t *testing.T, s *Store, shard int) []storage.ChunkRef {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		refs, err := s.Claim(context.Background(), shard, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(refs) > 0 {
			return refs
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("nothing became claimable")
	return nil
}

func TestFlushLifecycle(t *testing.T) {
	layout := model.Layout{Shards: 2, ChunkRange: 500, Grace: 200}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	id := model.SeriesID(99)
	shard := layout.Shard(id)

	now := nowMs()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now, V: 1}, {T: now + 1, V: 2}}}}); err != nil {
		t.Fatal(err)
	}
	if refs, _ := s.Claim(ctx, shard, 10); len(refs) != 0 {
		t.Fatalf("open chunk must not be claimable, got %v", refs)
	}

	refs := waitClaim(t, s, shard)
	if len(refs) != 1 || refs[0].Series != id || refs[0].Start != layout.WindowStart(id, now) {
		t.Fatalf("unexpected claim %+v", refs)
	}

	// The window is closed now: late samples are rejected, never lost.
	res, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now + 2, V: 3}}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.TooOld != 1 {
		t.Fatalf("late sample must be rejected, got %+v", res)
	}

	chunks, missing, err := load(ctx, s, refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 || len(chunks) != 1 || len(chunks[0].Samples) != 2 || chunks[0].Fragment == 0 {
		t.Fatalf("unexpected load %+v", chunks)
	}
	// Data stays readable until the chunk is completed.
	if got, _ := s.Read(ctx, []model.SeriesID{id}, now-1000, now+1000); len(got[id]) != 2 {
		t.Fatalf("in-flight chunk must stay readable, got %v", got)
	}
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Read(ctx, []model.SeriesID{id}, now-1000, now+1000); len(got[id]) != 0 {
		t.Fatalf("completed chunk must be gone, got %v", got)
	}
	if n := client.ZCard(ctx, s.inflightKey(shard)).Val(); n != 0 {
		t.Fatalf("in-flight set not empty: %d", n)
	}
	b, err := s.Backlog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b.PendingChunks != 0 || b.InflightChunks != 0 {
		t.Fatalf("unexpected backlog %+v", b)
	}
}

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, _ := newTestStore(t, layout, 500*time.Millisecond)
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	first := waitClaim(t, s, 0)
	if refs, _ := s.Claim(ctx, 0, 10); len(refs) != 0 {
		t.Fatalf("leased chunk handed out twice: %v", refs)
	}
	// The first flusher "dies"; after the lease the chunk is handed out again.
	second := waitClaim(t, s, 0)
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatalf("reclaimed %v, want %v", second, first)
	}
}

func TestCompleteIgnoresNewerIncarnation(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	refs := waitClaim(t, s, 0)
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("load: %v %v", chunks, err)
	}
	// Simulate a newer incarnation of the same chunk.
	key := s.chunkKey(0, member(refs[0].Series, refs[0].Start))
	client.Set(ctx, key, "9999999999999999", 0)
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if v := client.Get(ctx, key).Val(); v != "9999999999999999" {
		t.Fatalf("newer incarnation was deleted")
	}
}

func TestQuarantinesCorruptChunks(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	s.quarantineTTL = time.Hour
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	refs := waitClaim(t, s, 0)
	m := member(refs[0].Series, refs[0].Start)
	client.Append(ctx, s.chunkKey(0, m), "x") // break record alignment
	content := client.Get(ctx, s.chunkKey(0, m)).Val()
	res, err := s.Load(ctx, refs)
	if err != nil || len(res.Chunks) != 0 || len(res.Missing) != 0 || res.Quarantined != 1 {
		t.Fatalf("corrupt chunk must be quarantined and skipped: %+v %v", res, err)
	}
	qkey := s.quarantineKey(0, m, []byte(content))
	if client.Get(ctx, qkey).Val() != content {
		t.Fatal("corrupt chunk was not quarantined")
	}
	if ttl := client.PTTL(ctx, qkey).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("quarantined chunk must expire after the TTL, got %v", ttl)
	}
	if client.ZCard(ctx, s.inflightKey(0)).Val() != 0 || client.ZCard(ctx, s.inflightDueKey(0)).Val() != 0 {
		t.Fatal("quarantined chunk is still in flight")
	}
	if b, err := s.Backlog(ctx); err != nil || b.QuarantinedChunks != 1 {
		t.Fatalf("backlog must count the quarantined chunk: %+v %v", b, err)
	}
	list, err := s.ListQuarantined(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	if q := list[0]; q.Key != qkey || q.Series != 5 || q.Start != refs[0].Start || q.Bytes != int64(len(content)) ||
		q.Header != content[:16] || q.Expires.IsZero() {
		t.Fatalf("unexpected listing %+v", q)
	}
}

// A second corrupt incarnation of the same window must not overwrite the
// first one in quarantine.
func TestQuarantineKeepsEveryIncarnation(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	id := model.SeriesID(5)
	var start int64
	for i := range 2 {
		now := nowMs()
		if i == 1 {
			now = start + 1 // same window as the first incarnation
		}
		key := s.chunkKey(0, member(id, layout.WindowStart(id, now)))
		start = layout.WindowStart(id, now)
		// Re-create the window as it would be after a restore, then corrupt it.
		client.Set(ctx, key, fmt.Sprintf("%016d", 1000+i)+strings.Repeat("y", 17+i), 0)
		client.ZAdd(ctx, s.inflightKey(0), redis.Z{Score: 0, Member: member(id, start)})
		res, err := s.Load(ctx, []storage.ChunkRef{{Shard: 0, Series: id, Start: start}})
		if err != nil || res.Quarantined != 1 {
			t.Fatalf("incarnation %d: %+v %v", i, res, err)
		}
	}
	list, err := s.ListQuarantined(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("both incarnations must be kept, got %+v %v", list, err)
	}
	if !list[0].Expires.IsZero() || !list[1].Expires.IsZero() {
		t.Fatal("without a TTL quarantined chunks must not expire")
	}
	n, err := s.DeleteQuarantined(ctx, []string{list[0].Key, list[1].Key})
	if err != nil || n != 2 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if list, _ := s.ListQuarantined(ctx); len(list) != 0 {
		t.Fatalf("deleted chunks still listed: %+v", list)
	}
	if b, _ := s.Backlog(ctx); b.QuarantinedChunks != 0 {
		t.Fatalf("deleted chunks still counted: %+v", b)
	}
	if _, err := s.DeleteQuarantined(ctx, []string{s.chunkKey(0, member(id, start))}); err == nil {
		t.Fatal("deleting a key that is not quarantined must be refused")
	}
}

// Quarantine is fenced like Complete: a chunk that changed after it was
// read (a newer incarnation) is not moved away.
func TestQuarantineIsFenced(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	ref := storage.ChunkRef{Shard: 0, Series: 5, Start: layout.WindowStart(5, nowMs())}
	m := member(ref.Series, ref.Start)
	key := s.chunkKey(0, m)
	corrupt := []byte("0000000000001000x")
	client.Set(ctx, key, "0000000000002000", 0) // healthy newer incarnation
	client.ZAdd(ctx, s.inflightKey(0), redis.Z{Score: 0, Member: m})
	if s.quarantine(ctx, ref, corrupt, errors.New("test")) {
		t.Fatal("a newer incarnation must not be quarantined")
	}
	if client.Get(ctx, key).Val() != "0000000000002000" {
		t.Fatal("the newer incarnation was moved away")
	}
	if client.ZCard(ctx, s.inflightKey(0)).Val() != 1 {
		t.Fatal("the chunk must stay in flight to be retried after its lease")
	}
	// Same header, different length: samples were appended since the read.
	client.Set(ctx, key, "0000000000001000xyz", 0)
	if s.quarantine(ctx, ref, corrupt, errors.New("test")) {
		t.Fatal("a chunk that grew must not be quarantined")
	}
	// A chunk that disappeared is released.
	client.Del(ctx, key)
	if s.quarantine(ctx, ref, corrupt, errors.New("test")) {
		t.Fatal("a missing chunk cannot be quarantined")
	}
	if client.ZCard(ctx, s.inflightKey(0)).Val() != 0 {
		t.Fatal("a missing chunk must be released")
	}
	if list, _ := s.ListQuarantined(ctx); len(list) != 0 {
		t.Fatalf("nothing must be quarantined, got %+v", list)
	}
}

func TestRecoversFromScriptFlush(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: time.Hour.Milliseconds(), Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	res, err := s.Append(ctx, []storage.SeriesSamples{{ID: 2, Samples: []model.Sample{{T: nowMs(), V: 1}}}})
	if err != nil || res.Accepted != 1 {
		t.Fatalf("append after SCRIPT FLUSH: %+v %v", res, err)
	}
	if _, err := s.Claim(ctx, 0, 1); err != nil {
		t.Fatalf("claim after SCRIPT FLUSH: %v", err)
	}
}

func TestRefusesShardCountChange(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 1000}
	s, client := newTestStore(t, layout, time.Minute)
	layout.Shards = 8
	_, err := New(context.Background(), client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "shards") {
		t.Fatalf("expected shard mismatch error, got %v", err)
	}
}

func TestRefusesChunkRangeChange(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 1000}
	s, client := newTestStore(t, layout, time.Minute)
	layout.ChunkRange = 2000
	_, err := New(context.Background(), client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute})
	if err == nil || !strings.Contains(err.Error(), "chunk_range") {
		t.Fatalf("expected chunk_range mismatch error, got %v", err)
	}
}

func TestRefusesBackendChange(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 1000}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	opts := Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute, Backend: "cassandra"}
	if _, err := New(ctx, client, opts); err != nil {
		t.Fatal(err)
	}
	// Opening the store does not record the backend: a start that fails
	// to reach the persistent tier must not lock it in.
	opts.Backend = "tikv"
	tikv, err := New(ctx, client, opts)
	if err != nil {
		t.Fatalf("an unrecorded backend must not be refused: %v", err)
	}
	if err := tikv.RecordBackend(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tikv.RecordBackend(ctx); err != nil {
		t.Fatalf("recording the same backend again: %v", err)
	}
	opts.Backend = "cassandra"
	cassandra, err := New(ctx, client, opts)
	if err == nil || !strings.Contains(err.Error(), "storage.backend") {
		t.Fatalf("expected a backend mismatch error, got %v", err)
	}
	if cassandra != nil {
		t.Fatal("no store expected on a mismatch")
	}
}

func TestChunkSizeLimit(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: time.Hour.Milliseconds()}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	s2, err := New(ctx, client, Options{Prefix: s.prefix, Layout: layout, Lease: time.Minute, MaxChunkBytes: 16 + 3*16})
	if err != nil {
		t.Fatal(err)
	}
	now := nowMs()
	var samples []model.Sample
	for i := range 5 {
		samples = append(samples, model.Sample{T: now + int64(i), V: 1})
	}
	res, err := s2.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: samples[:3]}})
	if err != nil || res.Accepted != 3 {
		t.Fatalf("first append: %+v %v", res, err)
	}
	res, err = s2.Append(ctx, []storage.SeriesSamples{{ID: 1, Samples: samples[3:]}})
	if err != nil || res.ChunkFull != 2 || res.Accepted != 0 {
		t.Fatalf("append beyond the limit: %+v %v", res, err)
	}
}

func TestCompleteKeepsChunkThatGrew(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, 500*time.Millisecond)
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	refs := waitClaim(t, s, 0)
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("load: %v %v", chunks, err)
	}
	// A sample appended after the chunk was loaded (only possible after a
	// failover or clock jump) must not be deleted by Complete.
	key := s.chunkKey(0, member(refs[0].Series, refs[0].Start))
	client.Append(ctx, key, string(make([]byte, 16)))
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, key).Val() != 1 {
		t.Fatal("chunk that grew after loading was deleted")
	}
	// It is handed out again after the lease and can then be completed.
	again := waitClaim(t, s, 0)
	chunks, _, err = load(ctx, s, again)
	if err != nil || len(chunks) != 1 || len(chunks[0].Samples) != 2 {
		t.Fatalf("reload: %v %v", chunks, err)
	}
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, key).Val() != 0 {
		t.Fatal("chunk was not completed")
	}
}

func TestReleaseMissingChunks(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 5, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	refs := waitClaim(t, s, 0)
	client.Del(ctx, s.chunkKey(0, member(refs[0].Series, refs[0].Start)))
	chunks, missing, err := load(ctx, s, refs)
	if err != nil || len(chunks) != 0 || len(missing) != 1 {
		t.Fatalf("load: %v %v %v", chunks, missing, err)
	}
	if err := s.Release(ctx, missing); err != nil {
		t.Fatal(err)
	}
	if n := client.ZCard(ctx, s.inflightKey(0)).Val() + client.ZCard(ctx, s.inflightDueKey(0)).Val(); n != 0 {
		t.Fatalf("in-flight entries leaked: %d", n)
	}
}

func TestBacklogCountsInflightChunks(t *testing.T) {
	layout := model.Layout{Shards: 2, ChunkRange: 300, Grace: 100}
	s, _ := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	id := model.SeriesID(9)
	t0 := nowMs()
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: t0, V: 1}}}}); err != nil {
		t.Fatal(err)
	}
	waitClaim(t, s, layout.Shard(id))
	b, err := s.Backlog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := layout.WindowStart(id, t0)
	if b.InflightChunks != 1 || b.OldestDue != layout.Due(start) || b.OldestStart != start {
		t.Fatalf("in-flight chunk not reflected in backlog: %+v (start %d)", b, start)
	}
}

// TestClaimWorksAtMaxmemory needs a throwaway server whose maxmemory the
// test may change: XTSDB_TEST_REDIS_OOM_ADDR.
func TestClaimWorksAtMaxmemory(t *testing.T) {
	addr := os.Getenv("XTSDB_TEST_REDIS_OOM_ADDR")
	if addr == "" {
		t.Skip("XTSDB_TEST_REDIS_OOM_ADDR not set")
	}
	t.Setenv("XTSDB_TEST_REDIS_ADDRS", addr)
	t.Setenv("XTSDB_TEST_REDIS_CLUSTER", "")
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, 300*time.Millisecond)
	ctx := context.Background()
	t.Cleanup(func() { client.ConfigSet(context.Background(), "maxmemory", "0") })
	client.ConfigSet(ctx, "maxmemory-policy", "noeviction")
	for i := range 20 {
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: model.SeriesID(i), Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err != nil {
			t.Fatal(err)
		}
	}
	refs := waitClaim(t, s, 0)
	time.Sleep(400 * time.Millisecond) // let the leases expire
	// Far below the memory in use: Redis is now out of memory.
	if err := client.ConfigSet(ctx, "maxmemory", "100kb").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: 99, Samples: []model.Sample{{T: nowMs(), V: 1}}}}); err == nil ||
		!strings.Contains(err.Error(), "OOM") {
		t.Fatalf("precondition: append must be refused at maxmemory, got %v", err)
	}
	// A flusher that starts now must be able to open the store.
	if _, err := New(ctx, client, Options{Prefix: s.prefix, Layout: layout, Lease: 300 * time.Millisecond}); err != nil {
		t.Fatalf("opening the store must work at maxmemory once the layout is recorded: %v", err)
	}
	again, err := s.Claim(ctx, 0, 100)
	if err != nil || len(again) == 0 {
		t.Fatalf("claim must work at maxmemory: %d refs, %v", len(again), err)
	}
	chunks, _, err := load(ctx, s, again)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatalf("complete must work at maxmemory: %v", err)
	}
	_ = refs
}

func TestManySeriesAcrossShards(t *testing.T) {
	layout := model.Layout{Shards: 64, ChunkRange: time.Hour.Milliseconds(), Grace: 0}
	s, _ := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	now := nowMs()
	var batch []storage.SeriesSamples
	var ids []model.SeriesID
	for i := range 2000 {
		id := model.SeriesID(rand.Uint64())
		ids = append(ids, id)
		batch = append(batch, storage.SeriesSamples{ID: id, Samples: []model.Sample{{T: now, V: float64(i)}}})
	}
	res, err := s.Append(ctx, batch)
	if err != nil || res.Accepted != 2000 {
		t.Fatalf("append: %+v %v", res, err)
	}
	got, err := s.Read(ctx, ids, now-1, now+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2000 {
		t.Fatalf("read %d series, want 2000", len(got))
	}
}

// Fragments come from a hybrid clock per shard: they keep increasing when
// the Redis clock is behind the last fragment issued (a clock step
// backwards, or a failover to a node with a slower clock).
func TestFragmentsSurviveClockStep(t *testing.T) {
	layout := model.Layout{Shards: 1, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	fragment := func(id model.SeriesID) int64 {
		t.Helper()
		now := nowMs()
		if _, err := s.Append(ctx, []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: now, V: 1}}}}); err != nil {
			t.Fatal(err)
		}
		hdr := client.GetRange(ctx, s.chunkKey(0, member(id, layout.WindowStart(id, now))), 0, 15).Val()
		f, err := strconv.ParseInt(hdr, 10, 64)
		if err != nil {
			t.Fatalf("header %q: %v", hdr, err)
		}
		return f
	}
	first := fragment(1)
	if us := time.Now().UnixMicro(); first < us-int64(time.Minute/time.Microsecond) || first > us {
		t.Fatalf("fragment %d does not follow the server clock (%d)", first, us)
	}
	// The last fragment issued is ahead of the clock, as if the clock had
	// stepped back by a day.
	ahead := time.Now().Add(24 * time.Hour).UnixMicro()
	client.Set(ctx, s.clockKey(0), strconv.FormatInt(ahead, 10), 0)
	if f := fragment(2); f != ahead+1 {
		t.Fatalf("fragment %d, want %d", f, ahead+1)
	}
	if f := fragment(3); f != ahead+2 {
		t.Fatalf("fragment %d, want %d", f, ahead+2)
	}
	// Appending to an existing chunk does not advance the clock.
	_ = fragment(3)
	if got := client.Get(ctx, s.clockKey(0)).Val(); got != fmt.Sprintf("%016d", ahead+2) {
		t.Fatalf("shard clock %s, want %d", got, ahead+2)
	}
	// Chunks with such fragments are completed normally.
	refs := waitClaim(t, s, 0)
	chunks, _, err := load(ctx, s, refs)
	if err != nil || len(chunks) == 0 {
		t.Fatalf("load: %v %v", chunks, err)
	}
	if err := s.Complete(ctx, chunks); err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		want := 1
		if c.Ref.Series == 3 {
			want = 2
		}
		if c.Records != want {
			t.Fatalf("series %d: %d records, want %d", c.Ref.Series, c.Records, want)
		}
		if client.Exists(ctx, s.chunkKey(0, member(c.Ref.Series, c.Ref.Start))).Val() != 0 {
			t.Fatalf("chunk %+v was not completed", c.Ref)
		}
	}
}

// Quarantine keys of earlier builds (<prefix>:{N}:q:<series>:<window>, not
// in the quarantine index) are listed and deleted too.
func TestListsUnindexedQuarantineKeys(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 300, Grace: 0}
	s, client := newTestStore(t, layout, time.Minute)
	ctx := context.Background()
	old := s.tag(2) + ":q:" + member(0x2a, 1_700_000_000_000)
	client.Set(ctx, old, "0000000000000001xyz", time.Hour)
	list, err := s.ListQuarantined(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	if c := list[0]; c.Key != old || c.Shard != 2 || c.Series != 0x2a || c.Start != 1_700_000_000_000 ||
		c.Bytes != 19 || c.Expires.IsZero() {
		t.Fatalf("unexpected listing %+v", c)
	}
	if n, err := s.DeleteQuarantined(ctx, []string{old}); err != nil || n != 1 {
		t.Fatalf("delete: %d %v", n, err)
	}
	if globEscape(`a*b?[c]\`) != `a\*b\?\[c\]\\` {
		t.Fatalf("glob escaping is wrong: %s", globEscape(`a*b?[c]\`))
	}
}
