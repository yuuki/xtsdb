package hot

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/faultnet"
	"github.com/yuuki/xtsdb/internal/flush"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/storage/storagetest"
)

// faultReadTimeout is the read and write timeout of the Redis clients of the
// fault tests.
const faultReadTimeout = 200 * time.Millisecond

// newProxiedStore returns a Store that reaches the Redis server of
// XTSDB_TEST_REDIS_ADDRS through a proxy that injects network faults. Redis
// Cluster redirects clients to the addresses of its nodes, around the proxy,
// so these tests need a standalone server.
func newProxiedStore(t *testing.T, layout model.Layout, lease time.Duration) (*Store, *faultnet.Proxy) {
	t.Helper()
	addrs := os.Getenv("XTSDB_TEST_REDIS_ADDRS")
	if addrs == "" {
		t.Skip("XTSDB_TEST_REDIS_ADDRS not set")
	}
	if os.Getenv("XTSDB_TEST_REDIS_CLUSTER") == "1" || strings.Contains(addrs, ",") {
		t.Skip("fault injection needs a standalone Redis")
	}
	p := faultnet.New(t, addrs)
	cfg := config.Default().Redis
	cfg.Addrs = []string{p.Addr()}
	cfg.DialTimeout = config.Duration(time.Second)
	cfg.ReadTimeout = config.Duration(faultReadTimeout)
	cfg.WriteTimeout = config.Duration(faultReadTimeout)
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("xttest%d", rand.Uint64())
	s, err := New(context.Background(), client, Options{Prefix: prefix, Layout: layout, Lease: lease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.SetMode(faultnet.Pass)
		deleteKeys(t, client, prefix)
		_ = client.Close()
	})
	return s, p
}

// A partitioned Redis makes every call fail within a bound set by the
// client timeouts, instead of hanging, and the store recovers once the
// partition heals. Without a context deadline the bound is several read
// timeouts: the client retries, and dials and initializes new connections
// that stall too (about 12 read timeouts here).
func TestPartitionedRedisFailsFast(t *testing.T) {
	layout := model.Layout{Shards: 2, ChunkRange: time.Hour.Milliseconds(), Grace: time.Minute.Milliseconds()}
	s, p := newProxiedStore(t, layout, time.Minute)
	ctx := context.Background()
	samples := []storage.SeriesSamples{{ID: 1, Samples: []model.Sample{{T: nowMs(), V: 1}}}}
	ops := []struct {
		name string
		op   func() error
	}{
		{"append", func() error { _, err := s.Append(ctx, samples); return err }},
		{"claim", func() error { _, err := s.Claim(ctx, 0, 10); return err }},
		{"read", func() error { _, err := s.Read(ctx, []model.SeriesID{1}, 0, nowMs()); return err }},
	}
	for _, mode := range []faultnet.Mode{faultnet.Stall, faultnet.Reset} {
		p.SetMode(mode)
		for _, o := range ops {
			start := time.Now()
			err := o.op()
			// The client retries a few times with back-off; 5 s is far
			// beyond that and far below "hanging".
			elapsed := time.Since(start)
			t.Logf("mode %d: %s failed after %v: %v", mode, o.name, elapsed.Round(time.Millisecond), err)
			if err == nil || elapsed > 5*time.Second {
				t.Errorf("mode %d: %s returned %v after %v, want an error within 5s (read timeout %v)",
					mode, o.name, err, elapsed, faultReadTimeout)
			}
		}
		// A context deadline bounds a call too: the client checks it
		// between attempts, so it returns at most a read timeout late.
		dctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		start := time.Now()
		_, err := s.Append(dctx, samples)
		cancel()
		if elapsed := time.Since(start); err == nil || elapsed > 300*time.Millisecond+2*faultReadTimeout+time.Second {
			t.Errorf("mode %d: append with a deadline of 300ms returned %v after %v", mode, err, elapsed)
		}
		p.SetMode(faultnet.Pass)
		for _, o := range ops {
			if err := o.op(); err != nil {
				t.Errorf("mode %d: %s after the partition healed: %v", mode, o.name, err)
			}
		}
	}
}

// While connections to Redis stall, reset or lose their replies, an
// ingester keeps appending (retrying failed batches like Prometheus) and a
// flusher keeps moving chunks to the cold tier. Once the faults stop:
// every acknowledged sample is in the cold tier with its value, nothing
// else is, and the hot tier is empty. Redis scripts are atomic, so a fault
// can only make a call take effect or not, never half.
func TestNetworkFaultsLoseNoAcknowledgedSample(t *testing.T) {
	layout := model.Layout{Shards: 4, ChunkRange: 400, Grace: 100}
	const lease = 600 * time.Millisecond
	s, p := newProxiedStore(t, layout, lease)
	cold := storagetest.NewCold(layout.ChunkRange)
	f := flush.New(s, cold, flush.Options{PollInterval: 20 * time.Millisecond, BatchSize: 8, Concurrency: 2, Lease: lease})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ran := make(chan error, 1)
	go func() { ran <- f.Run(ctx) }()

	const chaos = 3 * time.Second
	stop := time.Now().Add(chaos)
	var wg sync.WaitGroup
	wg.Go(func() {
		rng := rand.New(rand.NewPCG(1, 2))
		modes := []faultnet.Mode{faultnet.Stall, faultnet.LoseReplies, faultnet.Reset}
		for time.Now().Before(stop) {
			p.SetMode(faultnet.Pass)
			time.Sleep(time.Duration(50+rng.IntN(150)) * time.Millisecond)
			p.SetMode(modes[rng.IntN(len(modes))])
			time.Sleep(time.Duration(50+rng.IntN(250)) * time.Millisecond)
		}
		p.SetMode(faultnet.Pass)
	})

	type key struct {
		id model.SeriesID
		t  int64
	}
	sent := make(map[key]float64) // every sample sent, acknowledged or not
	var acked []key
	ids := make([]model.SeriesID, 16)
	last := make(map[model.SeriesID]int64)
	for i := range ids {
		ids[i] = model.SeriesID(uint64(i)<<32 | uint64(i)*7919)
	}
	appendCtx := context.Background()
	for v := 0; time.Now().Before(stop); v++ {
		for _, id := range ids {
			ts := max(nowMs(), last[id]+1)
			last[id] = ts
			k := key{id, ts}
			sent[k] = float64(v)
			batch := []storage.SeriesSamples{{ID: id, Samples: []model.Sample{{T: ts, V: float64(v)}}}}
			for range 3 { // a failed batch is retried as a whole
				res, err := s.Append(appendCtx, batch)
				if err == nil {
					if res.Accepted == 1 {
						acked = append(acked, k)
					}
					break
				}
			}
		}
	}
	wg.Wait()
	if len(acked) == 0 {
		t.Fatal("no append was acknowledged")
	}

	// Drain: every window closes, and leases taken by calls whose reply
	// was lost expire.
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := s.Backlog(context.Background())
		if err == nil && b.PendingChunks == 0 && b.InflightChunks == 0 {
			if b.QuarantinedChunks != 0 {
				t.Fatalf("%d chunks quarantined", b.QuarantinedChunks)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("hot tier not drained: %+v, %v", b, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}

	got, err := cold.Read(context.Background(), ids, 0, nowMs())
	if err != nil {
		t.Fatal(err)
	}
	stored := make(map[key]float64)
	for id, samples := range got {
		for _, smp := range samples {
			k := key{id, smp.T}
			v, ok := sent[k]
			if !ok || v != smp.V {
				t.Fatalf("cold tier holds %v of series %v, which was never sent", smp, id)
			}
			stored[k] = smp.V
		}
	}
	for _, k := range acked {
		if _, ok := stored[k]; !ok {
			t.Fatalf("acknowledged sample %v lost (%d acknowledged, %d sent, %d stored)", k, len(acked), len(sent), len(stored))
		}
	}
	t.Logf("%d samples sent, %d acknowledged, %d stored", len(sent), len(acked), len(stored))
}
