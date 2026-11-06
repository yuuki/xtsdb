package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/redis/go-redis/v9"

	"github.com/yuuki/xtsdb/internal/backendtest"
	"github.com/yuuki/xtsdb/internal/config"
)

// TestEndToEnd runs all roles in one process against real Redis and every
// persistent backend available (see backendtest.Available). Set
// XTSDB_TEST_REDIS_ADDRS and XTSDB_TEST_CASSANDRA_HOSTS or
// XTSDB_TEST_TIKV_PD.
//
// It writes samples once and never again, then checks that they stay
// queryable while they move from Redis to the persistent tier and after
// Redis no longer holds them.
func TestEndToEnd(t *testing.T) {
	forEachBackend(t, testEndToEnd)
}

// forEachBackend runs a test against Redis and each available persistent
// backend.
func forEachBackend(t *testing.T, test func(t *testing.T, redisAddrs []string, backend string)) {
	redisAddrs := os.Getenv("XTSDB_TEST_REDIS_ADDRS")
	backends := backendtest.Available()
	if redisAddrs == "" || len(backends) == 0 {
		t.Skip("XTSDB_TEST_REDIS_ADDRS and XTSDB_TEST_CASSANDRA_HOSTS or XTSDB_TEST_TIKV_PD not set")
	}
	for _, b := range backends {
		t.Run(b, func(t *testing.T) { test(t, strings.Split(redisAddrs, ","), b) })
	}
}

func testEndToEnd(t *testing.T, redisAddrs []string, backend string) {
	cfg := config.Default()
	cfg.Log.Level = "warn"
	cfg.Server.HTTPListenAddress = "127.0.0.1:0"
	cfg.Server.GracefulShutdownTimeout = config.Duration(5 * time.Second)
	cfg.Redis.Addrs = redisAddrs
	cfg.Redis.ClusterMode = os.Getenv("XTSDB_TEST_REDIS_CLUSTER") == "1"
	cfg.Storage.KeyPrefix = fmt.Sprintf("xte2e%d", rand.Uint32())
	backendtest.Configure(t, cfg, backend)
	cfg.Storage.Shards = 16
	cfg.Storage.ChunkRange = config.Duration(2 * time.Second)
	cfg.Storage.GracePeriod = config.Duration(time.Second)
	cfg.Storage.Retention = config.Duration(24 * time.Hour)
	cfg.Ingester.GraphiteListenAddress = "127.0.0.1:0"
	cfg.Flusher.PollInterval = config.Duration(100 * time.Millisecond)
	cfg.Flusher.LeaseDuration = config.Duration(5 * time.Second)
	cfg.Querier.HotLookback = config.Duration(time.Hour)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, TargetAll, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var (
		stopOnce sync.Once
		runErr   error
	)
	stop := func() error {
		stopOnce.Do(func() {
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(15 * time.Second):
				runErr = errors.New("shutdown timed out")
			}
		})
		return runErr
	}
	// Cleanups run in reverse order: stop the app before dropping its data.
	t.Cleanup(func() { cleanupRedis(t, cfg) })
	t.Cleanup(func() { _ = stop() })
	base := "http://" + a.Server.Addr().String()

	// Remote write: two samples of one series, and two native histograms.
	now := time.Now()
	ts := now.Add(-500 * time.Millisecond).UnixMilli()
	hist := func(n uint64) *histogram.Histogram {
		return &histogram.Histogram{Schema: 0, Count: n, Sum: float64(n), PositiveSpans: []histogram.Span{{Length: 1}},
			PositiveBuckets: []int64{int64(n)}}
	}
	wr := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "e2e_up"}, {Name: "job", Value: "e2e"}},
		Samples: []prompb.Sample{{Timestamp: ts - 100, Value: 1}, {Timestamp: ts, Value: 2}},
	}, {
		Labels:     []prompb.Label{{Name: "__name__", Value: "e2e_latency"}, {Name: "job", Value: "e2e"}},
		Histograms: []prompb.Histogram{prompb.FromIntHistogram(ts-100, hist(3)), prompb.FromIntHistogram(ts, hist(5))},
	}}}
	body, _ := wr.Marshal()
	resp, err := http.Post(base+"/api/v1/write", "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, body)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("remote write status %d", resp.StatusCode)
	}

	// Graphite: one tagged sample.
	conn, err := net.Dial("tcp", a.Graphite.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "servers.web01.load;dc=tokyo 0.75 %d\n", now.Unix())
	conn.Close()

	query := func(q string) []any {
		t.Helper()
		params := url.Values{"query": {q}, "time": {strconv.FormatFloat(float64(time.Now().UnixMilli())/1000, 'f', 3, 64)}}
		var out struct {
			Status string `json:"status"`
			Data   struct {
				Result []any `json:"result"`
			} `json:"data"`
			Error string `json:"error"`
		}
		resp, err := http.Get(base + "/api/v1/query?" + params.Encode())
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if out.Status != "success" {
			t.Fatalf("query %q failed: %s", q, out.Error)
		}
		return out.Data.Result
	}
	countSamples := func(q string) int {
		r := query(q)
		if len(r) == 0 {
			return 0
		}
		return len(r[0].(map[string]any)["values"].([]any))
	}

	// While the samples are in Redis.
	waitFor(t, "graphite sample", func() bool { return len(query(`{"servers.web01.load", dc="tokyo"}`)) == 1 })
	if n := countSamples(`e2e_up{job="e2e"}[1m]`); n != 2 {
		t.Fatalf("hot tier: got %d samples, want 2", n)
	}
	histogramCount := func() string {
		r := query(`histogram_count(e2e_latency)`)
		if len(r) != 1 {
			return fmt.Sprint(r)
		}
		return r[0].(map[string]any)["value"].([]any)[1].(string)
	}
	if c := histogramCount(); c != "5" {
		t.Fatalf("hot tier: histogram_count = %s, want 5", c)
	}

	// The windows close a few seconds later; the flusher must move them.
	waitFor(t, "flush to "+backend, func() bool {
		b, err := a.Hot.Backlog(context.Background())
		return err == nil && b.PendingChunks == 0 && b.InflightChunks == 0
	})
	if n := countSamples(`e2e_up{job="e2e"}[1m]`); n != 2 {
		t.Fatalf("cold tier: got %d samples, want 2", n)
	}
	if r := query(`{"servers.web01.load"}[1m]`); len(r) != 1 {
		t.Fatalf("cold tier: graphite series missing: %v", r)
	}
	if c := histogramCount(); c != "5" {
		t.Fatalf("cold tier: histogram_count = %s, want 5", c)
	}
	if r := query(`e2e_latency[1m]`); len(r) != 1 || len(r[0].(map[string]any)["histograms"].([]any)) != 2 {
		t.Fatalf("cold tier: histograms %v", r)
	}

	// Metadata API and metrics.
	resp, err = http.Get(base + "/api/v1/label/job/values")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"e2e"`) {
		t.Fatalf("label values: %s", b)
	}
	resp, err = http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `xtsdb_flusher_chunks_total{outcome="flushed"}`) {
		t.Fatal("flusher metrics missing")
	}
	resp, err = http.Get(base + "/-/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ready = %d", resp.StatusCode)
	}

	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func cleanupRedis(t *testing.T, cfg *config.Config) {
	ctx := context.Background()
	client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: cfg.Redis.Addrs, IsClusterMode: cfg.Redis.ClusterMode})
	defer client.Close()
	del := func(ctx context.Context, c *redis.Client) error {
		iter := c.Scan(ctx, 0, cfg.Storage.KeyPrefix+":*", 1000).Iterator()
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
