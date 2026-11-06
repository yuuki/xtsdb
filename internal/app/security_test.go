package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/yuuki/xtsdb/internal/backendtest"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/tlstest"
)

// TestSecurity serves the API over TLS with separate write and read
// credentials. Set XTSDB_TEST_REDIS_ADDRS and XTSDB_TEST_CASSANDRA_HOSTS or
// XTSDB_TEST_TIKV_PD; it runs against one persistent backend.
func TestSecurity(t *testing.T) {
	redisAddrs := os.Getenv("XTSDB_TEST_REDIS_ADDRS")
	backends := backendtest.Available()
	if redisAddrs == "" || len(backends) == 0 {
		t.Skip("XTSDB_TEST_REDIS_ADDRS and XTSDB_TEST_CASSANDRA_HOSTS or XTSDB_TEST_TIKV_PD not set")
	}
	f := tlstest.Generate(t, t.TempDir())
	cfg := config.Default()
	cfg.Server.HTTPListenAddress = "127.0.0.1:0"
	cfg.Server.GracefulShutdownTimeout = config.Duration(5 * time.Second)
	cfg.Server.EnablePprof = true
	cfg.Server.TLS = config.ServerTLSConfig{CertFile: f.ServerCert, KeyFile: f.ServerKey}
	cfg.Server.Auth.Write.BasicAuth = []config.BasicAuthUser{{Username: "prometheus", Password: "write-secret"}}
	cfg.Server.Auth.Read.BearerTokens = []string{"read-token"}
	cfg.Redis.Addrs = strings.Split(redisAddrs, ",")
	cfg.Redis.ClusterMode = os.Getenv("XTSDB_TEST_REDIS_CLUSTER") == "1"
	cfg.Storage.KeyPrefix = fmt.Sprintf("xtsec%d", rand.Uint32())
	backendtest.Configure(t, cfg, backends[0])
	cfg.Storage.Shards = 4
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupRedis(t, cfg) })

	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, TargetAll, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	base := "https://" + a.Server.Addr().String()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: f.ClientConfig(t, false)}, Timeout: 10 * time.Second}
	body, err := (&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "secured"}},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: 1}},
	}}}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, user, password, token string) int {
		t.Helper()
		var rd io.Reader
		if method == http.MethodPost {
			rd = bytes.NewReader(snappy.Encode(nil, body))
		}
		req, err := http.NewRequest(method, base+path, rd)
		if err != nil {
			t.Fatal(err)
		}
		if method == http.MethodPost {
			req.Header.Set("Content-Type", "application/x-protobuf")
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	const query = "/api/v1/query?query=secured"
	cases := []struct {
		name, method, path    string
		user, password, token string
		want                  int
	}{
		{"healthy is open", http.MethodGet, "/-/healthy", "", "", "", http.StatusOK},
		{"root page needs read credentials", http.MethodGet, "/", "", "", "", http.StatusUnauthorized},
		{"ready is open", http.MethodGet, "/-/ready", "", "", "", http.StatusOK},
		{"write without credentials", http.MethodPost, "/api/v1/write", "", "", "", http.StatusUnauthorized},
		{"write with a wrong password", http.MethodPost, "/api/v1/write", "prometheus", "nope", "", http.StatusUnauthorized},
		{"write with read credentials", http.MethodPost, "/api/v1/write", "", "", "read-token", http.StatusUnauthorized},
		{"write", http.MethodPost, "/api/v1/write", "prometheus", "write-secret", "", http.StatusNoContent},
		{"query without credentials", http.MethodGet, query, "", "", "", http.StatusUnauthorized},
		{"query with write credentials", http.MethodGet, query, "prometheus", "write-secret", "", http.StatusUnauthorized},
		{"query", http.MethodGet, query, "", "", "read-token", http.StatusOK},
		{"metrics without credentials", http.MethodGet, "/metrics", "", "", "", http.StatusUnauthorized},
		{"metrics", http.MethodGet, "/metrics", "", "", "read-token", http.StatusOK},
		{"pprof without credentials", http.MethodGet, "/debug/pprof/", "", "", "", http.StatusUnauthorized},
	}
	for _, c := range cases {
		if got := do(c.method, c.path, c.user, c.password, c.token); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
	// Plain HTTP is not served.
	resp, err := http.Get("http://" + a.Server.Addr().String() + "/-/healthy")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP must not be served when TLS is enabled")
		}
	}
}
