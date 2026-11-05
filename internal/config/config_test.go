package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadExampleConfig(t *testing.T) {
	t.Setenv("XTSDB_CASSANDRA_HOSTS", "c1:9042, c2:9042")
	t.Setenv("XTSDB_TIKV_PD_ENDPOINTS", "pd-0:2379")
	cfg, err := Load(filepath.Join("..", "..", "examples", "xtsdb.yaml"), true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.ChunkRange.D() != time.Hour {
		t.Fatalf("chunk_range = %v", cfg.Storage.ChunkRange)
	}
	if len(cfg.Cassandra.Hosts) != 2 || cfg.Redis.Addrs[0] != "127.0.0.1:6379" {
		t.Fatalf("env lists not expanded: %v %v", cfg.Cassandra.Hosts, cfg.Redis.Addrs)
	}
	// The example documents the defaults; only fields marked "example" differ.
	def := Default()
	if len(cfg.TiKV.PDEndpoints) != 1 || cfg.TiKV.PDEndpoints[0] != "pd-0:2379" {
		t.Fatalf("env lists not expanded: %v", cfg.TiKV.PDEndpoints)
	}
	cfg.Redis.Addrs, cfg.Cassandra.Hosts, cfg.TiKV.PDEndpoints = def.Redis.Addrs, def.Cassandra.Hosts, def.TiKV.PDEndpoints
	cfg.Cassandra.AutoMigrate = def.Cassandra.AutoMigrate
	cfg.Ingester.GraphiteListenAddress = def.Ingester.GraphiteListenAddress
	if got, want := fmt.Sprintf("%+v", cfg), fmt.Sprintf("%+v", def); got != want {
		t.Fatalf("example config differs from the defaults:\n%s\n%s", got, want)
	}
}

func TestLoadOverridesDefaultsAndExpandsEnv(t *testing.T) {
	t.Setenv("XT_REDIS", "redis-0:6379")
	path := filepath.Join(t.TempDir(), "c.yaml")
	err := os.WriteFile(path, []byte(`
redis:
  addrs: ["${XT_REDIS}"]
storage:
  retention: 30d
  chunk_range: 30m
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addrs[0] != "redis-0:6379" {
		t.Fatalf("env not expanded: %v", cfg.Redis.Addrs)
	}
	if cfg.Storage.Retention.D() != 30*24*time.Hour || cfg.Storage.ChunkRange.D() != 30*time.Minute {
		t.Fatalf("durations not parsed: %+v", cfg.Storage)
	}
	if cfg.Storage.Shards != 256 {
		t.Fatal("defaults must be kept for unset fields")
	}
}

func TestExpandEnvDefaults(t *testing.T) {
	t.Setenv("XT_SET", "v")
	t.Setenv("XT_EMPTY", "")
	got := ExpandEnv("${XT_SET:-d} ${XT_EMPTY:-d} ${XT_UNSET:-a:b} $XT_SET ${XT_UNSET} ${XT_EMPTY-d} ${XT_UNSET-d} p$$w$$")
	if got != "v d a:b v   d p$w$" {
		t.Fatalf("got %q", got)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, []byte("storage:\n  shard: 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, false); err == nil {
		t.Fatal("typo in a field name must be an error")
	}
}

func TestEmptyFileUsesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, false); err != nil {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*Config){
		"shards":                func(c *Config) { c.Storage.Shards = 0 },
		"key_prefix":            func(c *Config) { c.Storage.KeyPrefix = "a{b}" },
		"chunk_range":           func(c *Config) { c.Storage.ChunkRange = Duration(48 * time.Hour) },
		"hot_lookback":          func(c *Config) { c.Querier.HotLookback = Duration(time.Minute) },
		"log.level":             func(c *Config) { c.Log.Level = "verbose" },
		"retention":             func(c *Config) { c.Storage.Retention = Duration(20 * 365 * 24 * time.Hour) },
		"consistency":           func(c *Config) { c.Cassandra.Consistency = "ANY" },
		"keyspace":              func(c *Config) { c.Cassandra.Keyspace = "bad-name" },
		"pool_size":             func(c *Config) { c.Redis.PoolSize = -1 },
		"redis.addrs":           func(c *Config) { c.Redis.Addrs = []string{""} },
		"max_request_body_size": func(c *Config) { c.Ingester.MaxRequestBodySize = 1 << 62 },
		"index_buckets":         func(c *Config) { c.Storage.IndexBuckets = 0 },
		"max_sample_future":     func(c *Config) { c.Ingester.MaxSampleFuture = Duration(72 * time.Hour) },
		"quarantine_ttl":        func(c *Config) { c.Flusher.QuarantineTTL = Duration(time.Minute) },
	}
	for field, mutate := range cases {
		cfg := Default()
		mutate(cfg)
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: expected validation error, got %v", field, err)
		}
	}
}

func TestTiKVConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	err := os.WriteFile(path, []byte(`
storage:
  backend: tikv
tikv:
  pd_endpoints: [pd-0:2379, pd-1:2379]
  keyspace: metrics
cassandra:
  hosts: []   # not used with the tikv backend
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.Backend != BackendTiKV || len(cfg.TiKV.PDEndpoints) != 2 || cfg.TiKV.APIVersion != "v2" {
		t.Fatalf("unexpected config %+v", cfg.TiKV)
	}
	for _, c := range []struct {
		edit func(*Config)
		want string
	}{
		{func(c *Config) { c.Storage.Backend = "hbase" }, "storage.backend"},
		{func(c *Config) { c.TiKV.PDEndpoints = nil }, "tikv.pd_endpoints"},
		{func(c *Config) { c.TiKV.APIVersion = "v1" }, "tikv.api_version"},
		{func(c *Config) { c.TiKV.APIVersion = "v1ttl" }, "tikv.keyspace"},
		{func(c *Config) { c.TiKV.Timeout = 0 }, "tikv.timeout"},
		{func(c *Config) { c.TiKV.TLS = TLSConfig{Enabled: true} }, "tikv.tls.ca_file"},
		{func(c *Config) { c.TiKV.TLS = TLSConfig{Enabled: true, CAFile: "ca", CertFile: "c"} }, "tikv.tls.cert_file"},
		{func(c *Config) { c.TiKV.TLS = TLSConfig{Enabled: true, CAFile: "ca", InsecureSkipVerify: true} }, "not supported"},
		{func(c *Config) { c.Storage.KeyPrefix = strings.Repeat("x", 65) }, "storage.key_prefix"},
	} {
		bad := *cfg
		c.edit(&bad)
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("expected an error about %s, got %v", c.want, err)
		}
	}
	// The Cassandra settings are checked only with the cassandra backend.
	cfg.Storage.Backend = BackendCassandra
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "cassandra.hosts") {
		t.Fatalf("expected an error about cassandra.hosts, got %v", err)
	}
}

// The output of xtsdb config is a valid configuration file.
func TestMarshalRoundTrip(t *testing.T) {
	cfg := Default()
	cfg.Storage.Backend = BackendTiKV
	cfg.TiKV.Keyspace = "metrics"
	b, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := &Config{}
	if err := Parse(b, got); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", cfg) {
		t.Fatalf("round trip changed the configuration:\n%s", b)
	}
}
