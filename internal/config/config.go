// Package config defines the xtsdb configuration file format.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v4"
)

// Config is the root of the xtsdb configuration.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Log       LogConfig       `yaml:"log"`
	Redis     RedisConfig     `yaml:"redis"`
	Cassandra CassandraConfig `yaml:"cassandra"`
	Storage   StorageConfig   `yaml:"storage"`
	Ingester  IngesterConfig  `yaml:"ingester"`
	Flusher   FlusherConfig   `yaml:"flusher"`
	Querier   QuerierConfig   `yaml:"querier"`
}

// ServerConfig configures the HTTP server shared by every role.
type ServerConfig struct {
	HTTPListenAddress       string   `yaml:"http_listen_address"`
	GracefulShutdownTimeout Duration `yaml:"graceful_shutdown_timeout"`
	EnablePprof             bool     `yaml:"enable_pprof"`
}

// LogConfig configures structured logging.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // logfmt, json
}

// TLSConfig configures a TLS client.
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	ServerName         string `yaml:"server_name"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// RedisConfig configures the in-memory tier.
//
// A single address connects to a standalone server, several addresses (or
// cluster_mode) connect to Redis Cluster, and master_name connects through
// Redis Sentinel. Valkey is supported as a drop-in replacement.
type RedisConfig struct {
	Addrs        []string  `yaml:"addrs"`
	ClusterMode  bool      `yaml:"cluster_mode"`
	MasterName   string    `yaml:"master_name"`
	Username     string    `yaml:"username"`
	Password     string    `yaml:"password"`
	DB           int       `yaml:"db"`
	PoolSize     int       `yaml:"pool_size"`
	DialTimeout  Duration  `yaml:"dial_timeout"`
	ReadTimeout  Duration  `yaml:"read_timeout"`
	WriteTimeout Duration  `yaml:"write_timeout"`
	TLS          TLSConfig `yaml:"tls"`
}

// CassandraConfig configures the persistent tier and the series index.
type CassandraConfig struct {
	Hosts             []string       `yaml:"hosts"`
	Keyspace          string         `yaml:"keyspace"`
	LocalDC           string         `yaml:"local_dc"`
	Consistency       string         `yaml:"consistency"`
	Username          string         `yaml:"username"`
	Password          string         `yaml:"password"`
	Timeout           Duration       `yaml:"timeout"`
	ConnectTimeout    Duration       `yaml:"connect_timeout"`
	NumConns          int            `yaml:"num_conns"`
	TLS               TLSConfig      `yaml:"tls"`
	AutoMigrate       bool           `yaml:"auto_migrate"`
	ReplicationClass  string         `yaml:"replication_class"`
	ReplicationFactor int            `yaml:"replication_factor"`
	Datacenters       map[string]int `yaml:"datacenters"`
}

// StorageConfig holds settings that define the on-storage layout. Changing
// key_prefix or shards on an existing deployment orphans the data written
// with the previous values, so xtsdb refuses to start when shards changes.
type StorageConfig struct {
	KeyPrefix   string   `yaml:"key_prefix"`
	Shards      int      `yaml:"shards"`
	ChunkRange  Duration `yaml:"chunk_range"`
	GracePeriod Duration `yaml:"grace_period"`
	Retention   Duration `yaml:"retention"`
}

// IngesterConfig configures the write path.
type IngesterConfig struct {
	GraphiteListenAddress  string   `yaml:"graphite_listen_address"`
	GraphiteMaxConnections int      `yaml:"graphite_max_connections"`
	MaxRequestBodySize     int64    `yaml:"max_request_body_size"`
	MaxDecompressedSize    int64    `yaml:"max_decompressed_size"`
	MaxSeriesPerRequest    int      `yaml:"max_series_per_request"`
	MaxSamplesPerRequest   int      `yaml:"max_samples_per_request"`
	MaxInflightRequests    int      `yaml:"max_inflight_requests"`
	MaxSampleFuture        Duration `yaml:"max_sample_future"`
	IndexCacheSize         int      `yaml:"index_cache_size"`
	IndexWriteConcurrency  int      `yaml:"index_write_concurrency"`
}

// FlusherConfig configures the tiering process.
type FlusherConfig struct {
	PollInterval  Duration `yaml:"poll_interval"`
	BatchSize     int      `yaml:"batch_size"`
	Concurrency   int      `yaml:"concurrency"`
	LeaseDuration Duration `yaml:"lease_duration"`
	// QuarantineTTL is how long undecodable chunks are kept in Redis for
	// inspection. Zero keeps them until they are deleted.
	QuarantineTTL Duration `yaml:"quarantine_ttl"`
}

// QuerierConfig configures the read path.
type QuerierConfig struct {
	LookbackDelta     Duration `yaml:"lookback_delta"`
	Timeout           Duration `yaml:"timeout"`
	MaxSamples        int      `yaml:"max_samples"`
	MaxSeriesPerQuery int      `yaml:"max_series_per_query"`
	HotLookback       Duration `yaml:"hot_lookback"`
	MaxQueryLookback  Duration `yaml:"max_query_lookback"`
	MaxConcurrent     int      `yaml:"max_concurrent_queries"`
	SeriesCacheSize   int      `yaml:"series_cache_size"`
	ReadConcurrency   int      `yaml:"read_concurrency"`
}

// Default returns a configuration populated with defaults suitable for a
// single-node development setup.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			HTTPListenAddress:       ":9201",
			GracefulShutdownTimeout: Duration(30 * time.Second),
		},
		Log: LogConfig{Level: "info", Format: "logfmt"},
		Redis: RedisConfig{
			Addrs:        []string{"127.0.0.1:6379"},
			DialTimeout:  Duration(5 * time.Second),
			ReadTimeout:  Duration(3 * time.Second),
			WriteTimeout: Duration(3 * time.Second),
		},
		Cassandra: CassandraConfig{
			Hosts:             []string{"127.0.0.1:9042"},
			Keyspace:          "xtsdb",
			Consistency:       "LOCAL_QUORUM",
			Timeout:           Duration(10 * time.Second),
			ConnectTimeout:    Duration(10 * time.Second),
			NumConns:          2,
			ReplicationClass:  "SimpleStrategy",
			ReplicationFactor: 1,
		},
		Storage: StorageConfig{
			KeyPrefix:   "xt",
			Shards:      256,
			ChunkRange:  Duration(time.Hour),
			GracePeriod: Duration(10 * time.Minute),
			Retention:   Duration(90 * 24 * time.Hour),
		},
		Ingester: IngesterConfig{
			GraphiteListenAddress:  "",
			GraphiteMaxConnections: 1024,
			MaxRequestBodySize:     10 << 20,
			MaxDecompressedSize:    32 << 20,
			MaxSeriesPerRequest:    100_000,
			MaxSamplesPerRequest:   1_000_000,
			MaxInflightRequests:    16,
			MaxSampleFuture:        Duration(10 * time.Minute),
			IndexCacheSize:         2_000_000,
			IndexWriteConcurrency:  64,
		},
		Flusher: FlusherConfig{
			PollInterval:  Duration(time.Second),
			BatchSize:     128,
			Concurrency:   8,
			LeaseDuration: Duration(2 * time.Minute),
			QuarantineTTL: Duration(30 * 24 * time.Hour),
		},
		Querier: QuerierConfig{
			LookbackDelta:     Duration(5 * time.Minute),
			Timeout:           Duration(2 * time.Minute),
			MaxSamples:        50_000_000,
			MaxSeriesPerQuery: 100_000,
			HotLookback:       Duration(6 * time.Hour),
			MaxQueryLookback:  0,
			MaxConcurrent:     20,
			SeriesCacheSize:   1_000_000,
			ReadConcurrency:   64,
		},
	}
}

// Load reads a YAML configuration file on top of the defaults. When expandEnv
// is true, ${VAR}, ${VAR:-default} and $VAR references are replaced with
// environment values before parsing.
func Load(path string, expandEnv bool) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, cfg.Validate()
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if expandEnv {
		b = []byte(ExpandEnv(string(b)))
	}
	if err := Parse(b, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, cfg.Validate()
}

// ExpandEnv replaces ${VAR}, ${VAR:-default}, ${VAR-default} and $VAR with
// environment values, like a POSIX shell: ":-" uses the default when VAR is
// unset or empty, "-" only when it is unset. "$$" is a literal "$".
// Expansion is textual and happens before YAML parsing, so quote values
// that may contain YAML syntax such as '#' or ': '.
func ExpandEnv(s string) string {
	return os.Expand(s, func(name string) string {
		if name == "$" {
			return "$"
		}
		if v, def, ok := strings.Cut(name, ":-"); ok {
			if val := os.Getenv(v); val != "" {
				return val
			}
			return def
		}
		if v, def, ok := strings.Cut(name, "-"); ok {
			if val, set := os.LookupEnv(v); set {
				return val
			}
			return def
		}
		return os.Getenv(name)
	})
}

// Parse decodes YAML into cfg, rejecting unknown fields.
func Parse(b []byte, cfg *Config) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// consistencyLevels are the levels valid for both reads and writes. For the
// hand-off to be invisible to queries with replication, reads and writes
// must overlap (QUORUM or LOCAL_QUORUM with RF >= 3).
var consistencyLevels = []string{"ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "LOCAL_ONE"}

func validKeyspace(s string) bool {
	if s == "" || len(s) > 48 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// Validate checks the configuration for values that would make xtsdb
// misbehave and returns a descriptive error for the first one found.
func (c *Config) Validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}
	check(c.Server.HTTPListenAddress != "", "server.http_listen_address must be set")
	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level must be one of debug, info, warn, error"))
	}
	switch strings.ToLower(c.Log.Format) {
	case "logfmt", "json":
	default:
		errs = append(errs, fmt.Errorf("log.format must be logfmt or json"))
	}
	check(len(c.Redis.Addrs) > 0 && !slices.Contains(c.Redis.Addrs, ""), "redis.addrs must contain non-empty addresses")
	check(c.Redis.PoolSize >= 0, "redis.pool_size must not be negative")
	check(c.Redis.DB >= 0, "redis.db must not be negative")
	check(len(c.Cassandra.Hosts) > 0 && !slices.Contains(c.Cassandra.Hosts, ""), "cassandra.hosts must contain non-empty hosts")
	check(validKeyspace(c.Cassandra.Keyspace), "cassandra.keyspace must be 1-48 letters, digits or underscores, starting with a letter")
	check(slices.Contains(consistencyLevels, strings.ToUpper(c.Cassandra.Consistency)),
		"cassandra.consistency must be one of %s", strings.Join(consistencyLevels, ", "))
	check(c.Cassandra.NumConns >= 0, "cassandra.num_conns must not be negative")
	check(c.Cassandra.ReplicationClass == "SimpleStrategy" || c.Cassandra.ReplicationClass == "NetworkTopologyStrategy",
		"cassandra.replication_class must be SimpleStrategy or NetworkTopologyStrategy")
	check(c.Storage.KeyPrefix != "" && !strings.ContainsAny(c.Storage.KeyPrefix, "{}: "),
		"storage.key_prefix must be non-empty and must not contain '{', '}', ':' or spaces")
	check(c.Storage.Shards > 0 && c.Storage.Shards <= 16384, "storage.shards must be in [1, 16384]")
	check(c.Storage.ChunkRange.D() >= time.Second && c.Storage.ChunkRange.D() <= 24*time.Hour,
		"storage.chunk_range must be between 1s and 24h")
	check(c.Storage.ChunkRange.D()%time.Millisecond == 0, "storage.chunk_range must be a whole number of milliseconds")
	check(c.Storage.GracePeriod.D() >= 0, "storage.grace_period must not be negative")
	// Index entries live two days longer than samples, within Cassandra's
	// 20 year TTL limit.
	check(c.Storage.Retention.D() >= 0 && c.Storage.Retention.D() <= 20*365*24*time.Hour-48*time.Hour,
		"storage.retention must be between 0 (forever) and 20y minus 2d")
	check(c.Ingester.MaxRequestBodySize > 0 && c.Ingester.MaxRequestBodySize <= 1<<30,
		"ingester.max_request_body_size must be in (0, 1GiB]")
	check(c.Ingester.MaxDecompressedSize >= c.Ingester.MaxRequestBodySize && c.Ingester.MaxDecompressedSize <= 1<<30,
		"ingester.max_decompressed_size must be in [max_request_body_size, 1GiB]")
	check(c.Ingester.MaxSeriesPerRequest > 0, "ingester.max_series_per_request must be positive")
	check(c.Ingester.MaxSamplesPerRequest > 0, "ingester.max_samples_per_request must be positive")
	check(c.Ingester.MaxInflightRequests > 0, "ingester.max_inflight_requests must be positive")
	check(c.Ingester.GraphiteMaxConnections > 0, "ingester.graphite_max_connections must be positive")
	check(c.Ingester.MaxSampleFuture.D() > 0, "ingester.max_sample_future must be positive")
	check(c.Ingester.IndexCacheSize > 0, "ingester.index_cache_size must be positive")
	check(c.Ingester.IndexWriteConcurrency > 0, "ingester.index_write_concurrency must be positive")
	check(c.Flusher.PollInterval.D() > 0, "flusher.poll_interval must be positive")
	check(c.Flusher.BatchSize > 0, "flusher.batch_size must be positive")
	check(c.Flusher.Concurrency > 0, "flusher.concurrency must be positive")
	check(c.Flusher.LeaseDuration.D() >= time.Second, "flusher.lease_duration must be at least 1s")
	check(c.Flusher.QuarantineTTL.D() == 0 || c.Flusher.QuarantineTTL.D() >= time.Hour,
		"flusher.quarantine_ttl must be 0 (keep until deleted) or at least 1h")
	check(c.Querier.LookbackDelta.D() > 0, "querier.lookback_delta must be positive")
	check(c.Querier.Timeout.D() > 0, "querier.timeout must be positive")
	check(c.Querier.MaxSamples > 0, "querier.max_samples must be positive")
	check(c.Querier.SeriesCacheSize > 0, "querier.series_cache_size must be positive")
	check(c.Querier.ReadConcurrency > 0, "querier.read_concurrency must be positive")
	check(c.Querier.MaxConcurrent > 0, "querier.max_concurrent_queries must be positive")
	check(c.Querier.MaxQueryLookback.D() >= 0, "querier.max_query_lookback must not be negative")
	check(c.Querier.HotLookback.D() >= c.Storage.ChunkRange.D()+c.Storage.GracePeriod.D(),
		"querier.hot_lookback must be at least storage.chunk_range + storage.grace_period")
	return errors.Join(errs...)
}
