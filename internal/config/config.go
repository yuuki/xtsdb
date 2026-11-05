// Package config defines the xtsdb configuration file format.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
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
	GraphiteListenAddress string   `yaml:"graphite_listen_address"`
	MaxRequestBodySize    int64    `yaml:"max_request_body_size"`
	MaxSampleFuture       Duration `yaml:"max_sample_future"`
	IndexCacheSize        int      `yaml:"index_cache_size"`
	IndexWriteConcurrency int      `yaml:"index_write_concurrency"`
}

// FlusherConfig configures the tiering process.
type FlusherConfig struct {
	PollInterval  Duration `yaml:"poll_interval"`
	BatchSize     int      `yaml:"batch_size"`
	Concurrency   int      `yaml:"concurrency"`
	LeaseDuration Duration `yaml:"lease_duration"`
}

// QuerierConfig configures the read path.
type QuerierConfig struct {
	LookbackDelta     Duration `yaml:"lookback_delta"`
	Timeout           Duration `yaml:"timeout"`
	MaxSamples        int      `yaml:"max_samples"`
	MaxSeriesPerQuery int      `yaml:"max_series_per_query"`
	HotLookback       Duration `yaml:"hot_lookback"`
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
			GraphiteListenAddress: "",
			MaxRequestBodySize:    32 << 20,
			MaxSampleFuture:       Duration(10 * time.Minute),
			IndexCacheSize:        2_000_000,
			IndexWriteConcurrency: 64,
		},
		Flusher: FlusherConfig{
			PollInterval:  Duration(time.Second),
			BatchSize:     128,
			Concurrency:   8,
			LeaseDuration: Duration(2 * time.Minute),
		},
		Querier: QuerierConfig{
			LookbackDelta:     Duration(5 * time.Minute),
			Timeout:           Duration(2 * time.Minute),
			MaxSamples:        50_000_000,
			MaxSeriesPerQuery: 100_000,
			HotLookback:       Duration(6 * time.Hour),
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

// ExpandEnv replaces ${VAR}, ${VAR:-default} and $VAR with environment
// values. With ":-", the default is used when VAR is unset or empty.
func ExpandEnv(s string) string {
	return os.Expand(s, func(name string) string {
		if v, def, ok := strings.Cut(name, ":-"); ok {
			if val := os.Getenv(v); val != "" {
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
	check(len(c.Redis.Addrs) > 0, "redis.addrs must contain at least one address")
	check(len(c.Cassandra.Hosts) > 0, "cassandra.hosts must contain at least one host")
	check(c.Cassandra.Keyspace != "", "cassandra.keyspace must be set")
	check(c.Cassandra.ReplicationClass == "SimpleStrategy" || c.Cassandra.ReplicationClass == "NetworkTopologyStrategy",
		"cassandra.replication_class must be SimpleStrategy or NetworkTopologyStrategy")
	check(c.Storage.KeyPrefix != "" && !strings.ContainsAny(c.Storage.KeyPrefix, "{}: "),
		"storage.key_prefix must be non-empty and must not contain '{', '}', ':' or spaces")
	check(c.Storage.Shards > 0 && c.Storage.Shards <= 16384, "storage.shards must be in [1, 16384]")
	check(c.Storage.ChunkRange.D() >= time.Second && c.Storage.ChunkRange.D() <= 24*time.Hour,
		"storage.chunk_range must be between 1s and 24h")
	check(c.Storage.ChunkRange.D()%time.Millisecond == 0, "storage.chunk_range must be a whole number of milliseconds")
	check(c.Storage.GracePeriod.D() >= 0, "storage.grace_period must not be negative")
	check(c.Storage.Retention.D() >= 0 && c.Storage.Retention.D() <= 20*365*24*time.Hour,
		"storage.retention must be between 0 (forever) and 20y")
	check(c.Ingester.MaxRequestBodySize > 0, "ingester.max_request_body_size must be positive")
	check(c.Ingester.IndexCacheSize > 0, "ingester.index_cache_size must be positive")
	check(c.Ingester.IndexWriteConcurrency > 0, "ingester.index_write_concurrency must be positive")
	check(c.Flusher.PollInterval.D() > 0, "flusher.poll_interval must be positive")
	check(c.Flusher.BatchSize > 0, "flusher.batch_size must be positive")
	check(c.Flusher.Concurrency > 0, "flusher.concurrency must be positive")
	check(c.Flusher.LeaseDuration.D() >= time.Second, "flusher.lease_duration must be at least 1s")
	check(c.Querier.LookbackDelta.D() > 0, "querier.lookback_delta must be positive")
	check(c.Querier.Timeout.D() > 0, "querier.timeout must be positive")
	check(c.Querier.MaxSamples > 0, "querier.max_samples must be positive")
	check(c.Querier.SeriesCacheSize > 0, "querier.series_cache_size must be positive")
	check(c.Querier.ReadConcurrency > 0, "querier.read_concurrency must be positive")
	check(c.Querier.HotLookback.D() >= c.Storage.ChunkRange.D()+c.Storage.GracePeriod.D(),
		"querier.hot_lookback must be at least storage.chunk_range + storage.grace_period")
	return errors.Join(errs...)
}
