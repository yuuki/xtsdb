// Package cassandra implements the persistent tier and the series index on
// Apache Cassandra (or ScyllaDB).
package cassandra

import (
	"fmt"
	"log/slog"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/yuuki/xtsdb/internal/config"
)

// NewSession connects to Cassandra. If useKeyspace is false the session is
// not bound to a keyspace, which is needed to create it.
func NewSession(cfg config.CassandraConfig, logger *slog.Logger, useKeyspace bool) (*gocql.Session, error) {
	consistency, err := gocql.ParseConsistencyWrapper(cfg.Consistency)
	if err != nil {
		return nil, fmt.Errorf("cassandra.consistency: %w", err)
	}
	cluster := gocql.NewCluster(cfg.Hosts...)
	if useKeyspace {
		cluster.Keyspace = cfg.Keyspace
	}
	cluster.Consistency = consistency
	// Lightweight transactions (series IDs) stay in the local datacenter
	// when the configured level does.
	cluster.SerialConsistency = gocql.Serial
	if strings.HasPrefix(strings.ToUpper(cfg.Consistency), "LOCAL_") {
		cluster.SerialConsistency = gocql.LocalSerial
	}
	cluster.Timeout = cfg.Timeout.D()
	cluster.ConnectTimeout = cfg.ConnectTimeout.D()
	if cfg.NumConns > 0 {
		cluster.NumConns = cfg.NumConns
	}
	if cfg.LocalDC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.DCAwareRoundRobinPolicy(cfg.LocalDC), gocql.ShuffleReplicas())
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(), gocql.ShuffleReplicas())
	}
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3}
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password}
	}
	tlsCfg, err := cfg.TLS.Build()
	if err != nil {
		return nil, fmt.Errorf("cassandra tls: %w", err)
	}
	if tlsCfg != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: tlsCfg, EnableHostVerification: !cfg.TLS.InsecureSkipVerify}
	}
	if logger != nil {
		cluster.Logger = slogAdapter{logger.With("component", "cassandra")}
	}
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect to cassandra: %w", err)
	}
	return s, nil
}

// slogAdapter routes driver logs to slog.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) log(level slog.Level, msg string, fields []gocql.LogField) {
	attrs := make([]any, 0, 2*len(fields))
	for _, f := range fields {
		attrs = append(attrs, f.Name, f.Value.Any())
	}
	a.l.Log(nil, level, msg, attrs...) //nolint:staticcheck // the driver does not pass a context
}

func (a slogAdapter) Error(msg string, f ...gocql.LogField)   { a.log(slog.LevelError, msg, f) }
func (a slogAdapter) Warning(msg string, f ...gocql.LogField) { a.log(slog.LevelWarn, msg, f) }
func (a slogAdapter) Info(msg string, f ...gocql.LogField)    { a.log(slog.LevelInfo, msg, f) }
func (a slogAdapter) Debug(msg string, f ...gocql.LogField)   { a.log(slog.LevelDebug, msg, f) }
