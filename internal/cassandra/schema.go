package cassandra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/yuuki/xtsdb/internal/config"
)

// Tables are partitioned by day (see model.Day). Rows expire through
// per-write TTLs; TimeWindowCompactionStrategy then drops whole SSTables.
var tables = []string{
	// Compressed chunks of the persistent tier. A window normally has one
	// version (fragment, total, digest) stored in one or more parts. More
	// versions appear only when a window is persisted again with different
	// content, for example after Redis was restored from an older snapshot.
	// The clustering order is the precedence of versions on reads: later
	// fragments, then more records (total), then the digest as a signed
	// 64-bit value.
	`CREATE TABLE IF NOT EXISTS chunks (
		series_id bigint,
		day       int,
		start_ms  bigint,
		fragment  bigint,
		total     int,
		digest    bigint,
		part      int,
		max_t     bigint,
		samples   int,
		encoding  tinyint,
		data      blob,
		PRIMARY KEY ((series_id, day), start_ms, fragment, total, digest, part)
	) WITH CLUSTERING ORDER BY (start_ms ASC, fragment ASC, total ASC, digest ASC, part ASC)
	  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
	  AND gc_grace_seconds = 86400`,

	// Labels of every series.
	`CREATE TABLE IF NOT EXISTS series (
		series_id bigint PRIMARY KEY,
		labels    frozen<map<text, text>>
	)`,

	// Inverted index: label pair -> series active on that day.
	`CREATE TABLE IF NOT EXISTS postings (
		day       int,
		name      text,
		value     text,
		series_id bigint,
		PRIMARY KEY ((day, name, value), series_id)
	) WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
	  AND gc_grace_seconds = 86400`,

	`CREATE TABLE IF NOT EXISTS label_values (
		day   int,
		name  text,
		value text,
		PRIMARY KEY ((day, name), value)
	) WITH gc_grace_seconds = 86400`,

	`CREATE TABLE IF NOT EXISTS label_names (
		day  int,
		name text,
		PRIMARY KEY (day, name)
	) WITH gc_grace_seconds = 86400`,
}

// Migrate creates the keyspace and tables if they do not exist.
func Migrate(ctx context.Context, cfg config.CassandraConfig, logger *slog.Logger) error {
	if !validIdentifier(cfg.Keyspace) {
		return fmt.Errorf("invalid keyspace name %q", cfg.Keyspace)
	}
	repl, err := replication(cfg)
	if err != nil {
		return err
	}
	s, err := NewSession(cfg, logger, false)
	if err != nil {
		return err
	}
	defer s.Close()
	stmt := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s", cfg.Keyspace, repl)
	if err := s.Query(stmt).ExecContext(ctx); err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}
	for _, t := range tables {
		stmt := strings.Replace(t, "IF NOT EXISTS ", "IF NOT EXISTS "+cfg.Keyspace+".", 1)
		if err := s.Query(stmt).ExecContext(ctx); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}
	if err := s.AwaitSchemaAgreement(ctx); err != nil {
		return fmt.Errorf("await schema agreement: %w", err)
	}
	if logger != nil {
		logger.Info("cassandra schema is up to date", "keyspace", cfg.Keyspace)
	}
	return nil
}

func replication(cfg config.CassandraConfig) (string, error) {
	switch cfg.ReplicationClass {
	case "SimpleStrategy":
		if cfg.ReplicationFactor < 1 {
			return "", errors.New("cassandra.replication_factor must be at least 1")
		}
		return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}", cfg.ReplicationFactor), nil
	case "NetworkTopologyStrategy":
		if len(cfg.Datacenters) == 0 {
			return "", errors.New("cassandra.datacenters is required with NetworkTopologyStrategy")
		}
		parts := []string{"'class': 'NetworkTopologyStrategy'"}
		for _, dc := range slices.Sorted(maps.Keys(cfg.Datacenters)) {
			if strings.ContainsAny(dc, "'\\") || cfg.Datacenters[dc] < 1 {
				return "", fmt.Errorf("invalid datacenter %q", dc)
			}
			parts = append(parts, fmt.Sprintf("'%s': %d", dc, cfg.Datacenters[dc]))
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	default:
		return "", fmt.Errorf("unknown replication class %q", cfg.ReplicationClass)
	}
}

func validIdentifier(s string) bool {
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
