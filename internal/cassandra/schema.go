package cassandra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

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

	// Inverted index: label pair -> series active on that day. The series of
	// a label pair are spread over storage.index_buckets partitions by
	// series ID, so that a pair shared by millions of series does not make
	// one huge partition.
	`CREATE TABLE IF NOT EXISTS postings (
		day       int,
		name      text,
		value     text,
		bucket    smallint,
		series_id bigint,
		PRIMARY KEY ((day, name, value, bucket), series_id)
	) WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}
	  AND gc_grace_seconds = 86400`,

	// Values of a label name seen on a day, spread over
	// storage.index_buckets partitions by a hash of the value.
	`CREATE TABLE IF NOT EXISTS label_values (
		day    int,
		name   text,
		bucket smallint,
		value  text,
		PRIMARY KEY ((day, name, bucket), value)
	) WITH gc_grace_seconds = 86400`,

	`CREATE TABLE IF NOT EXISTS label_names (
		day  int,
		name text,
		PRIMARY KEY (day, name)
	) WITH gc_grace_seconds = 86400`,

	// Layout parameters the data was written with (see CheckLayout).
	`CREATE TABLE IF NOT EXISTS meta (
		name  text PRIMARY KEY,
		value text
	)`,
}

// bucketedTables are the tables whose partition key gained a bucket column.
// Tables created by earlier builds cannot be changed in place.
var bucketedTables = []string{"postings", "label_values"}

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
	if err := checkBucketedTables(ctx, s, cfg.Keyspace); err != nil {
		return err
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

// checkBucketedTables refuses to migrate a keyspace whose index tables were
// created without buckets: CREATE TABLE IF NOT EXISTS would keep them, and
// every index write would then fail.
func checkBucketedTables(ctx context.Context, s *gocql.Session, keyspace string) error {
	for _, table := range bucketedTables {
		iter := s.Query(`SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`,
			keyspace, table).IterContext(ctx)
		var (
			col     string
			columns int
			bucket  bool
		)
		for iter.Scan(&col) {
			columns++
			bucket = bucket || col == "bucket"
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("read schema of %s.%s: %w", keyspace, table, err)
		}
		if columns > 0 && !bucket {
			return fmt.Errorf("table %s.%s was created by an earlier build without index buckets and cannot be changed in place; "+
				"drop the postings and label_values tables and run the migration again (series indexed before become "+
				"discoverable again once they are written to), or use a new keyspace", keyspace, table)
		}
	}
	return nil
}

// CheckLayout records the index layout on first use and refuses to run
// with a different one: changing the number of buckets would make existing
// index entries unreachable.
func CheckLayout(ctx context.Context, s *gocql.Session, indexBuckets int) error {
	want := strconv.Itoa(indexBuckets)
	existing := make(map[string]any)
	applied, err := s.Query(`INSERT INTO meta (name, value) VALUES ('index_buckets', ?) IF NOT EXISTS`, want).
		MapScanCASContext(ctx, existing)
	if err != nil {
		return fmt.Errorf("cassandra meta: %w (run xtsdb migrate)", err)
	}
	if got, _ := existing["value"].(string); !applied && got != want {
		return fmt.Errorf("storage.index_buckets is %s but the index in Cassandra was written with %s; "+
			"changing it is not supported", want, got)
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
