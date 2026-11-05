package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/yuuki/xtsdb/internal/cassandra"
	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/storage"
	"github.com/yuuki/xtsdb/internal/tikv"
)

// Persistent is the persistent tier and the series index, on the backend
// selected by storage.backend.
type Persistent struct {
	// Backend is the name of the backend, as in storage.backend.
	Backend string
	Cold    storage.ColdStore
	Index   storage.Index
	close   func()
}

// Close releases the connections to the backend.
func (p *Persistent) Close() {
	if p != nil && p.close != nil {
		p.close()
	}
}

// OpenPersistent connects to the backend of the persistent tier and the
// index and checks that its data was written with the same layout. With
// Cassandra, it first creates the schema if cassandra.auto_migrate is set.
func OpenPersistent(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Persistent, error) {
	retention := cfg.Storage.Retention.D()
	chunkRange := cfg.Storage.ChunkRange.D()
	readConcurrency := cfg.Querier.ReadConcurrency
	switch cfg.Storage.Backend {
	case config.BackendCassandra:
		if cfg.Cassandra.AutoMigrate {
			if err := cassandra.Migrate(ctx, cfg.Cassandra, logger); err != nil {
				return nil, fmt.Errorf("migrate: %w", err)
			}
		}
		session, err := cassandra.NewSession(cfg.Cassandra, logger, true)
		if err != nil {
			return nil, err
		}
		if err := cassandra.CheckLayout(ctx, session, cfg.Storage.IndexBuckets, chunkRange); err != nil {
			session.Close()
			return nil, err
		}
		return &Persistent{
			Backend: config.BackendCassandra,
			Cold:    cassandra.NewChunkStore(session, retention, chunkRange.Milliseconds(), readConcurrency, logger),
			Index:   cassandra.NewIndex(session, retention, readConcurrency, cfg.Storage.IndexBuckets),
			close:   session.Close,
		}, nil
	case config.BackendTiKV:
		db, err := tikv.Open(ctx, cfg.TiKV, cfg.Storage.KeyPrefix, logger)
		if err != nil {
			return nil, err
		}
		if err := db.CheckLayout(ctx, chunkRange); err != nil {
			_ = db.Close()
			return nil, err
		}
		return &Persistent{
			Backend: config.BackendTiKV,
			Cold:    tikv.NewChunkStore(db, retention, chunkRange.Milliseconds(), readConcurrency, logger),
			Index:   tikv.NewIndex(db, retention, readConcurrency),
			close:   func() { _ = db.Close() },
		}, nil
	}
	return nil, fmt.Errorf("unknown storage.backend %q", cfg.Storage.Backend)
}

// Migrate prepares the backend for use: it creates or updates the Cassandra
// schema. TiKV has no schema; Migrate checks that it can be reached.
func Migrate(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	switch cfg.Storage.Backend {
	case config.BackendCassandra:
		return cassandra.Migrate(ctx, cfg.Cassandra, logger)
	case config.BackendTiKV:
		db, err := tikv.Open(ctx, cfg.TiKV, cfg.Storage.KeyPrefix, logger)
		if err != nil {
			return err
		}
		defer db.Close()
		if err := db.Ping(ctx); err != nil {
			return fmt.Errorf("tikv: %w", err)
		}
		logger.Info("the tikv backend has no schema to migrate; TiKV is reachable")
		return nil
	}
	return fmt.Errorf("unknown storage.backend %q", cfg.Storage.Backend)
}
