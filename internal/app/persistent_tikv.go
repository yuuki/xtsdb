//go:build !notikv && (!darwin || cgo)

package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/tikv"
)

// TiKVSupported reports whether this build includes the TiKV backend.
const TiKVSupported = true

func openTiKV(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Persistent, error) {
	retention := cfg.Storage.Retention.D()
	chunkRange := cfg.Storage.ChunkRange.D()
	db, err := tikv.Open(ctx, cfg.TiKV, cfg.Storage.KeyPrefix, logger)
	if err != nil {
		return nil, err
	}
	err = db.CheckLayout(ctx, chunkRange)
	if err == nil && retention > 0 {
		err = db.CheckTTL(ctx)
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Persistent{
		Backend: config.BackendTiKV,
		Cold:    tikv.NewChunkStore(db, retention, chunkRange.Milliseconds(), cfg.Querier.ReadConcurrency, logger),
		Index:   tikv.NewIndex(db, retention, cfg.Querier.ReadConcurrency),
		close:   func() { _ = db.Close() },
	}, nil
}

func migrateTiKV(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	db, err := tikv.Open(ctx, cfg.TiKV, cfg.Storage.KeyPrefix, logger)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("tikv: %w", err)
	}
	if cfg.Storage.Retention.D() > 0 {
		if err := db.CheckTTL(ctx); err != nil {
			return err
		}
	}
	logger.Info("the tikv backend has no schema to migrate; TiKV is reachable")
	return nil
}
