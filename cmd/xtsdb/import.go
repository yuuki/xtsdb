package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/yuuki/xtsdb/internal/app"
	"github.com/yuuki/xtsdb/internal/backfill"
	"github.com/yuuki/xtsdb/internal/cassandra"
	"github.com/yuuki/xtsdb/internal/config"
)

type importOptions struct {
	format      string
	stateFile   string
	concurrency int
	maxBuffered int
}

// runImport imports historical data directly into Cassandra.
func runImport(ctx context.Context, cfg *config.Config, logger *slog.Logger, opts importOptions, paths []string, stdout io.Writer) error {
	if opts.format != "tsdb" && opts.format != "openmetrics" {
		return fmt.Errorf("unknown format %q (tsdb or openmetrics)", opts.format)
	}
	if opts.concurrency < 1 {
		return fmt.Errorf("-concurrency must be positive")
	}
	if cfg.Cassandra.AutoMigrate {
		if err := cassandra.Migrate(ctx, cfg.Cassandra, logger); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	session, err := cassandra.NewSession(cfg.Cassandra, logger, true)
	if err != nil {
		return err
	}
	defer session.Close()
	if err := cassandra.CheckLayout(ctx, session, cfg.Storage.IndexBuckets, cfg.Storage.ChunkRange.D()); err != nil {
		return err
	}
	retention := cfg.Storage.Retention.D()
	layout := app.Layout(cfg)
	im, err := backfill.New(
		cassandra.NewChunkStore(session, retention, layout.ChunkRange, cfg.Querier.ReadConcurrency, logger),
		cassandra.NewIndex(session, retention, cfg.Querier.ReadConcurrency, cfg.Storage.IndexBuckets),
		backfill.Options{Layout: layout, Retention: retention, MaxFuture: cfg.Ingester.MaxSampleFuture.D(), Logger: logger},
	)
	if err != nil {
		return err
	}
	state, err := backfill.OpenState(opts.stateFile)
	if err != nil {
		return err
	}
	defer state.Close()

	start := time.Now()
	type source struct{ id, path string }
	var sources []source
	for _, p := range paths {
		switch {
		case opts.format == "openmetrics" && p == "-":
			sources = append(sources, source{path: p}) // stdin cannot be resumed
		case opts.format == "openmetrics":
			id, err := backfill.FileID(p)
			if err != nil {
				return err
			}
			sources = append(sources, source{id: id, path: p})
		default:
			blocks, err := backfill.Blocks(p)
			if err != nil {
				return err
			}
			for _, b := range blocks {
				id, err := backfill.BlockID(b)
				if err != nil {
					return err
				}
				sources = append(sources, source{id: "block " + id, path: b})
			}
		}
	}
	for i, src := range sources {
		if src.id != "" && state.Done(src.id) {
			logger.Info("skipping source imported before", "source", src.path)
			continue
		}
		logger.Info("importing", "source", src.path, "progress", fmt.Sprintf("%d/%d", i+1, len(sources)))
		if opts.format == "tsdb" {
			err = im.ImportBlock(ctx, src.path, opts.concurrency)
		} else {
			err = importOpenMetrics(ctx, im, src.path, opts)
		}
		if err != nil {
			return fmt.Errorf("import %s: %w (importing again is safe)", src.path, err)
		}
		if src.id != "" {
			if err := state.MarkDone(src.id); err != nil {
				return err
			}
		}
		logger.Info("imported", "source", src.path, "stats", im.Stats.String())
	}
	fmt.Fprintf(stdout, "%s in %v\n", im.Stats.String(), time.Since(start).Round(time.Second))
	return nil
}

func importOpenMetrics(ctx context.Context, im *backfill.Importer, path string, opts importOptions) error {
	var (
		data []byte
		err  error
	)
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return err
	}
	return im.ImportOpenMetrics(ctx, data, opts.concurrency, opts.maxBuffered)
}
