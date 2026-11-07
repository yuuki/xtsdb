package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/prometheus/tsdb/fileutil"

	"github.com/yuuki/xtsdb/internal/app"
	"github.com/yuuki/xtsdb/internal/backfill"
	"github.com/yuuki/xtsdb/internal/config"
)

type importOptions struct {
	format      string
	stateFile   string
	concurrency int
	maxBuffered int
}

// runImport imports historical data directly into the persistent tier.
func runImport(ctx context.Context, cfg *config.Config, logger *slog.Logger, opts importOptions, paths []string, stdout io.Writer) error {
	if opts.format != "tsdb" && opts.format != "openmetrics" {
		return fmt.Errorf("unknown format %q (tsdb or openmetrics)", opts.format)
	}
	if opts.concurrency < 1 {
		return fmt.Errorf("-concurrency must be positive")
	}
	p, err := app.OpenPersistent(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer p.Close()
	im, err := backfill.New(p.Cold, p.Index, backfill.Options{
		Layout: app.Layout(cfg), Retention: cfg.Storage.Retention.D(), MaxFuture: cfg.Ingester.MaxSampleFuture.D(), Logger: logger,
	})
	if err != nil {
		return err
	}
	state, err := backfill.OpenState(opts.stateFile)
	if err != nil {
		return err
	}
	defer state.Close()

	start := time.Now()
	sources, err := importSources(opts.format, paths, logger)
	if err != nil {
		return err
	}
	if err := importAll(ctx, im, state, sources, opts, logger); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s in %v\n", im.Stats.String(), time.Since(start).Round(time.Second))
	return nil
}

// importSource is a block or a file to import. Its id records it in the
// state file; it is empty for standard input, which cannot be resumed.
type importSource struct{ id, path string }

// importSources lists the sources to import from paths: the blocks of each
// data or block directory with the tsdb format, and each file with the
// openmetrics format.
func importSources(format string, paths []string, logger *slog.Logger) ([]importSource, error) {
	var sources []importSource
	for _, p := range paths {
		switch {
		case format == "openmetrics" && p == "-":
			sources = append(sources, importSource{path: p}) // stdin cannot be resumed
		case format == "openmetrics":
			id, err := backfill.FileID(p)
			if err != nil {
				return nil, err
			}
			sources = append(sources, importSource{id: id, path: p})
		default:
			blocks, err := blockSources(p, logger)
			if err != nil {
				return nil, err
			}
			sources = append(sources, blocks...)
		}
	}
	return sources, nil
}

// blockSources lists the blocks of a Prometheus data directory or block
// directory.
func blockSources(dir string, logger *slog.Logger) ([]importSource, error) {
	blocks, err := backfill.Blocks(dir)
	if err != nil {
		return nil, err
	}
	if backfill.HasHead(dir) {
		logger.Warn("the head of this Prometheus data directory (wal/) is not a block and is not imported: "+
			"the most recent hours of data are missing; import a snapshot instead "+
			"(POST /api/v1/admin/tsdb/snapshot writes the head as a block)", "dir", dir)
	}
	sources := make([]importSource, 0, len(blocks))
	for _, b := range blocks {
		id, err := backfill.BlockID(b)
		if err != nil {
			return nil, err
		}
		sources = append(sources, importSource{id: "block " + id, path: b})
	}
	return sources, nil
}

// importAll imports the sources in order, skipping those that state records
// as imported completely, and records in state each source with an id once
// it is imported completely.
func importAll(ctx context.Context, im *backfill.Importer, state *backfill.State, sources []importSource, opts importOptions, logger *slog.Logger) error {
	for i, src := range sources {
		if src.id != "" && state.Done(src.id) {
			logger.Info("skipping source imported before", "source", src.path)
			continue
		}
		logger.Info("importing", "source", src.path, "progress", fmt.Sprintf("%d/%d", i+1, len(sources)))
		if err := importOne(ctx, im, src.path, opts); err != nil {
			return fmt.Errorf("import %s: %w (importing again is safe)", src.path, err)
		}
		if src.id != "" {
			if err := state.MarkDone(src.id); err != nil {
				return err
			}
		}
		logger.Info("imported", "source", src.path, "stats", im.Stats.String())
	}
	return nil
}

// importOne imports the block, or with the openmetrics format the file, at
// path.
func importOne(ctx context.Context, im *backfill.Importer, path string, opts importOptions) error {
	if opts.format == "tsdb" {
		return im.ImportBlock(ctx, path, opts.concurrency)
	}
	return importOpenMetrics(ctx, im, path, opts)
}

// importOpenMetrics imports a file, mapped into memory rather than read, so
// that its size is not bounded by the memory available. Standard input has
// to be read completely.
func importOpenMetrics(ctx context.Context, im *backfill.Importer, path string, opts importOptions) error {
	if path == "-" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		return im.ImportOpenMetrics(ctx, data, opts.concurrency, opts.maxBuffered)
	}
	if st, err := os.Stat(path); err != nil {
		return err
	} else if st.Size() == 0 {
		return im.ImportOpenMetrics(ctx, nil, opts.concurrency, opts.maxBuffered)
	}
	f, err := fileutil.OpenMmapFile(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return im.ImportOpenMetrics(ctx, f.Bytes(), opts.concurrency, opts.maxBuffered)
}
