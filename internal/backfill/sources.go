package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

func asCollision(err error) (*storage.SeriesCollisionError, bool) {
	var coll *storage.SeriesCollisionError
	return coll, errors.As(err, &coll)
}

// Blocks returns the Prometheus TSDB blocks in dir: dir itself if it is a
// block, otherwise its subdirectories that are blocks, in name (ULID, hence
// time) order. The head of a running Prometheus (its WAL) is not a block;
// take a snapshot or stop Prometheus first.
func Blocks(dir string) ([]string, error) {
	if isBlock(dir) {
		return []string{dir}, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if p := filepath.Join(dir, e.Name()); e.IsDir() && isBlock(p) {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s contains no Prometheus TSDB blocks", dir)
	}
	slices.Sort(out)
	return out, nil
}

func isBlock(dir string) bool {
	st, err := os.Stat(filepath.Join(dir, "meta.json"))
	return err == nil && !st.IsDir()
}

// BlockID returns the ULID of a block, which identifies its contents.
func BlockID(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return "", err
	}
	var meta struct {
		ULID string `json:"ulid"`
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return "", fmt.Errorf("invalid meta.json in %s: %w", dir, err)
	}
	if meta.ULID == "" {
		return "", fmt.Errorf("meta.json in %s has no ulid", dir)
	}
	return meta.ULID, nil
}

// ImportBlock imports the float samples of every series of a Prometheus
// TSDB block, up to concurrency series at a time, native histograms
// included. Deleted samples (tombstones) are left out.
//
// The block is read with the index, chunks and tombstones packages of the
// Prometheus TSDB rather than the tsdb package, which would link in the
// Prometheus configuration and the cloud SDKs it depends on.
func (im *Importer) ImportBlock(ctx context.Context, dir string, concurrency int) error {
	ir, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return fmt.Errorf("open index of block %s: %w", dir, err)
	}
	defer ir.Close()
	cr, err := chunks.NewDirReader(filepath.Join(dir, "chunks"), chunkenc.NewPool())
	if err != nil {
		return fmt.Errorf("open chunks of block %s: %w", dir, err)
	}
	defer cr.Close()
	tr, _, err := tombstones.ReadTombstones(dir)
	if err != nil {
		return fmt.Errorf("read tombstones of block %s: %w", dir, err)
	}
	defer tr.Close()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	allName, allValue := index.AllPostingsKey()
	postings, err := ir.Postings(gctx, allName, allValue)
	if err != nil {
		return fmt.Errorf("read postings of block %s: %w", dir, err)
	}
	var (
		builder labels.ScratchBuilder
		metas   []chunks.Meta
		it      chunkenc.Iterator
	)
	read := func(ref promstorage.SeriesRef) (labels.Labels, []model.Sample, error) {
		if err := ir.Series(ref, &builder, &metas); err != nil {
			return labels.EmptyLabels(), nil, err
		}
		deleted, err := tr.Get(ref)
		if err != nil {
			return labels.EmptyLabels(), nil, err
		}
		var samples []model.Sample
		for _, meta := range metas {
			chk, iterable, err := cr.ChunkOrIterable(meta)
			if err != nil {
				return labels.EmptyLabels(), nil, err
			}
			if chk != nil {
				it = chk.Iterator(it)
			} else {
				it = iterable.Iterator(it)
			}
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				var s model.Sample
				switch vt {
				case chunkenc.ValFloat:
					s.T, s.V = it.At()
				case chunkenc.ValHistogram:
					var h *histogram.Histogram
					s.T, h = it.AtHistogram(nil)
					s.H = h.ToFloat(nil)
				case chunkenc.ValFloatHistogram:
					s.T, s.H = it.AtFloatHistogram(nil)
				default:
					return labels.EmptyLabels(), nil, fmt.Errorf("unexpected value type %v", vt)
				}
				if !slices.ContainsFunc(deleted, func(d tombstones.Interval) bool { return d.InBounds(s.T) }) {
					samples = append(samples, s)
				}
			}
			if err := it.Err(); err != nil {
				return labels.EmptyLabels(), nil, err
			}
		}
		return builder.Labels(), samples, nil
	}
	for postings.Next() && gctx.Err() == nil {
		lset, samples, err := read(postings.At())
		if err != nil {
			_ = g.Wait()
			return fmt.Errorf("read series from block %s: %w", dir, err)
		}
		g.Go(func() error { return im.Import(gctx, lset, samples) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := postings.Err(); err != nil {
		return fmt.Errorf("read postings of block %s: %w", dir, err)
	}
	return ctx.Err()
}

// DefaultMaxBuffered bounds the samples ImportOpenMetrics holds in memory
// (about 16 bytes each) before importing what it has read so far.
const DefaultMaxBuffered = 10_000_000

// ImportOpenMetrics imports OpenMetrics text in which every sample has a
// timestamp, the input of `promtool tsdb create-blocks-from openmetrics`.
// Samples are collected per series and imported when maxBuffered samples
// have been read, and at the end; a window split this way is stored as
// several versions, which reads merge. _created series are skipped.
func (im *Importer) ImportOpenMetrics(ctx context.Context, data []byte, concurrency, maxBuffered int) error {
	if maxBuffered <= 0 {
		maxBuffered = DefaultMaxBuffered
	}
	type series struct {
		lset    labels.Labels
		samples []model.Sample
	}
	var (
		buffered = make(map[string]*series)
		order    []string // first appearance, for a deterministic import
		n        int
		key      []byte
	)
	importAll := func() error {
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)
		for _, k := range order {
			s := buffered[k]
			g.Go(func() error { return im.Import(gctx, s.lset, s.samples) })
		}
		err := g.Wait()
		buffered, order, n = make(map[string]*series), nil, 0
		return err
	}
	p := textparse.NewOpenMetricsParser(data, labels.NewSymbolTable(), textparse.WithOMParserSTSeriesSkipped())
	for ctx.Err() == nil {
		entry, err := p.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("parse OpenMetrics: %w", err)
		}
		if entry != textparse.EntrySeries {
			continue // OpenMetrics 1.0 text has no native histograms
		}
		raw, ts, v := p.Series()
		if ts == nil {
			return fmt.Errorf("sample %s has no timestamp; every sample to import needs one", raw)
		}
		var lset labels.Labels
		p.Labels(&lset)
		key = lset.Bytes(key[:0])
		s := buffered[string(key)]
		if s == nil {
			s = &series{lset: lset}
			buffered[string(key)] = s
			order = append(order, string(key))
		}
		s.samples = append(s.samples, model.Sample{T: *ts, V: v})
		if n++; n >= maxBuffered {
			if err := importAll(); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return importAll()
}
