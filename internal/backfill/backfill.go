// Package backfill imports historical samples into the persistent tier and
// the index, bypassing the hot tier. The hot tier accepts samples only while
// their chunk window is open (chunk_range + grace_period), so this is the
// only way to load history: data migrated from Prometheus, samples lost in
// an outage longer than the grace period, or data from xtsdb 0.x.
//
// Every chunk window of an imported series is written as a version of that
// window (see storage.ColdChunk), next to any version flushed from the hot
// tier; reads merge the versions.
package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/flush"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Fragment is the fragment of imported chunk versions. Chunks flushed from
// the hot tier have fragments that are timestamps in microseconds, so where
// an imported sample and an ingested one have the same timestamp, the
// ingested one wins: an import fills gaps but never overrides what xtsdb
// received itself. Being constant, it also makes imports idempotent:
// importing the same samples again writes the same rows.
const Fragment = 0

// Options configures an Importer.
type Options struct {
	Layout model.Layout
	// Retention skips samples older than this; zero keeps all of them.
	Retention time.Duration
	// MaxFuture skips samples further than this in the future.
	MaxFuture time.Duration
	Logger    *slog.Logger
}

// Stats counts what an import did.
type Stats struct {
	Series     atomic.Int64 // series with at least one sample imported
	Samples    atomic.Int64 // samples imported
	Chunks     atomic.Int64 // chunk window versions written
	Bytes      atomic.Int64 // compressed bytes written
	TooOld     atomic.Int64 // samples older than the retention, skipped
	TooNew     atomic.Int64 // samples too far in the future, skipped
	Invalid    atomic.Int64 // samples of series with invalid labels, skipped
	Collisions atomic.Int64 // samples of series whose ID collides, skipped
	Histograms atomic.Int64 // native histogram samples imported (part of Samples)
}

// String summarizes the stats.
func (s *Stats) String() string {
	return fmt.Sprintf("imported %d samples (%d native histograms) of %d series in %d chunks (%d bytes); "+
		"skipped %d too old, %d too new, %d invalid, %d of colliding series",
		s.Samples.Load(), s.Histograms.Load(), s.Series.Load(), s.Chunks.Load(), s.Bytes.Load(),
		s.TooOld.Load(), s.TooNew.Load(), s.Invalid.Load(), s.Collisions.Load())
}

// indexKey identifies an index entry that was already written in this run.
type indexKey struct {
	day         int32
	id          model.SeriesID
	check       uint64
	name, value string
}

// indexCacheSize bounds the index entries remembered across series and
// sources, which saves rewriting them for every block.
const indexCacheSize = 1 << 20

// Importer writes series to the cold tier and the index.
type Importer struct {
	cold    storage.ColdStore
	index   storage.IndexWriter
	opts    Options
	now     func() time.Time
	indexed *lru.Cache[indexKey, struct{}]

	Stats Stats
}

// New creates an Importer.
func New(cold storage.ColdStore, index storage.IndexWriter, opts Options) (*Importer, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	cache, err := lru.New[indexKey, struct{}](indexCacheSize)
	if err != nil {
		return nil, err
	}
	return &Importer{cold: cold, index: index, opts: opts, now: time.Now, indexed: cache}, nil
}

// Import imports the samples of one series and takes ownership of samples.
// They need not be sorted; of several samples with the same timestamp, the
// last one wins. Samples that cannot be imported are counted in Stats; an
// error means that a backend failed, and importing the series again is
// safe. Import is safe for concurrent use.
func (im *Importer) Import(ctx context.Context, lset labels.Labels, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	lset, err := model.Canonicalize(lset)
	if err != nil {
		im.Stats.Invalid.Add(int64(len(samples)))
		im.opts.Logger.Debug("skipping invalid series", "series", lset, "err", err)
		return nil
	}
	now := im.now()
	minT, maxT := int64(math.MinInt64), now.Add(im.opts.MaxFuture).UnixMilli()
	if im.opts.Retention > 0 {
		minT = now.Add(-im.opts.Retention).UnixMilli()
	}
	kept := samples[:0]
	for _, s := range samples {
		switch {
		case s.H != nil && s.H.Validate() != nil:
			im.Stats.Invalid.Add(1)
		case s.T < minT:
			im.Stats.TooOld.Add(1)
		case s.T > maxT:
			im.Stats.TooNew.Add(1)
		default:
			kept = append(kept, s)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	samples = chunk.Normalize(kept)
	id := model.ID(lset)

	// Index first, as the ingester does: no sample of a series that cannot
	// be found, and no sample of a series whose ID belongs to another one.
	if err := im.writeIndex(ctx, id, lset, samples); err != nil {
		if coll, ok := asCollision(err); ok {
			im.Stats.Collisions.Add(int64(len(samples)))
			im.opts.Logger.Warn("series ID collision: series not imported",
				"id", id, "series", lset.String(), "existing", coll.Existing.String())
			return nil
		}
		return err
	}

	// Samples are sorted, so each window is a contiguous run.
	for i := 0; i < len(samples); {
		start := im.opts.Layout.WindowStart(id, samples[i].T)
		j := i + 1
		for j < len(samples) && samples[j].T < start+im.opts.Layout.ChunkRange {
			j++
		}
		n, err := flush.WriteCold(ctx, im.cold, id, start, Fragment, j-i, samples[i:j])
		im.Stats.Bytes.Add(int64(n))
		if err != nil {
			return fmt.Errorf("write chunk of %s: %w", lset, err)
		}
		im.Stats.Chunks.Add(1)
		i = j
	}
	im.Stats.Series.Add(1)
	im.Stats.Samples.Add(int64(len(samples)))
	for _, s := range samples {
		if s.IsHistogram() {
			im.Stats.Histograms.Add(1)
		}
	}
	return nil
}

// writeIndex records the series and its labels for every day it has
// samples on.
func (im *Importer) writeIndex(ctx context.Context, id model.SeriesID, lset labels.Labels, samples []model.Sample) error {
	check := model.Check(lset)
	last := int32(math.MinInt32)
	for _, s := range samples {
		day := model.Day(s.T)
		if day == last {
			continue
		}
		last = day
		key := indexKey{day: day, id: id, check: check}
		if !im.indexed.Contains(key) {
			if err := im.index.WriteSeries(ctx, day, id, lset); err != nil {
				return err
			}
			im.indexed.Add(key, struct{}{})
		}
		var err error
		lset.Range(func(l labels.Label) {
			if err != nil {
				return
			}
			if key := (indexKey{day: day, name: l.Name, value: l.Value}); !im.indexed.Contains(key) {
				if err = im.index.WriteLabelValue(ctx, day, l.Name, l.Value); err == nil {
					im.indexed.Add(key, struct{}{})
				}
			}
			if key := (indexKey{day: day, name: l.Name}); err == nil && !im.indexed.Contains(key) {
				if err = im.index.WriteLabelName(ctx, day, l.Name); err == nil {
					im.indexed.Add(key, struct{}{})
				}
			}
		})
		if err != nil {
			return fmt.Errorf("index %s: %w", lset, err)
		}
	}
	return nil
}
