// Package ingest implements the write path: protocol handlers, label
// validation, index maintenance and appends to the hot tier.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Series is an incoming series with its samples.
type Series struct {
	Labels  labels.Labels
	Samples []model.Sample
}

// Result summarizes a write.
type Result struct {
	Accepted int // samples stored, floats and histograms
	// AcceptedHistograms is how many of the accepted samples are native
	// histograms.
	AcceptedHistograms int
	TooOld             int // window already closed
	TooNew             int // beyond max_sample_future
	Invalid            int // samples of series with invalid labels, and invalid histograms
	// Collision counts samples of series whose ID is already used by a
	// different label set.
	Collision int
	// ChunkFull counts samples whose chunk reached its maximum size.
	ChunkFull int
	// FirstInvalid describes the first invalid series, if any.
	FirstInvalid error
}

// Rejected returns the number of samples that were not stored.
func (r Result) Rejected() int { return r.TooOld + r.TooNew + r.Invalid + r.Collision + r.ChunkFull }

// Merge adds the counts of o to r.
func (r *Result) Merge(o Result) {
	r.Accepted += o.Accepted
	r.AcceptedHistograms += o.AcceptedHistograms
	r.TooOld += o.TooOld
	r.TooNew += o.TooNew
	r.Invalid += o.Invalid
	r.Collision += o.Collision
	r.ChunkFull += o.ChunkFull
	if r.FirstInvalid == nil {
		r.FirstInvalid = o.FirstInvalid
	}
}

// indexKey identifies an index entry that has already been written.
type indexKey struct {
	kind uint8
	day  int32
	id   model.SeriesID
	// check is model.Check of the labels for kindSeries entries, so that a
	// series whose ID collides with a cached one is not taken for it.
	check uint64
	name  string
	value string
}

// collisionCacheSize bounds the series known to collide. They are rejected
// without asking the index again, and logged once per day.
const collisionCacheSize = 4096

const (
	kindSeries uint8 = iota
	kindValue
	kindName
)

// WriterOptions configures a Writer.
type WriterOptions struct {
	MaxSampleFuture time.Duration
	// MaxSampleAge drops samples older than this before they are indexed.
	// Set it to chunk_range + grace_period (plus a little slack for clock
	// skew): older samples belong to closed windows and would be rejected
	// by the hot tier anyway.
	MaxSampleAge          time.Duration
	IndexCacheSize        int
	IndexWriteConcurrency int
	Registerer            prometheus.Registerer
	Logger                *slog.Logger
}

// Writer validates samples, maintains the index and appends samples to the
// hot tier. It is safe for concurrent use.
type Writer struct {
	hot        storage.HotStore
	index      storage.IndexWriter
	cache      *lru.Cache[indexKey, struct{}]
	collisions *lru.Cache[indexKey, struct{}]
	maxFuture  time.Duration
	maxAge     time.Duration
	indexConc  int
	logger     *slog.Logger
	now        func() time.Time
	idOf       func(labels.Labels) model.SeriesID // model.ID; replaced in tests

	samples          *prometheus.CounterVec
	indexWrites      *prometheus.CounterVec
	seriesCollisions prometheus.Counter
	writeLatency     prometheus.Histogram
}

// NewWriter creates a Writer.
func NewWriter(hot storage.HotStore, index storage.IndexWriter, opts WriterOptions) (*Writer, error) {
	if opts.MaxSampleAge <= 0 || opts.MaxSampleFuture <= 0 {
		return nil, errors.New("MaxSampleAge and MaxSampleFuture must be positive")
	}
	cache, err := lru.New[indexKey, struct{}](opts.IndexCacheSize)
	if err != nil {
		return nil, err
	}
	collisions, err := lru.New[indexKey, struct{}](collisionCacheSize)
	if err != nil {
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	reg := promauto.With(opts.Registerer)
	return &Writer{
		hot:        hot,
		index:      index,
		cache:      cache,
		collisions: collisions,
		maxFuture:  opts.MaxSampleFuture,
		maxAge:     opts.MaxSampleAge,
		indexConc:  opts.IndexWriteConcurrency,
		logger:     opts.Logger,
		now:        time.Now,
		idOf:       model.ID,
		samples: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_ingester_samples_total",
			Help: "Samples received by the ingester, by outcome.",
		}, []string{"outcome"}),
		indexWrites: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_ingester_index_writes_total",
			Help: "Index entries written, by kind.",
		}, []string{"kind"}),
		seriesCollisions: reg.NewCounter(prometheus.CounterOpts{
			Name: "xtsdb_ingester_series_collisions_total",
			Help: "Series refused because their ID is used by a different label set (counted once per series, day and ingester).",
		}),
		writeLatency: reg.NewHistogram(prometheus.HistogramOpts{
			Name:                        "xtsdb_ingester_write_duration_seconds",
			Help:                        "Time to index and store a batch of samples.",
			Buckets:                     prometheus.DefBuckets,
			NativeHistogramBucketFactor: 1.1,
		}),
	}, nil
}

// ErrStorage wraps failures of the storage backends. Clients should retry.
var ErrStorage = errors.New("storage unavailable")

// Write stores a batch of series. Samples that cannot be stored are counted
// in the result; an error is returned only if the storage failed, in which
// case nothing or only part of the batch may have been written and the
// client should retry the whole batch.
func (w *Writer) Write(ctx context.Context, batch []Series) (Result, error) {
	start := w.now()
	defer func() { w.writeLatency.Observe(time.Since(start).Seconds()) }()

	var res Result
	maxT := start.Add(w.maxFuture).UnixMilli()
	minT := start.Add(-w.maxAge).UnixMilli()
	type entry struct {
		id    model.SeriesID
		check uint64
		lset  labels.Labels
		days  []int32
	}
	entries := make([]entry, 0, len(batch))
	toStore := make([]storage.SeriesSamples, 0, len(batch))

	for _, s := range batch {
		if len(s.Samples) == 0 {
			continue
		}
		lset, err := model.Canonicalize(s.Labels)
		if err != nil {
			res.Invalid += len(s.Samples)
			if res.FirstInvalid == nil {
				res.FirstInvalid = fmt.Errorf("series %s: %w", s.Labels, err)
			}
			continue
		}
		samples := s.Samples[:0:0]
		minS, maxS := int64(0), int64(0)
		for _, smp := range s.Samples {
			if smp.H != nil {
				if err := model.ValidateHistogram(smp.H); err != nil {
					res.Invalid++
					if res.FirstInvalid == nil {
						res.FirstInvalid = fmt.Errorf("histogram of %s at %d: %w", lset, smp.T, err)
					}
					continue
				}
			}
			if smp.T > maxT {
				res.TooNew++
				continue
			}
			if smp.T < minT {
				res.TooOld++
				continue
			}
			if len(samples) == 0 || smp.T < minS {
				minS = smp.T
			}
			if len(samples) == 0 || smp.T > maxS {
				maxS = smp.T
			}
			samples = append(samples, smp)
		}
		if len(samples) == 0 {
			continue
		}
		id := w.idOf(lset)
		entries = append(entries, entry{id: id, check: model.Check(lset), lset: lset, days: model.Days(minS, maxS)})
		toStore = append(toStore, storage.SeriesSamples{ID: id, Samples: samples})
	}

	// Index first: a sample must never be stored for a series that cannot
	// be found. Series are recorded before their label values, so that a
	// series whose ID collides with another one leaves no trace.
	var (
		mu       sync.Mutex
		collided = make(map[indexKey]struct{})
	)
	seriesKey := func(e entry, day int32) indexKey {
		return indexKey{kind: kindSeries, day: day, id: e.id, check: e.check}
	}
	err := w.writeIndex(ctx, func(once func(indexKey, func(context.Context) error)) {
		for _, e := range entries {
			for _, day := range e.days {
				key := seriesKey(e, day)
				if w.collisions.Contains(key) {
					mu.Lock()
					collided[key] = struct{}{}
					mu.Unlock()
					continue
				}
				once(key, func(ctx context.Context) error {
					err := w.index.WriteSeries(ctx, day, e.id, e.lset)
					var coll *storage.SeriesCollisionError
					if !errors.As(err, &coll) {
						return err
					}
					w.logger.Error("series ID collision: samples of this series are rejected",
						"id", e.id, "day", day, "series", e.lset.String(), "existing", coll.Existing.String())
					w.seriesCollisions.Inc()
					w.collisions.Add(key, struct{}{})
					mu.Lock()
					collided[key] = struct{}{}
					mu.Unlock()
					return errSkip
				})
			}
		}
	})
	if err == nil && len(collided) > 0 {
		kept := 0
		for i, e := range entries {
			if slices.ContainsFunc(e.days, func(day int32) bool {
				_, ok := collided[seriesKey(e, day)]
				return ok
			}) {
				res.Collision += len(toStore[i].Samples)
				continue
			}
			entries[kept], toStore[kept] = e, toStore[i]
			kept++
		}
		entries, toStore = entries[:kept], toStore[:kept]
	}
	if err == nil {
		err = w.writeIndex(ctx, func(once func(indexKey, func(context.Context) error)) {
			for _, e := range entries {
				for _, day := range e.days {
					e.lset.Range(func(l labels.Label) {
						once(indexKey{kind: kindValue, day: day, name: l.Name, value: l.Value}, func(ctx context.Context) error {
							return w.index.WriteLabelValue(ctx, day, l.Name, l.Value)
						})
						once(indexKey{kind: kindName, day: day, name: l.Name}, func(ctx context.Context) error {
							return w.index.WriteLabelName(ctx, day, l.Name)
						})
					})
				}
			}
		})
	}
	if err != nil {
		w.samples.WithLabelValues("error").Add(float64(countSamples(toStore)))
		return res, fmt.Errorf("%w: index: %w", ErrStorage, err)
	}

	if len(toStore) > 0 {
		ar, err := w.hot.Append(ctx, toStore)
		if err != nil {
			w.samples.WithLabelValues("error").Add(float64(countSamples(toStore)))
			return res, fmt.Errorf("%w: hot tier: %w", ErrStorage, err)
		}
		res.Accepted += ar.Accepted
		res.AcceptedHistograms += ar.AcceptedHistograms
		res.TooOld += ar.TooOld
		res.ChunkFull += ar.ChunkFull
	}

	w.samples.WithLabelValues("accepted").Add(float64(res.Accepted))
	w.samples.WithLabelValues("too_old").Add(float64(res.TooOld))
	w.samples.WithLabelValues("too_new").Add(float64(res.TooNew))
	w.samples.WithLabelValues("invalid").Add(float64(res.Invalid))
	w.samples.WithLabelValues("collision").Add(float64(res.Collision))
	w.samples.WithLabelValues("chunk_full").Add(float64(res.ChunkFull))
	return res, nil
}

// errSkip tells writeIndex that an entry was not written but that this is
// not an error: it is neither cached nor counted.
var errSkip = errors.New("skip")

// writeIndex runs the index writes scheduled by schedule concurrently.
// Entries that are cached, or already scheduled, are skipped; written
// entries are cached.
func (w *Writer) writeIndex(ctx context.Context, schedule func(once func(indexKey, func(context.Context) error))) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(w.indexConc)
	scheduled := make(map[indexKey]struct{})
	schedule(func(key indexKey, write func(context.Context) error) {
		if _, ok := scheduled[key]; ok || w.cache.Contains(key) {
			return
		}
		scheduled[key] = struct{}{}
		g.Go(func() error {
			err := write(gctx)
			if errors.Is(err, errSkip) {
				return nil
			}
			if err != nil {
				return err
			}
			w.cache.Add(key, struct{}{})
			w.indexWrites.WithLabelValues(kindNames[key.kind]).Inc()
			return nil
		})
	})
	return g.Wait()
}

var kindNames = [...]string{kindSeries: "series", kindValue: "label_value", kindName: "label_name"}

// discard counts samples dropped outside of Write.
func (w *Writer) discard(outcome string, n int) {
	w.samples.WithLabelValues(outcome).Add(float64(n))
}

func countSamples(batch []storage.SeriesSamples) int {
	n := 0
	for _, s := range batch {
		n += len(s.Samples)
	}
	return n
}
