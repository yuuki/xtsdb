// Package ingest implements the write path: protocol handlers, label
// validation, index maintenance and appends to the hot tier.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	Accepted int
	TooOld   int // window already closed
	TooNew   int // beyond max_sample_future
	Invalid  int // samples of series with invalid labels
	// FirstInvalid describes the first invalid series, if any.
	FirstInvalid error
}

// Rejected returns the number of samples that were not stored.
func (r Result) Rejected() int { return r.TooOld + r.TooNew + r.Invalid }

// Merge adds the counts of o to r.
func (r *Result) Merge(o Result) {
	r.Accepted += o.Accepted
	r.TooOld += o.TooOld
	r.TooNew += o.TooNew
	r.Invalid += o.Invalid
	if r.FirstInvalid == nil {
		r.FirstInvalid = o.FirstInvalid
	}
}

// indexKey identifies an index entry that has already been written.
type indexKey struct {
	kind  uint8
	day   int32
	id    model.SeriesID
	name  string
	value string
}

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
	hot       storage.HotStore
	index     storage.IndexWriter
	cache     *lru.Cache[indexKey, struct{}]
	maxFuture time.Duration
	maxAge    time.Duration
	indexConc int
	logger    *slog.Logger
	now       func() time.Time

	samples      *prometheus.CounterVec
	indexWrites  *prometheus.CounterVec
	writeLatency prometheus.Histogram
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
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	reg := promauto.With(opts.Registerer)
	return &Writer{
		hot:       hot,
		index:     index,
		cache:     cache,
		maxFuture: opts.MaxSampleFuture,
		maxAge:    opts.MaxSampleAge,
		indexConc: opts.IndexWriteConcurrency,
		logger:    opts.Logger,
		now:       time.Now,
		samples: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_ingester_samples_total",
			Help: "Samples received by the ingester, by outcome.",
		}, []string{"outcome"}),
		indexWrites: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_ingester_index_writes_total",
			Help: "Index entries written, by kind.",
		}, []string{"kind"}),
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
		id   model.SeriesID
		lset labels.Labels
		days []int32
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
		id := model.ID(lset)
		entries = append(entries, entry{id: id, lset: lset, days: model.Days(minS, maxS)})
		toStore = append(toStore, storage.SeriesSamples{ID: id, Samples: samples})
	}

	// Index first: a sample must never be stored for a series that cannot
	// be found.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(w.indexConc)
	scheduled := make(map[indexKey]struct{})
	once := func(key indexKey, write func() error) {
		if _, ok := scheduled[key]; ok || w.cache.Contains(key) {
			return
		}
		scheduled[key] = struct{}{}
		g.Go(func() error {
			if err := write(); err != nil {
				return err
			}
			w.cache.Add(key, struct{}{})
			w.indexWrites.WithLabelValues(kindNames[key.kind]).Inc()
			return nil
		})
	}
	for _, e := range entries {
		for _, day := range e.days {
			once(indexKey{kind: kindSeries, day: day, id: e.id}, func() error {
				return w.index.WriteSeries(gctx, day, e.id, e.lset)
			})
			e.lset.Range(func(l labels.Label) {
				once(indexKey{kind: kindValue, day: day, name: l.Name, value: l.Value}, func() error {
					return w.index.WriteLabelValue(gctx, day, l.Name, l.Value)
				})
				once(indexKey{kind: kindName, day: day, name: l.Name}, func() error {
					return w.index.WriteLabelName(gctx, day, l.Name)
				})
			})
		}
	}
	if err := g.Wait(); err != nil {
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
		res.TooOld += ar.TooOld
	}

	w.samples.WithLabelValues("accepted").Add(float64(res.Accepted))
	w.samples.WithLabelValues("too_old").Add(float64(res.TooOld))
	w.samples.WithLabelValues("too_new").Add(float64(res.TooNew))
	w.samples.WithLabelValues("invalid").Add(float64(res.Invalid))
	return res, nil
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
