// Package query implements the read path: resolving label matchers through
// the index, merging samples from both tiers, and the Prometheus-compatible
// HTTP API.
package query

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Options configures a Queryable.
type Options struct {
	// HotLookback is how far back the hot tier is consulted. It must cover
	// chunk_range + grace_period plus the expected flush lag.
	HotLookback time.Duration
	// Retention bounds queries; zero means unbounded.
	Retention         time.Duration
	MaxSeriesPerQuery int
	SeriesCacheSize   int
	ReadConcurrency   int
}

// Queryable implements storage.Queryable on top of the xtsdb tiers.
type Queryable struct {
	hot   storage.HotStore
	cold  storage.ColdStore
	index storage.IndexReader
	cache *lru.Cache[model.SeriesID, labels.Labels]
	opts  Options
	now   func() time.Time
}

var _ promstorage.Queryable = (*Queryable)(nil)

// NewQueryable creates a Queryable.
func NewQueryable(hot storage.HotStore, cold storage.ColdStore, index storage.IndexReader, opts Options) (*Queryable, error) {
	cache, err := lru.New[model.SeriesID, labels.Labels](opts.SeriesCacheSize)
	if err != nil {
		return nil, err
	}
	return &Queryable{hot: hot, cold: cold, index: index, cache: cache, opts: opts, now: time.Now}, nil
}

// ErrTooManySeries is returned when a selector matches more series than
// allowed.
var ErrTooManySeries = errors.New("query matches too many series")

// ErrNoPositiveMatcher is returned for selectors that would match every series.
var ErrNoPositiveMatcher = errors.New("selector must contain at least one matcher that does not match the empty string")

// Querier implements storage.Queryable.
func (q *Queryable) Querier(mint, maxt int64) (promstorage.Querier, error) {
	return &querier{q: q, mint: mint, maxt: maxt}, nil
}

// clamp restricts a time range to what the storage can hold.
func (q *Queryable) clamp(mint, maxt int64) (int64, int64) {
	now := q.now()
	if q.opts.Retention > 0 {
		mint = max(mint, now.Add(-q.opts.Retention).UnixMilli())
	}
	maxt = min(maxt, now.Add(24*time.Hour).UnixMilli())
	return mint, maxt
}

type seriesRef struct {
	id   model.SeriesID
	lset labels.Labels
}

// Series resolves matchers to series active within [mint, maxt], sorted by
// labels.
func (q *Queryable) Series(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]seriesRef, error) {
	mint, maxt = q.clamp(mint, maxt)
	if mint > maxt {
		return nil, nil
	}
	var positive []*labels.Matcher
	for _, m := range matchers {
		if !m.Matches("") {
			positive = append(positive, m)
		}
	}
	if len(positive) == 0 {
		return nil, ErrNoPositiveMatcher
	}
	// Equality matchers are cheapest; resolve them first so that others can
	// be skipped once the candidate set is empty.
	slices.SortStableFunc(positive, func(a, b *labels.Matcher) int {
		return matcherCost(a) - matcherCost(b)
	})

	days := model.Days(mint, maxt)
	perDay := make([]map[model.SeriesID]struct{}, len(days))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(q.opts.ReadConcurrency)
	for i, day := range days {
		g.Go(func() error {
			ids, err := q.postingsForDay(gctx, day, positive)
			perDay[i] = ids
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	candidates := make(map[model.SeriesID]struct{})
	for _, ids := range perDay {
		maps.Copy(candidates, ids)
	}
	if q.opts.MaxSeriesPerQuery > 0 && len(candidates) > q.opts.MaxSeriesPerQuery {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManySeries, len(candidates), q.opts.MaxSeriesPerQuery)
	}

	lsets, err := q.labels(ctx, slices.Collect(maps.Keys(candidates)))
	if err != nil {
		return nil, err
	}
	out := make([]seriesRef, 0, len(lsets))
	for id, lset := range lsets {
		if matchesAll(lset, matchers) {
			out = append(out, seriesRef{id: id, lset: lset})
		}
	}
	slices.SortFunc(out, func(a, b seriesRef) int { return labels.Compare(a.lset, b.lset) })
	return out, nil
}

func matcherCost(m *labels.Matcher) int {
	switch {
	case m.Type == labels.MatchEqual:
		return 0
	case len(m.SetMatches()) > 0:
		return 1
	default:
		return 2
	}
}

// postingsForDay intersects the postings of every positive matcher.
func (q *Queryable) postingsForDay(ctx context.Context, day int32, matchers []*labels.Matcher) (map[model.SeriesID]struct{}, error) {
	var result map[model.SeriesID]struct{}
	for _, m := range matchers {
		values, err := q.matchingValues(ctx, day, m)
		if err != nil {
			return nil, err
		}
		union := make(map[model.SeriesID]struct{})
		for _, v := range values {
			ids, err := q.index.Postings(ctx, day, m.Name, v)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				if result == nil {
					union[id] = struct{}{}
				} else if _, ok := result[id]; ok {
					union[id] = struct{}{}
				}
			}
		}
		result = union
		if len(result) == 0 {
			break
		}
	}
	return result, nil
}

// matchingValues lists the label values a matcher selects on a day.
func (q *Queryable) matchingValues(ctx context.Context, day int32, m *labels.Matcher) ([]string, error) {
	if m.Type == labels.MatchEqual {
		return []string{m.Value}, nil
	}
	if set := m.SetMatches(); len(set) > 0 && m.Type == labels.MatchRegexp {
		return set, nil
	}
	all, err := q.index.LabelValues(ctx, day, m.Name)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, v := range all {
		if m.Matches(v) {
			out = append(out, v)
		}
	}
	return out, nil
}

// labels returns the label sets of series, using the cache where possible.
func (q *Queryable) labels(ctx context.Context, ids []model.SeriesID) (map[model.SeriesID]labels.Labels, error) {
	out := make(map[model.SeriesID]labels.Labels, len(ids))
	var missing []model.SeriesID
	for _, id := range ids {
		if l, ok := q.cache.Get(id); ok {
			out[id] = l
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return out, nil
	}
	fetched, err := q.index.SeriesLabels(ctx, missing)
	if err != nil {
		return nil, err
	}
	for id, l := range fetched {
		q.cache.Add(id, l)
		out[id] = l
	}
	return out, nil
}

func matchesAll(lset labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

// Samples reads and merges the samples of series from both tiers.
func (q *Queryable) Samples(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	mint, maxt = q.clamp(mint, maxt)
	if mint > maxt || len(ids) == 0 {
		return nil, nil
	}
	var hot, cold map[model.SeriesID][]model.Sample
	g, gctx := errgroup.WithContext(ctx)
	hotMin := max(mint, q.now().Add(-q.opts.HotLookback).UnixMilli())
	if hotMin <= maxt {
		g.Go(func() (err error) {
			hot, err = q.hot.Read(gctx, ids, hotMin, maxt)
			return err
		})
	}
	g.Go(func() (err error) {
		cold, err = q.cold.Read(gctx, ids, mint, maxt)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := make(map[model.SeriesID][]model.Sample, len(ids))
	for _, id := range ids {
		// The hot tier holds the most recent writes, so it takes precedence.
		if s := chunk.Merge(cold[id], hot[id]); len(s) > 0 {
			out[id] = s
		}
	}
	return out, nil
}

type querier struct {
	q          *Queryable
	mint, maxt int64
}

func (qr *querier) Select(ctx context.Context, _ bool, hints *promstorage.SelectHints, matchers ...*labels.Matcher) promstorage.SeriesSet {
	mint, maxt := qr.mint, qr.maxt
	if hints != nil {
		mint, maxt = max(mint, hints.Start), min(maxt, hints.End)
	}
	refs, err := qr.q.Series(ctx, mint, maxt, matchers...)
	if err != nil {
		return promstorage.ErrSeriesSet(err)
	}
	if hints != nil && hints.Func == "series" {
		out := make([]promstorage.Series, len(refs))
		for i, r := range refs {
			out[i] = &series{lset: r.lset}
		}
		return &seriesSet{series: out}
	}
	ids := make([]model.SeriesID, len(refs))
	for i, r := range refs {
		ids[i] = r.id
	}
	samples, err := qr.q.Samples(ctx, ids, mint, maxt)
	if err != nil {
		return promstorage.ErrSeriesSet(err)
	}
	out := make([]promstorage.Series, 0, len(refs))
	for _, r := range refs {
		if s := samples[r.id]; len(s) > 0 {
			out = append(out, &series{lset: r.lset, samples: s})
		}
	}
	return &seriesSet{series: out}
}

func (qr *querier) LabelValues(ctx context.Context, name string, hints *promstorage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	vals, err := qr.q.LabelValues(ctx, qr.mint, qr.maxt, name, limit, matchers...)
	return vals, nil, err
}

func (qr *querier) LabelNames(ctx context.Context, hints *promstorage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	names, err := qr.q.LabelNames(ctx, qr.mint, qr.maxt, limit, matchers...)
	return names, nil, err
}

func (qr *querier) Close() error { return nil }

// LabelValues returns the sorted values of a label within [mint, maxt],
// optionally restricted to series matching matchers.
func (q *Queryable) LabelValues(ctx context.Context, mint, maxt int64, name string, limit int, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		refs, err := q.Series(ctx, mint, maxt, matchers...)
		if err != nil {
			return nil, err
		}
		set := make(map[string]struct{})
		for _, r := range refs {
			if v := r.lset.Get(name); v != "" {
				set[v] = struct{}{}
			}
		}
		return sortedLimit(set, limit), nil
	}
	return q.perDayStrings(ctx, mint, maxt, limit, func(ctx context.Context, day int32) ([]string, error) {
		return q.index.LabelValues(ctx, day, name)
	})
}

// LabelNames returns the sorted label names within [mint, maxt], optionally
// restricted to series matching matchers.
func (q *Queryable) LabelNames(ctx context.Context, mint, maxt int64, limit int, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		refs, err := q.Series(ctx, mint, maxt, matchers...)
		if err != nil {
			return nil, err
		}
		set := make(map[string]struct{})
		for _, r := range refs {
			r.lset.Range(func(l labels.Label) { set[l.Name] = struct{}{} })
		}
		return sortedLimit(set, limit), nil
	}
	return q.perDayStrings(ctx, mint, maxt, limit, q.index.LabelNames)
}

func (q *Queryable) perDayStrings(ctx context.Context, mint, maxt int64, limit int, read func(context.Context, int32) ([]string, error)) ([]string, error) {
	mint, maxt = q.clamp(mint, maxt)
	days := model.Days(mint, maxt)
	var mu sync.Mutex
	set := make(map[string]struct{})
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(q.opts.ReadConcurrency)
	for _, day := range days {
		g.Go(func() error {
			vals, err := read(gctx, day)
			if err != nil {
				return err
			}
			mu.Lock()
			for _, v := range vals {
				set[v] = struct{}{}
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return sortedLimit(set, limit), nil
}

func sortedLimit(set map[string]struct{}, limit int) []string {
	out := slices.Sorted(maps.Keys(set))
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// seriesSet is an in-memory storage.SeriesSet.
type seriesSet struct {
	series []promstorage.Series
	cur    int
}

func (s *seriesSet) Next() bool {
	if s.cur >= len(s.series) {
		return false
	}
	s.cur++
	return true
}
func (s *seriesSet) At() promstorage.Series            { return s.series[s.cur-1] }
func (s *seriesSet) Err() error                        { return nil }
func (s *seriesSet) Warnings() annotations.Annotations { return nil }

type series struct {
	lset    labels.Labels
	samples []model.Sample
}

func (s *series) Labels() labels.Labels { return s.lset }

func (s *series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return promstorage.NewListSeriesIterator(sampleList(s.samples))
}

// sampleList adapts sorted float samples to the Prometheus iterator.
type sampleList []model.Sample

func (l sampleList) Get(i int) chunks.Sample { return fsample{t: l[i].T, v: l[i].V} }
func (l sampleList) Len() int                { return len(l) }

type fsample struct {
	t int64
	v float64
}

func (s fsample) T() int64                      { return s.t }
func (s fsample) ST() int64                     { return 0 }
func (s fsample) F() float64                    { return s.v }
func (s fsample) H() *histogram.Histogram       { return nil }
func (s fsample) FH() *histogram.FloatHistogram { return nil }
func (s fsample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s fsample) Copy() chunks.Sample           { return s }
