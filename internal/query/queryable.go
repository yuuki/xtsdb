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
	"unicode/utf8"

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
	// HotLookback is the minimum span read from the hot tier. It must cover
	// chunk_range + grace_period. Chunks older than that which are still in
	// Redis (for example while flushing lags) are found through the hot
	// tier's backlog and read as well.
	HotLookback time.Duration
	// Retention bounds queries. When it is zero, MaxQueryLookback does.
	Retention         time.Duration
	MaxQueryLookback  time.Duration
	MaxSeriesPerQuery int
	SeriesCacheSize   int
	ReadConcurrency   int
	// LoadBatchSize is the number of series whose samples are loaded at
	// once while the engine iterates. It bounds the memory of raw samples.
	LoadBatchSize int
}

// Defaults applied by NewQueryable.
const (
	defaultMaxQueryLookback = 365 * 24 * time.Hour
	defaultLoadBatchSize    = 256
	// When this few candidates remain, further matchers are applied to the
	// series labels instead of reading more postings.
	smallCandidateSet = 1024
	backlogRefresh    = 10 * time.Second
)

// Queryable implements storage.Queryable on top of the xtsdb tiers.
type Queryable struct {
	hot   storage.HotStore
	cold  storage.ColdStore
	index storage.IndexReader
	cache *lru.Cache[model.SeriesID, labels.Labels]
	opts  Options
	now   func() time.Time

	mu            sync.Mutex
	oldestHot     int64 // start of the oldest chunk in Redis, 0 if unknown
	oldestFetched time.Time
}

var _ promstorage.Queryable = (*Queryable)(nil)

// NewQueryable creates a Queryable.
func NewQueryable(hot storage.HotStore, cold storage.ColdStore, index storage.IndexReader, opts Options) (*Queryable, error) {
	cache, err := lru.New[model.SeriesID, labels.Labels](opts.SeriesCacheSize)
	if err != nil {
		return nil, err
	}
	if opts.MaxQueryLookback <= 0 {
		opts.MaxQueryLookback = defaultMaxQueryLookback
	}
	if opts.LoadBatchSize <= 0 {
		opts.LoadBatchSize = defaultLoadBatchSize
	}
	return &Queryable{hot: hot, cold: cold, index: index, cache: cache, opts: opts, now: time.Now}, nil
}

// Errors returned by the Queryable.
var (
	ErrTooManySeries     = errors.New("query matches too many series")
	ErrNoPositiveMatcher = errors.New("selector must contain at least one matcher that does not match the empty string")
	// ErrUnavailable wraps failures of the storage backends; they are
	// transient and the query may be retried.
	ErrUnavailable = errors.New("storage unavailable")
)

func unavailable(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

// Querier implements storage.Queryable.
func (q *Queryable) Querier(mint, maxt int64) (promstorage.Querier, error) {
	return &querier{q: q, mint: mint, maxt: maxt}, nil
}

// clamp restricts a time range to what the storage can hold, which also
// bounds the number of day partitions a query touches.
func (q *Queryable) clamp(mint, maxt int64) (int64, int64) {
	now := q.now()
	lookback := q.opts.Retention
	if lookback <= 0 {
		lookback = q.opts.MaxQueryLookback
	}
	mint = max(mint, now.Add(-lookback).UnixMilli())
	maxt = min(maxt, now.Add(24*time.Hour).UnixMilli())
	return mint, maxt
}

// hotStart returns the earliest time that can still be held by the hot
// tier: the configured lookback, extended to the oldest chunk Redis holds.
// Chunks present when the backlog was read start no earlier than that
// chunk; chunks created later start within chunk_range + grace_period.
func (q *Queryable) hotStart(ctx context.Context) int64 {
	start := q.now().Add(-q.opts.HotLookback).UnixMilli()
	q.mu.Lock()
	defer q.mu.Unlock()
	if time.Since(q.oldestFetched) >= backlogRefresh {
		bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		b, err := q.hot.Backlog(bctx)
		cancel()
		if err == nil {
			q.oldestHot = b.OldestStart
			q.oldestFetched = time.Now()
		}
	}
	if q.oldestHot != 0 {
		start = min(start, q.oldestHot)
	}
	return start
}

type seriesRef struct {
	id   model.SeriesID
	lset labels.Labels
}

// Series resolves matchers to series active within [mint, maxt], sorted by
// labels. With limit > 0 at most limit series are returned and truncated
// reports whether more matched; without a limit, more than
// MaxSeriesPerQuery candidates is an error.
func (q *Queryable) Series(ctx context.Context, mint, maxt int64, limit int, matchers ...*labels.Matcher) (refs []seriesRef, truncated bool, err error) {
	mint, maxt = q.clamp(mint, maxt)
	if mint > maxt {
		return nil, false, nil
	}
	var positive []*labels.Matcher
	for _, m := range matchers {
		if !m.Matches("") {
			positive = append(positive, m)
		}
	}
	if len(positive) == 0 {
		return nil, false, ErrNoPositiveMatcher
	}
	// Cheap matchers first, so that expensive ones can be skipped once the
	// candidate set is small.
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
		return nil, false, unavailable(err)
	}
	candidates := make(map[model.SeriesID]struct{})
	for i, ids := range perDay {
		maps.Copy(candidates, ids)
		perDay[i] = nil
	}
	if limit <= 0 && q.opts.MaxSeriesPerQuery > 0 && len(candidates) > q.opts.MaxSeriesPerQuery {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrTooManySeries, len(candidates), q.opts.MaxSeriesPerQuery)
	}

	ids := slices.Sorted(maps.Keys(candidates))
	const labelBatch = 1000
	for start := 0; start < len(ids); start += labelBatch {
		lsets, err := q.labels(ctx, ids[start:min(start+labelBatch, len(ids))])
		if err != nil {
			return nil, false, unavailable(err)
		}
		for id, lset := range lsets {
			if matchesAll(lset, matchers) {
				refs = append(refs, seriesRef{id: id, lset: lset})
			}
		}
		if limit > 0 && len(refs) > limit {
			break
		}
	}
	slices.SortFunc(refs, func(a, b seriesRef) int { return labels.Compare(a.lset, b.lset) })
	if limit > 0 && len(refs) > limit {
		return refs[:limit], true, nil
	}
	return refs, false, nil
}

func matcherCost(m *labels.Matcher) int {
	switch {
	case m.Type == labels.MatchEqual:
		return 0
	case m.Type == labels.MatchRegexp && len(m.SetMatches()) > 0:
		return 1
	default:
		return 2
	}
}

// postingsForDay intersects the postings of the positive matchers.
func (q *Queryable) postingsForDay(ctx context.Context, day int32, matchers []*labels.Matcher) (map[model.SeriesID]struct{}, error) {
	var result map[model.SeriesID]struct{}
	for i, m := range matchers {
		if i > 0 && matcherCost(m) == 2 && len(result) <= smallCandidateSet {
			break // the remaining matchers are applied to the labels
		}
		values, err := q.matchingValues(ctx, day, m)
		if err != nil {
			return nil, err
		}
		lists := make([][]model.SeriesID, len(values))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(16)
		for j, v := range values {
			g.Go(func() (err error) {
				lists[j], err = q.index.Postings(gctx, day, m.Name, v)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		union := make(map[model.SeriesID]struct{})
		for _, ids := range lists {
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
	// Stored labels are valid UTF-8, so other values cannot match.
	valid := func(vs []string) []string {
		return slices.DeleteFunc(vs, func(v string) bool { return !utf8.ValidString(v) })
	}
	if m.Type == labels.MatchEqual {
		return valid([]string{m.Value}), nil
	}
	if set := m.SetMatches(); len(set) > 0 && m.Type == labels.MatchRegexp {
		return valid(slices.Clone(set)), nil
	}
	all, err := q.index.LabelValues(ctx, day, m.Name)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(v string) bool { return !m.Matches(v) }), nil
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
	// The hot tier must be read strictly before the cold tier. A flusher
	// writes a chunk to the cold tier before deleting it from the hot tier,
	// so a chunk missing from the hot read was already persisted when the
	// cold read starts. Reading both concurrently can miss a chunk that is
	// persisted after the cold read and deleted before the hot read.
	var hot map[model.SeriesID][]model.Sample
	if hotMin := max(mint, q.hotStart(ctx)); hotMin <= maxt {
		var err error
		if hot, err = q.hot.Read(ctx, ids, hotMin, maxt); err != nil {
			return nil, unavailable(err)
		}
	}
	cold, err := q.cold.Read(ctx, ids, mint, maxt)
	if err != nil {
		return nil, unavailable(err)
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
	refs, _, err := qr.q.Series(ctx, mint, maxt, 0, matchers...)
	if err != nil {
		return promstorage.ErrSeriesSet(err)
	}
	return &lazySet{ctx: ctx, q: qr.q, refs: refs, mint: mint, maxt: maxt, labelsOnly: hints != nil && hints.Func == "series", batchStart: -1}
}

func (qr *querier) LabelValues(ctx context.Context, name string, hints *promstorage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	vals, _, err := qr.q.LabelValues(ctx, qr.mint, qr.maxt, name, limit, matchers...)
	return vals, nil, err
}

func (qr *querier) LabelNames(ctx context.Context, hints *promstorage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	names, _, err := qr.q.LabelNames(ctx, qr.mint, qr.maxt, limit, matchers...)
	return names, nil, err
}

func (qr *querier) Close() error { return nil }

// LabelValues returns the sorted values of a label within [mint, maxt],
// optionally restricted to series matching matchers. truncated reports
// whether limit cut the result.
func (q *Queryable) LabelValues(ctx context.Context, mint, maxt int64, name string, limit int, matchers ...*labels.Matcher) ([]string, bool, error) {
	if len(matchers) > 0 {
		refs, _, err := q.Series(ctx, mint, maxt, 0, matchers...)
		if err != nil {
			return nil, false, err
		}
		set := make(map[string]struct{})
		for _, r := range refs {
			if v := r.lset.Get(name); v != "" {
				set[v] = struct{}{}
			}
		}
		out, truncated := sortedLimit(set, limit)
		return out, truncated, nil
	}
	return q.perDayStrings(ctx, mint, maxt, limit, func(ctx context.Context, day int32) ([]string, error) {
		return q.index.LabelValues(ctx, day, name)
	})
}

// LabelNames returns the sorted label names within [mint, maxt], optionally
// restricted to series matching matchers.
func (q *Queryable) LabelNames(ctx context.Context, mint, maxt int64, limit int, matchers ...*labels.Matcher) ([]string, bool, error) {
	if len(matchers) > 0 {
		refs, _, err := q.Series(ctx, mint, maxt, 0, matchers...)
		if err != nil {
			return nil, false, err
		}
		set := make(map[string]struct{})
		for _, r := range refs {
			r.lset.Range(func(l labels.Label) { set[l.Name] = struct{}{} })
		}
		out, truncated := sortedLimit(set, limit)
		return out, truncated, nil
	}
	return q.perDayStrings(ctx, mint, maxt, limit, q.index.LabelNames)
}

func (q *Queryable) perDayStrings(ctx context.Context, mint, maxt int64, limit int, read func(context.Context, int32) ([]string, error)) ([]string, bool, error) {
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
		return nil, false, unavailable(err)
	}
	out, truncated := sortedLimit(set, limit)
	return out, truncated, nil
}

// sortedLimit returns the sorted members of set, never nil.
func sortedLimit(set map[string]struct{}, limit int) ([]string, bool) {
	out := slices.Sorted(maps.Keys(set))
	if out == nil {
		out = []string{}
	}
	if limit > 0 && len(out) > limit {
		return out[:limit], true
	}
	return out, false
}

// lazySet is a SeriesSet whose samples are loaded only when the engine
// iterates a series, LoadBatchSize series at a time. The engine enforces
// max_samples on what it keeps, so raw samples held by xtsdb stay bounded
// by one batch instead of the whole query.
type lazySet struct {
	ctx        context.Context
	q          *Queryable
	refs       []seriesRef
	mint, maxt int64
	labelsOnly bool
	cur        int

	mu         sync.Mutex
	batchStart int
	batch      map[model.SeriesID][]model.Sample
}

func (s *lazySet) Next() bool {
	if s.cur >= len(s.refs) {
		return false
	}
	s.cur++
	return true
}

func (s *lazySet) At() promstorage.Series {
	return &lazySeries{set: s, idx: s.cur - 1}
}

func (s *lazySet) Err() error                        { return nil }
func (s *lazySet) Warnings() annotations.Annotations { return nil }

func (s *lazySet) samples(idx int) ([]model.Sample, error) {
	if s.labelsOnly {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.q.opts.LoadBatchSize
	if s.batchStart < 0 || idx < s.batchStart || idx >= s.batchStart+n {
		end := min(idx+n, len(s.refs))
		ids := make([]model.SeriesID, 0, end-idx)
		for _, r := range s.refs[idx:end] {
			ids = append(ids, r.id)
		}
		s.batch = nil // release the previous batch before loading
		batch, err := s.q.Samples(s.ctx, ids, s.mint, s.maxt)
		if err != nil {
			s.batchStart = -1
			return nil, err
		}
		s.batch, s.batchStart = batch, idx
	}
	return s.batch[s.refs[idx].id], nil
}

type lazySeries struct {
	set *lazySet
	idx int
}

func (s *lazySeries) Labels() labels.Labels { return s.set.refs[s.idx].lset }

func (s *lazySeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	samples, err := s.set.samples(s.idx)
	if err != nil {
		return errIterator{Iterator: promstorage.NewListSeriesIterator(sampleList(nil)), err: err}
	}
	return promstorage.NewListSeriesIterator(sampleList(samples))
}

// errIterator is an empty iterator that reports a load error.
type errIterator struct {
	chunkenc.Iterator
	err error
}

func (e errIterator) Err() error { return e.err }

// sampleList adapts sorted samples to the Prometheus iterator.
type sampleList []model.Sample

func (l sampleList) Get(i int) chunks.Sample {
	if l[i].H != nil {
		return fhsample{t: l[i].T, h: l[i].H}
	}
	return fsample{t: l[i].T, v: l[i].V}
}
func (l sampleList) Len() int { return len(l) }

// fhsample is a native histogram sample.
type fhsample struct {
	t int64
	h *histogram.FloatHistogram
}

func (s fhsample) T() int64                      { return s.t }
func (s fhsample) ST() int64                     { return 0 }
func (s fhsample) F() float64                    { return 0 }
func (s fhsample) H() *histogram.Histogram       { return nil }
func (s fhsample) FH() *histogram.FloatHistogram { return s.h }
func (s fhsample) Type() chunkenc.ValueType      { return chunkenc.ValFloatHistogram }
func (s fhsample) Copy() chunks.Sample           { return fhsample{t: s.t, h: s.h.Copy()} }

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
