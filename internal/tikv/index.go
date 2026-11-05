package tikv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Index is a storage.IndexWriter and storage.IndexReader backed by TiKV.
// The entries of a day expire storage.IndexSlack after the samples of that
// day, so that a series stays discoverable for as long as any of its
// samples exist; entries of days whose samples have already expired are not
// written. Series entries expire storage.IndexSlack after their last write.
//
// The postings of a label pair, the values of a label and the label names
// of a day are each a contiguous range of keys, read with one scan. Unlike
// Cassandra partitions, ranges need no bucketing: TiKV splits a large range
// over several regions by itself.
type Index struct {
	db              *DB
	retention       time.Duration
	readConcurrency int
	now             func() time.Time
}

var (
	_ storage.IndexWriter = (*Index)(nil)
	_ storage.IndexReader = (*Index)(nil)
)

// Bounds of the encoded index entries, from the limits that ingestion
// enforces on label sets (see model.Canonicalize).
const (
	maxStringLen   = binary.MaxVarintLen16 + model.MaxLabelLength
	maxSeriesValue = 1 + 2*model.MaxLabelsPerSeries*maxStringLen
)

// NewIndex creates an Index.
func NewIndex(db *DB, retention time.Duration, readConcurrency int) *Index {
	return &Index{db: db, retention: retention, readConcurrency: max(readConcurrency, 1), now: time.Now}
}

// dayTTL returns the TTL of the entries of a day, and false if the samples
// of that day have expired.
func (x *Index) dayTTL(day int32) (uint64, bool) {
	end := time.UnixMilli((int64(day) + 1) * model.DayMillis)
	return ttlSeconds(x.retention, end.Add(x.retention+storage.IndexSlack), x.now())
}

// seriesTTL is the TTL of a series entry, refreshed every day the series is
// written.
func (x *Index) seriesTTL() uint64 {
	if x.retention <= 0 {
		return 0
	}
	return uint64((x.retention + storage.IndexSlack) / time.Second)
}

// WriteSeries records a series and its postings for a day.
//
// The series entry is claimed with a compare-and-swap that succeeds only if
// the ID is unused. If it is already used by the same labels, the entry is
// kept; if by other labels, the IDs collide: no postings are written and a
// *storage.SeriesCollisionError is returned. The ingester does this once
// per series and day.
//
// A compare-and-swap cannot set a TTL, so the entry is then written again
// with one, which also refreshes the TTL of an existing entry. Between the
// two, a new entry has no TTL and cannot expire, so no other label set can
// claim the ID; an existing entry could, if it expired in that instant,
// after not being written for the whole retention.
func (x *Index) WriteSeries(ctx context.Context, day int32, id model.SeriesID, lset labels.Labels) error {
	ttl, ok := x.dayTTL(day)
	if !ok {
		return nil
	}
	key, value := x.db.keys.series(id), encodeLabels(lset)
	prev, swapped, err := x.db.compareAndSwap(ctx, key, nil, value)
	if err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	if !swapped && !bytes.Equal(prev, value) {
		existing, err := decodeLabels(prev)
		if err != nil {
			return fmt.Errorf("index series %s: the series entry is malformed: %w", id, err)
		}
		return &storage.SeriesCollisionError{ID: id, Labels: lset, Existing: existing}
	}
	if err := x.db.put(ctx, key, value, x.seriesTTL()); err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	keys := make([][]byte, 0, lset.Len())
	values := make([][]byte, 0, lset.Len())
	lset.Range(func(l labels.Label) {
		keys = append(keys, x.db.keys.posting(day, l.Name, l.Value, id))
		values = append(values, presence)
	})
	if len(keys) == 0 {
		return nil
	}
	if err := x.db.putAll(ctx, keys, values, ttl); err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	return nil
}

// WriteLabelValue records that a label value exists on a day.
func (x *Index) WriteLabelValue(ctx context.Context, day int32, name, value string) error {
	ttl, ok := x.dayTTL(day)
	if !ok {
		return nil
	}
	if err := x.db.put(ctx, x.db.keys.labelValue(day, name, value), presence, ttl); err != nil {
		return fmt.Errorf("index label value: %w", err)
	}
	return nil
}

// WriteLabelName records that a label name exists on a day.
func (x *Index) WriteLabelName(ctx context.Context, day int32, name string) error {
	ttl, ok := x.dayTTL(day)
	if !ok {
		return nil
	}
	if err := x.db.put(ctx, x.db.keys.labelName(day, name), presence, ttl); err != nil {
		return fmt.Errorf("index label name: %w", err)
	}
	return nil
}

// Postings returns the series that had the label pair on a day.
func (x *Index) Postings(ctx context.Context, day int32, name, value string) ([]model.SeriesID, error) {
	prefix := x.db.keys.labelPair(day, name, value)
	var out []model.SeriesID
	limit := x.db.scanLimit(len(prefix)+8, len(presence))
	err := x.db.scan(ctx, prefix, prefixEnd(prefix), limit, func(key, _ []byte) error {
		if len(key) != len(prefix)+8 {
			return fmt.Errorf("malformed posting %x", key)
		}
		out = append(out, model.SeriesID(binary.BigEndian.Uint64(key[len(prefix):])))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read postings: %w", err)
	}
	return out, nil
}

// LabelValues returns the values of a label name seen on a day, sorted.
func (x *Index) LabelValues(ctx context.Context, day int32, name string) ([]string, error) {
	return x.strings(ctx, x.db.keys.labelValues(day, name))
}

// LabelNames returns the label names seen on a day, sorted.
func (x *Index) LabelNames(ctx context.Context, day int32) ([]string, error) {
	return x.strings(ctx, x.db.keys.labelNames(day))
}

// strings reads the strings that follow a prefix in keys.
func (x *Index) strings(ctx context.Context, prefix []byte) ([]string, error) {
	var out []string
	limit := x.db.scanLimit(len(prefix)+maxStringLen, len(presence))
	err := x.db.scan(ctx, prefix, prefixEnd(prefix), limit, func(key, _ []byte) error {
		s, rest, err := getString(key[len(prefix):])
		if err != nil || len(rest) > 0 {
			return fmt.Errorf("malformed index entry %x", key)
		}
		out = append(out, s)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	slices.Sort(out)
	return out, nil
}

// SeriesLabels returns the labels of the given series. Unknown series are
// omitted.
func (x *Index) SeriesLabels(ctx context.Context, ids []model.SeriesID) (map[model.SeriesID]labels.Labels, error) {
	out := make(map[model.SeriesID]labels.Labels, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	// Neighbouring keys are likely in the same region, so sorted batches
	// need fewer requests. A batch asks for as many entries as fit in the
	// read budget even if every label set is as large as allowed.
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	batch := x.db.scanLimit(len(x.db.keys.series(0)), maxSeriesValue)
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(x.readConcurrency)
	for b := range slices.Chunk(sorted, batch) {
		g.Go(func() error {
			keys := make([][]byte, len(b))
			for i, id := range b {
				keys[i] = x.db.keys.series(id)
			}
			values, err := x.db.batchGet(ctx, keys)
			if err != nil {
				return fmt.Errorf("read series: %w", err)
			}
			for i, v := range values {
				if v == nil {
					continue
				}
				lset, err := decodeLabels(v)
				if err != nil {
					return fmt.Errorf("read series %s: %w", b[i], err)
				}
				mu.Lock()
				out[b[i]] = lset
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
