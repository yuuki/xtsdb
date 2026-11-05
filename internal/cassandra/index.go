package cassandra

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Index is a storage.IndexWriter and storage.IndexReader backed by
// Cassandra. Index entries live one day longer than samples so that a
// series stays discoverable for as long as any of its samples exist.
//
// Postings and label values are spread over a fixed number of buckets per
// day and label (storage.index_buckets), so that a label pair shared by
// millions of series, or a label with millions of values, does not make one
// huge partition. Writes go to one bucket; reads query every bucket in
// parallel.
type Index struct {
	session         *gocql.Session
	ttl             int
	readConcurrency int
	buckets         int
}

var (
	_ storage.IndexWriter = (*Index)(nil)
	_ storage.IndexReader = (*Index)(nil)
)

// NewIndex creates an Index with the given number of buckets, which must
// match the one the index was written with (see CheckLayout).
func NewIndex(s *gocql.Session, retention time.Duration, readConcurrency, buckets int) *Index {
	ttl := 0
	if retention > 0 {
		ttl = TTLSeconds(retention + 48*time.Hour)
	}
	return &Index{session: s, ttl: ttl, readConcurrency: readConcurrency, buckets: max(buckets, 1)}
}

// postingsBucket and valueBucket place index entries in buckets. They are
// part of the storage format.
func postingsBucket(id model.SeriesID, buckets int) int16 {
	return int16(uint64(id) % uint64(buckets))
}

func valueBucket(value string, buckets int) int16 {
	return int16(xxhash.Sum64String(value) % uint64(buckets))
}

// WriteSeries records a series and its postings for a day.
//
// The series row is written with a lightweight transaction that succeeds
// only if the ID is unused or already belongs to the same labels (which
// refreshes its TTL). If it belongs to other labels, the IDs collide: no
// postings are written and a *storage.SeriesCollisionError is returned.
// The ingester does this once per series and day.
func (x *Index) WriteSeries(ctx context.Context, day int32, id model.SeriesID, lset labels.Labels) error {
	m := lset.Map()
	var existing map[string]string
	applied, err := x.session.Query(`UPDATE series USING TTL ? SET labels = ? WHERE series_id = ? IF labels IN (?, null)`,
		x.ttl, m, int64(id), m).Idempotent(true).ScanCASContext(ctx, &existing)
	if err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	if !applied {
		return &storage.SeriesCollisionError{ID: id, Labels: lset, Existing: labels.FromMap(existing)}
	}
	if err := x.writePostings(ctx, day, id, lset); err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	return nil
}

func (x *Index) writePostings(ctx context.Context, day int32, id model.SeriesID, lset labels.Labels) error {
	g, ctx := errgroup.WithContext(ctx)
	bucket := postingsBucket(id, x.buckets)
	lset.Range(func(l labels.Label) {
		g.Go(func() error {
			return x.session.Query(`INSERT INTO postings (day, name, value, bucket, series_id) VALUES (?, ?, ?, ?, ?) USING TTL ?`,
				day, l.Name, l.Value, bucket, int64(id), x.ttl).Idempotent(true).ExecContext(ctx)
		})
	})
	return g.Wait()
}

// WriteLabelValue records that a label value exists on a day.
func (x *Index) WriteLabelValue(ctx context.Context, day int32, name, value string) error {
	err := x.session.Query(`INSERT INTO label_values (day, name, bucket, value) VALUES (?, ?, ?, ?) USING TTL ?`,
		day, name, valueBucket(value, x.buckets), value, x.ttl).Idempotent(true).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("index label value: %w", err)
	}
	return nil
}

// WriteLabelName records that a label name exists on a day.
func (x *Index) WriteLabelName(ctx context.Context, day int32, name string) error {
	err := x.session.Query(`INSERT INTO label_names (day, name) VALUES (?, ?) USING TTL ?`,
		day, name, x.ttl).Idempotent(true).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("index label name: %w", err)
	}
	return nil
}

// Postings returns the series that had the label pair on a day.
func (x *Index) Postings(ctx context.Context, day int32, name, value string) ([]model.SeriesID, error) {
	return readBuckets(ctx, x.buckets, func(ctx context.Context, bucket int16) ([]model.SeriesID, error) {
		iter := x.session.Query(`SELECT series_id FROM postings WHERE day = ? AND name = ? AND value = ? AND bucket = ?`,
			day, name, value, bucket).Idempotent(true).PageSize(10000).IterContext(ctx)
		var (
			id  int64
			out []model.SeriesID
		)
		for iter.Scan(&id) {
			out = append(out, model.SeriesID(id))
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("read postings: %w", err)
		}
		return out, nil
	})
}

// LabelValues returns the values of a label name seen on a day, sorted.
func (x *Index) LabelValues(ctx context.Context, day int32, name string) ([]string, error) {
	vals, err := readBuckets(ctx, x.buckets, func(ctx context.Context, bucket int16) ([]string, error) {
		return x.strings(ctx, `SELECT value FROM label_values WHERE day = ? AND name = ? AND bucket = ?`, day, name, bucket)
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(vals)
	return vals, nil
}

// readBuckets reads every bucket in parallel and concatenates the results.
// Buckets are disjoint, so there is nothing to de-duplicate.
func readBuckets[T any](ctx context.Context, buckets int, read func(context.Context, int16) ([]T, error)) ([]T, error) {
	if buckets == 1 {
		return read(ctx, 0)
	}
	parts := make([][]T, buckets)
	g, ctx := errgroup.WithContext(ctx)
	for b := range buckets {
		g.Go(func() (err error) {
			parts[b], err = read(ctx, int16(b))
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.Concat(parts...), nil
}

// LabelNames returns the label names seen on a day, sorted.
func (x *Index) LabelNames(ctx context.Context, day int32) ([]string, error) {
	return x.strings(ctx, `SELECT name FROM label_names WHERE day = ?`, day)
}

func (x *Index) strings(ctx context.Context, stmt string, args ...any) ([]string, error) {
	iter := x.session.Query(stmt, args...).Idempotent(true).PageSize(10000).IterContext(ctx)
	var (
		s   string
		out []string
	)
	for iter.Scan(&s) {
		out = append(out, s)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	return out, nil
}

// SeriesLabels returns the labels of the given series. Unknown series are
// omitted.
func (x *Index) SeriesLabels(ctx context.Context, ids []model.SeriesID) (map[model.SeriesID]labels.Labels, error) {
	out := make(map[model.SeriesID]labels.Labels, len(ids))
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(x.readConcurrency)
	for _, id := range ids {
		g.Go(func() error {
			var m map[string]string
			err := x.session.Query(`SELECT labels FROM series WHERE series_id = ?`, int64(id)).
				Idempotent(true).ScanContext(ctx, &m)
			if errors.Is(err, gocql.ErrNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("read series %s: %w", id, err)
			}
			mu.Lock()
			out[id] = labels.FromMap(m)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
