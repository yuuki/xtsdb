package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Index is a storage.IndexWriter and storage.IndexReader backed by
// Cassandra. Index entries live one day longer than samples so that a
// series stays discoverable for as long as any of its samples exist.
type Index struct {
	session         *gocql.Session
	ttl             int
	readConcurrency int
}

var (
	_ storage.IndexWriter = (*Index)(nil)
	_ storage.IndexReader = (*Index)(nil)
)

// NewIndex creates an Index.
func NewIndex(s *gocql.Session, retention time.Duration, readConcurrency int) *Index {
	ttl := 0
	if retention > 0 {
		ttl = TTLSeconds(retention + 48*time.Hour)
	}
	return &Index{session: s, ttl: ttl, readConcurrency: readConcurrency}
}

// WriteSeries records a series and its postings for a day.
func (x *Index) WriteSeries(ctx context.Context, day int32, id model.SeriesID, lset labels.Labels) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return x.session.Query(`INSERT INTO series (series_id, labels) VALUES (?, ?) USING TTL ?`,
			int64(id), lset.Map(), x.ttl).Idempotent(true).ExecContext(ctx)
	})
	lset.Range(func(l labels.Label) {
		g.Go(func() error {
			return x.session.Query(`INSERT INTO postings (day, name, value, series_id) VALUES (?, ?, ?, ?) USING TTL ?`,
				day, l.Name, l.Value, int64(id), x.ttl).Idempotent(true).ExecContext(ctx)
		})
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("index series %s: %w", id, err)
	}
	return nil
}

// WriteLabelValue records that a label value exists on a day.
func (x *Index) WriteLabelValue(ctx context.Context, day int32, name, value string) error {
	err := x.session.Query(`INSERT INTO label_values (day, name, value) VALUES (?, ?, ?) USING TTL ?`,
		day, name, value, x.ttl).Idempotent(true).ExecContext(ctx)
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
	iter := x.session.Query(`SELECT series_id FROM postings WHERE day = ? AND name = ? AND value = ?`,
		day, name, value).Idempotent(true).PageSize(10000).IterContext(ctx)
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
}

// LabelValues returns the values of a label name seen on a day, sorted.
func (x *Index) LabelValues(ctx context.Context, day int32, name string) ([]string, error) {
	return x.strings(ctx, `SELECT value FROM label_values WHERE day = ? AND name = ?`, day, name)
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
