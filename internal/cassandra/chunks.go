package cassandra

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// maxTTL is the largest TTL Cassandra accepts (20 years).
const maxTTL = 20 * 365 * 24 * time.Hour

// TTLSeconds converts a retention period to a CQL TTL. Zero means forever.
func TTLSeconds(retention time.Duration) int {
	if retention <= 0 {
		return 0
	}
	return int(min(retention, maxTTL) / time.Second)
}

// ChunkStore is a storage.ColdStore backed by Cassandra.
type ChunkStore struct {
	session         *gocql.Session
	ttl             int
	chunkRange      int64
	readConcurrency int
	logger          *slog.Logger
}

var _ storage.ColdStore = (*ChunkStore)(nil)

// NewChunkStore creates a ChunkStore. chunkRange is the width of a chunk
// window in milliseconds; reads look back that far to find chunks starting
// before the queried range.
func NewChunkStore(s *gocql.Session, retention time.Duration, chunkRange int64, readConcurrency int, logger *slog.Logger) *ChunkStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ChunkStore{
		session:         s,
		ttl:             TTLSeconds(retention),
		chunkRange:      chunkRange,
		readConcurrency: readConcurrency,
		logger:          logger,
	}
}

// WriteChunk stores a chunk. The primary key makes the write idempotent.
func (c *ChunkStore) WriteChunk(ctx context.Context, ch storage.ColdChunk) error {
	err := c.session.Query(`INSERT INTO chunks
		(series_id, day, start_ms, fragment, total, digest, part, max_t, samples, encoding, data)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?`,
		int64(ch.Series), model.Day(ch.Start), ch.Start, ch.Fragment, ch.Total, int64(ch.Digest), ch.Part,
		ch.MaxT, ch.Count, int8(ch.Encoding), ch.Data, c.ttl,
	).Idempotent(true).ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("write chunk %s@%d: %w", ch.Series, ch.Start, err)
	}
	return nil
}

// Read returns the persisted samples of the given series within [mint, maxt].
func (c *ChunkStore) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	from := mint - c.chunkRange + 1
	days := model.Days(from, maxt)
	out := make(map[model.SeriesID][]model.Sample, len(ids))
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(c.readConcurrency)
	for _, id := range ids {
		g.Go(func() error {
			samples, err := c.readSeries(ctx, id, days, from, mint, maxt)
			if err != nil {
				return err
			}
			if len(samples) > 0 {
				mu.Lock()
				out[id] = samples
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

func (c *ChunkStore) readSeries(ctx context.Context, id model.SeriesID, days []int32, from, mint, maxt int64) ([]model.Sample, error) {
	// Rows arrive in clustering order, so the parts of a version are
	// consecutive and versions of a window are ordered by (fragment, total,
	// digest): later incarnations, then later states of an incarnation, come
	// last and win when merged (see storage.ColdChunk).
	type version struct {
		start, fragment, digest int64
		total                   int
	}
	var (
		sets    [][]model.Sample
		cur     version
		pending []model.Sample
		started bool
	)
	flushVersion := func() {
		if s := chunk.Filter(chunk.Normalize(pending), mint, maxt); len(s) > 0 {
			sets = append(sets, s)
		}
		pending = nil
	}
	for _, day := range days {
		iter := c.session.Query(`SELECT start_ms, fragment, total, digest, encoding, data FROM chunks
			WHERE series_id = ? AND day = ? AND start_ms >= ? AND start_ms <= ?`,
			int64(id), day, from, maxt,
		).Idempotent(true).IterContext(ctx)
		var (
			v    version
			enc  int8
			data []byte
		)
		for iter.Scan(&v.start, &v.fragment, &v.total, &v.digest, &enc, &data) {
			if started && v != cur {
				flushVersion()
			}
			cur, started = v, true
			samples, err := chunk.Decode(chunk.Encoding(enc), data)
			if err != nil {
				c.logger.Error("skipping undecodable chunk", "series", id, "day", day, "err", err)
			} else {
				pending = append(pending, samples...)
			}
			data = nil
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("read chunks of %s: %w", id, err)
		}
	}
	if started {
		flushVersion()
	}
	return chunk.Merge(sets...), nil
}

// Ping checks connectivity.
func (c *ChunkStore) Ping(ctx context.Context) error {
	return c.session.Query("SELECT release_version FROM system.local").ExecContext(ctx)
}
