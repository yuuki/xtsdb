//go:build !notikv && (!darwin || cgo)

package tikv

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// ChunkStore is a storage.ColdStore backed by TiKV.
//
// A part is stored in one or more segments (see segmentSize); the key of a
// segment holds the identity of the part (series, window start, version,
// part number) and its position, so writing a part again writes the same
// entries. The chunks of a series are contiguous and ordered by window
// start, so a read is a single range scan per series.
type ChunkStore struct {
	db              *DB
	retention       time.Duration
	chunkRange      int64
	readConcurrency int
	logger          *slog.Logger
	now             func() time.Time
}

var _ storage.ColdStore = (*ChunkStore)(nil)

// NewChunkStore creates a ChunkStore. chunkRange is the width of a chunk
// window in milliseconds; reads look back that far to find chunks starting
// before the queried range.
func NewChunkStore(db *DB, retention time.Duration, chunkRange int64, readConcurrency int, logger *slog.Logger) *ChunkStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ChunkStore{
		db:              db,
		retention:       retention,
		chunkRange:      chunkRange,
		readConcurrency: max(readConcurrency, 1),
		logger:          logger,
		now:             time.Now,
	}
}

// WriteChunk stores a part. It expires storage.retention after its last
// sample; a part that is already older than that is not written.
func (c *ChunkStore) WriteChunk(ctx context.Context, ch storage.ColdChunk) error {
	ttl, ok := ttlSeconds(c.retention, time.UnixMilli(ch.MaxT).Add(c.retention), c.now())
	if !ok {
		return nil
	}
	if ch.Count < 0 || int64(ch.Count) > math.MaxUint32 {
		return fmt.Errorf("write chunk %s@%d: invalid sample count %d", ch.Series, ch.Start, ch.Count)
	}
	n := max(1, (len(ch.Data)+segmentSize-1)/segmentSize)
	h := segmentHeader{encoding: ch.Encoding, segments: uint32(n), maxT: ch.MaxT, count: uint32(ch.Count),
		sum: xxhash.Sum64(ch.Data)}
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := range n {
		data := ch.Data[i*segmentSize : min(len(ch.Data), (i+1)*segmentSize)]
		keys[i] = c.db.keys.chunk(ch.Series, segmentKey{
			start: ch.Start, fragment: ch.Fragment, total: int64(ch.Total), digest: int64(ch.Digest),
			part: int64(ch.Part), seg: uint32(i),
		})
		values[i] = appendSegment(make([]byte, 0, chunkHeaderLen+len(data)), h, data)
	}
	if err := c.db.putAll(ctx, keys, values, ttl); err != nil {
		return fmt.Errorf("write chunk %s@%d: %w", ch.Series, ch.Start, err)
	}
	return nil
}

// Read returns the persisted samples of the given series within [mint, maxt].
func (c *ChunkStore) Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	// Chunks that start up to chunkRange-1 before mint overlap it.
	from := int64(math.MinInt64)
	if mint >= math.MinInt64+(c.chunkRange-1) {
		from = mint - (c.chunkRange - 1)
	}
	out := make(map[model.SeriesID][]model.Sample, len(ids))
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(c.readConcurrency)
	for _, id := range ids {
		g.Go(func() error {
			samples, err := c.readSeries(ctx, id, from, mint, maxt)
			if err != nil {
				return fmt.Errorf("read chunks of %s: %w", id, err)
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

func (c *ChunkStore) readSeries(ctx context.Context, id model.SeriesID, from, mint, maxt int64) ([]model.Sample, error) {
	if maxt < from {
		return nil, nil
	}
	prefix := c.db.keys.chunkSeries(id)
	r := versionReader{mint: mint, maxt: maxt, logger: c.logger.With("series", id)}
	limit := c.db.scanLimit(len(prefix)+chunkKeyLen, chunkHeaderLen+segmentSize)
	err := c.db.scan(ctx, c.db.keys.chunkFrom(id, from), c.db.keys.chunkUntil(id, maxt), limit, func(key, value []byte) error {
		s, err := parseChunk(key, len(prefix))
		if err != nil {
			c.logger.Error("skipping malformed chunk key", "series", id, "key", fmt.Sprintf("%x", key))
			return nil
		}
		r.add(s, value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return r.finish(), nil
}

// versionReader assembles the segments of a series, which arrive in key
// order: the segments of a part are consecutive, the parts of a version are
// consecutive, and the versions of a window come in the order of
// precedence, so later versions win when merged.
//
// A part is used only if its segments 0 to n-1 are all present, carry the
// same header, and their data matches the checksum, where n is the segment
// count of segment 0. Segments beyond n are left over from an earlier write
// of the part in more segments and are ignored. A part that is incomplete,
// for example while it is being written, is skipped: the hot tier still
// holds its samples until the write succeeds.
type versionReader struct {
	mint, maxt int64
	logger     *slog.Logger

	sets    [][]model.Sample // samples of the versions read so far
	pending []model.Sample   // samples of the current version

	cur     segmentKey // the last segment read
	started bool
	header  segmentHeader // of segment 0 of the current part
	next    uint32        // the next segment expected
	data    []byte        // of the current part
	broken  bool          // the current part cannot be used
}

// partAttrs returns the key of the part of s as log attributes, so that a
// part that is skipped can be found (the logger has the series).
func (s segmentKey) partAttrs() []any {
	return []any{"start", s.start, "fragment", s.fragment, "total", s.total, "digest", s.digest, "part", s.part}
}

func (r *versionReader) add(s segmentKey, value []byte) {
	newPart := !r.started || !s.sameVersion(r.cur) || s.part != r.cur.part
	if r.started && newPart {
		r.endPart()
		if !s.sameVersion(r.cur) {
			r.endVersion()
		}
	}
	if newPart {
		r.header, r.next, r.data, r.broken = segmentHeader{}, 0, nil, false
	}
	r.cur, r.started = s, true
	if r.broken || (r.next > 0 && r.next == r.header.segments) {
		return // a broken part, or a segment left over after a complete one
	}
	h, data, err := parseSegment(value)
	switch {
	case err != nil:
		r.broken = true
		r.logger.Error("skipping chunk with a malformed segment", append(s.partAttrs(), "segment", s.seg)...)
	case s.seg != r.next:
		// A segment is missing: the part is being written, or its write
		// failed half way and is retried until it succeeds.
		r.broken = true
		r.logger.Debug("skipping incomplete chunk", append(s.partAttrs(), "segment", s.seg)...)
	case s.seg > 0 && h != r.header:
		r.broken = true
		r.logger.Warn("skipping chunk with segments of different writes", append(s.partAttrs(), "segment", s.seg)...)
	default:
		if s.seg == 0 {
			r.header = h
		}
		r.data = append(r.data, data...)
		r.next++
	}
}

// endPart decodes the current part, if it is complete.
func (r *versionReader) endPart() {
	if r.broken {
		return
	}
	if r.next != r.header.segments {
		r.logger.Debug("skipping incomplete chunk", append(r.cur.partAttrs(),
			"segments", r.next, "want", r.header.segments)...)
		return
	}
	if xxhash.Sum64(r.data) != r.header.sum {
		r.logger.Warn("skipping chunk with segments of different writes", r.cur.partAttrs()...)
		return
	}
	samples, err := chunk.Decode(r.header.encoding, r.data)
	if err != nil {
		// See storage.ColdStore: skipped, with the key of the part so that
		// it can be inspected or deleted.
		r.logger.Error("skipping undecodable chunk", append(r.cur.partAttrs(), "err", err)...)
		return
	}
	r.pending = append(r.pending, samples...)
}

func (r *versionReader) endVersion() {
	if s := chunk.Filter(chunk.Normalize(r.pending), r.mint, r.maxt); len(s) > 0 {
		r.sets = append(r.sets, s)
	}
	r.pending = nil
}

func (r *versionReader) finish() []model.Sample {
	if r.started {
		r.endPart()
		r.endVersion()
	}
	return chunk.Merge(r.sets...)
}

// Ping checks connectivity.
func (c *ChunkStore) Ping(ctx context.Context) error { return c.db.Ping(ctx) }
