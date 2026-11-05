// Package storage defines the interfaces between xtsdb services and the
// storage tiers, so that tiers can be backed by different key-value stores.
package storage

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
)

// SeriesSamples is a batch of samples for one series.
type SeriesSamples struct {
	ID      model.SeriesID
	Samples []model.Sample
}

// AppendResult reports how many samples were stored and how many were
// rejected because their chunk window had already closed.
type AppendResult struct {
	Accepted int
	TooOld   int
}

// ChunkRef identifies a chunk of the hot tier.
type ChunkRef struct {
	Shard  int
	Series model.SeriesID
	Start  int64 // window start in milliseconds
}

// HotChunk is a closed hot chunk loaded for flushing.
type HotChunk struct {
	Ref      ChunkRef
	Fragment int64 // identifies this incarnation of the chunk
	Samples  []model.Sample
}

// Backlog summarizes the chunks held by the hot tier.
type Backlog struct {
	PendingChunks  int64 // chunks not yet claimed, open or closed
	InflightChunks int64 // chunks claimed by a flusher
	OldestDue      int64 // earliest due time of a pending chunk in ms; 0 if none
}

// HotStore is the in-memory tier. Samples are appended to per-series time
// windows; once a window closes, flushers claim it, persist it and complete
// it. A claim is a lease: if the flusher dies, the chunk is claimed again
// after the lease expires, which gives at-least-once delivery.
type HotStore interface {
	Append(ctx context.Context, batch []SeriesSamples) (AppendResult, error)
	Claim(ctx context.Context, shard, limit int) ([]ChunkRef, error)
	// Load returns the chunks that still exist. Chunks completed by another
	// flusher in the meantime are omitted.
	Load(ctx context.Context, refs []ChunkRef) ([]HotChunk, error)
	Complete(ctx context.Context, chunks []HotChunk) error
	Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error)
	Backlog(ctx context.Context) (Backlog, error)
	Shards() int
	Ping(ctx context.Context) error
	Close() error
}

// ColdChunk is a compressed chunk in the persistent tier.
type ColdChunk struct {
	Series   model.SeriesID
	Start    int64
	Fragment int64
	MaxT     int64
	Count    int
	Encoding chunk.Encoding
	Data     []byte
}

// ColdStore is the persistent tier. Writes must be idempotent: writing the
// same (Series, Start, Fragment) twice stores it once.
type ColdStore interface {
	WriteChunk(ctx context.Context, c ColdChunk) error
	Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error)
	Ping(ctx context.Context) error
}

// IndexWriter records which series and label values exist on a given day.
// All writes are idempotent.
type IndexWriter interface {
	WriteSeries(ctx context.Context, day int32, id model.SeriesID, lset labels.Labels) error
	WriteLabelValue(ctx context.Context, day int32, name, value string) error
	WriteLabelName(ctx context.Context, day int32, name string) error
}

// IndexReader answers the lookups needed to resolve label matchers.
type IndexReader interface {
	Postings(ctx context.Context, day int32, name, value string) ([]model.SeriesID, error)
	LabelValues(ctx context.Context, day int32, name string) ([]string, error)
	LabelNames(ctx context.Context, day int32) ([]string, error)
	SeriesLabels(ctx context.Context, ids []model.SeriesID) (map[model.SeriesID]labels.Labels, error)
}
