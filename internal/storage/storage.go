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
// rejected because their chunk window had already closed or their chunk
// reached the maximum size.
type AppendResult struct {
	Accepted  int
	TooOld    int
	ChunkFull int
}

// ChunkRef identifies a chunk of the hot tier.
type ChunkRef struct {
	Shard  int
	Series model.SeriesID
	Start  int64 // window start in milliseconds
}

// HotChunk is a closed hot chunk loaded for flushing.
type HotChunk struct {
	Ref ChunkRef
	// Fragment identifies this incarnation of the window. Later
	// incarnations have larger fragments.
	Fragment int64
	Size     int // encoded size when loaded; used to detect later appends
	// Records is the number of samples appended to this incarnation,
	// including those that were overwritten by a later sample with the same
	// timestamp. An incarnation only grows, so a larger count means a later
	// state of it.
	Records int
	Samples []model.Sample // sorted and de-duplicated
}

// LoadResult is the outcome of HotStore.Load.
type LoadResult struct {
	// Chunks are the claimed chunks that still exist and can be persisted.
	Chunks []HotChunk
	// Missing are the claimed chunks that no longer exist (completed by
	// another flusher in the meantime). Their in-flight entries must be
	// released.
	Missing []ChunkRef
	// Quarantined is the number of undecodable chunks that were moved out of
	// the way. They are kept for inspection and are no longer in flight.
	Quarantined int
}

// Backlog summarizes the chunks held by the hot tier.
type Backlog struct {
	PendingChunks     int64 // chunks not yet claimed, open or closed
	InflightChunks    int64 // chunks claimed by a flusher
	QuarantinedChunks int64 // undecodable chunks kept for inspection
	OldestDue         int64 // earliest due time of any chunk in ms; 0 if none
	OldestStart       int64 // window start of that chunk in ms; 0 if none
}

// HotStore is the in-memory tier. Samples are appended to per-series time
// windows; once a window closes, flushers claim it, persist it and complete
// it. A claim is a lease: if the flusher dies, the chunk is claimed again
// after the lease expires, which gives at-least-once delivery.
type HotStore interface {
	Append(ctx context.Context, batch []SeriesSamples) (AppendResult, error)
	Claim(ctx context.Context, shard, limit int) ([]ChunkRef, error)
	// Load reads claimed chunks. Undecodable chunks are quarantined: moved
	// out of the way, unless they changed since they were read, so that
	// they do not block flushing.
	Load(ctx context.Context, refs []ChunkRef) (LoadResult, error)
	// Complete deletes persisted chunks. A chunk that changed since it was
	// loaded is kept and re-processed after its lease expires.
	Complete(ctx context.Context, chunks []HotChunk) error
	// Release drops the in-flight entries of chunks that no longer exist.
	Release(ctx context.Context, refs []ChunkRef) error
	Read(ctx context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error)
	Backlog(ctx context.Context) (Backlog, error)
	Shards() int
	Ping(ctx context.Context) error
	Close() error
}

// ColdChunk is one part of a compressed chunk in the persistent tier.
//
// A hot chunk is persisted as one or more parts. (Fragment, Total, Digest)
// identify the version of the chunk that was persisted: if the same window
// is persisted again with different content (for example after Redis was
// restored from an older snapshot), it is stored as a separate version
// instead of overwriting a more complete one.
//
// Reads merge all versions of a window. Where versions disagree on the
// value at a timestamp, the version that is greatest in (Fragment, Total,
// Digest) order wins: the latest incarnation, then the latest state of an
// incarnation (the most records appended). Versions that are equal in
// (Fragment, Total) can only come from diverging histories of one
// incarnation, after a restore or a failover; which one is newer cannot be
// known, and the digest decides, deterministically but arbitrarily.
type ColdChunk struct {
	Series   model.SeriesID
	Start    int64
	Fragment int64
	// Total is the number of samples appended to the incarnation in the hot
	// tier (HotChunk.Records), including overwritten ones.
	Total    int
	Digest   uint64 // hash of the whole chunk's samples
	Part     int
	MaxT     int64
	Count    int // samples in this part
	Encoding chunk.Encoding
	Data     []byte
}

// ColdStore is the persistent tier. Writes must be idempotent: writing the
// same (Series, Start, Fragment, Total, Digest, Part) twice stores it once.
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
