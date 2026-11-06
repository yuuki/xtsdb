// Package storage defines the interfaces between xtsdb services and the
// storage tiers, so that tiers can be backed by different key-value stores.
package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	Accepted int
	// AcceptedHistograms is how many of the accepted samples are native
	// histograms.
	AcceptedHistograms int
	TooOld             int
	ChunkFull          int
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
	Size     int // size of the raw part when loaded; used to detect later appends
	// CompactedSize is the size of the compacted part when loaded (zero if
	// the chunk has none).
	CompactedSize int
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

// CompactMargin is how long before its window closes a chunk is left alone
// by HotStore.Compact: the flusher compresses it soon anyway.
const CompactMargin = time.Minute

// CompactMinRecords is the number of raw float records (16 bytes each, or
// their size in histogram records) a chunk needs before HotStore.Compact
// compacts it. With fewer, creating the compacted part can take more memory
// than it frees, and the cost of a compaction is spread over few samples.
const CompactMinRecords = 12

// CompactResult reports what HotStore.Compact did.
type CompactResult struct {
	// Skipped means that the shard was compacted less than an interval
	// ago, by this caller or another one, and nothing was done.
	Skipped bool
	// Compacted is the number of chunks whose raw records were moved into
	// their compacted part.
	Compacted int
	// Changed counts chunks that changed between their read and their
	// compaction (another compaction, a closed window, a new incarnation).
	// They are compacted next time if they are still open.
	Changed int
	// Failed counts chunks that could not be decoded or encoded. They are
	// left as they are; flushing quarantines undecodable chunks.
	Failed int
	// Kept counts chunks left raw because compaction would not help: they
	// hold native histograms, too many records, or their compacted part
	// would not be smaller than what it replaces.
	Kept int
	// BytesBefore is the size of the raw records and compacted parts that
	// were replaced, and BytesAfter the size of the new compacted parts.
	BytesBefore, BytesAfter int64
}

// HotStore is the in-memory tier. Samples are appended to per-series time
// windows; once a window closes, flushers claim it, persist it and complete
// it. A claim is a lease: if the flusher dies, the chunk is claimed again
// after the lease expires, which gives at-least-once delivery.
//
// A chunk holds raw records, appended without reading the chunk, and a
// compacted part: compaction compresses the raw records of open chunks
// into it, so that they take less memory until they are flushed.
type HotStore interface {
	Append(ctx context.Context, batch []SeriesSamples) (AppendResult, error)
	// Compact moves the raw records of the open chunks of a shard into their
	// compacted part, at most once per interval per shard across all
	// callers. The samples a chunk holds do not change. Chunks whose window
	// is closed or closes within CompactMargin, and chunks with fewer than
	// CompactMinRecords raw records, are left alone.
	Compact(ctx context.Context, shard int, interval time.Duration) (CompactResult, error)
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

// ErrSeriesCollision means that a series ID is already used by a different
// label set. It is wrapped in a *SeriesCollisionError.
var ErrSeriesCollision = errors.New("series ID collision")

// SeriesCollisionError reports that two label sets hash to the same series
// ID. The series that recorded the ID first keeps it.
type SeriesCollisionError struct {
	ID       model.SeriesID
	Labels   labels.Labels // the series that was refused
	Existing labels.Labels // the series that owns the ID
}

func (e *SeriesCollisionError) Error() string {
	return fmt.Sprintf("series ID %s of %s is already used by %s", e.ID, e.Labels, e.Existing)
}

// Unwrap returns ErrSeriesCollision.
func (e *SeriesCollisionError) Unwrap() error { return ErrSeriesCollision }

// IndexSlack is how much longer index entries are kept than samples: the
// entries of a day expire IndexSlack after the samples of the end of that
// day, and series rows IndexSlack after their last write. Entries of days
// whose samples have already expired are not written.
const IndexSlack = 48 * time.Hour

// IndexWriter records which series and label values exist on a given day.
// All writes are idempotent.
type IndexWriter interface {
	// WriteSeries records a series and its postings for a day. If the ID
	// already belongs to a different label set, nothing is written and a
	// *SeriesCollisionError is returned.
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

// Index is the series index, written by ingesters and imports and read by
// queriers.
type Index interface {
	IndexWriter
	IndexReader
}
