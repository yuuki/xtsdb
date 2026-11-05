// Package storagetest provides in-memory implementations of the storage
// interfaces with the same semantics as the real backends, for tests.
package storagetest

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

type hotChunk struct {
	fragment int64
	raw      []model.Sample
	due      int64
}

// Hot is an in-memory storage.HotStore.
type Hot struct {
	mu       sync.Mutex
	Layout   model.Layout
	Lease    time.Duration
	Now      func() time.Time
	MaxChunk int // maximum samples per chunk; 0 means unlimited
	chunks   map[storage.ChunkRef]*hotChunk
	inflight map[storage.ChunkRef]int64
	seq      int64
}

func size(c *hotChunk) int { return 16 + 16*len(c.raw) }

// NewHot creates a Hot store.
func NewHot(layout model.Layout, lease time.Duration) *Hot {
	return &Hot{
		Layout:   layout,
		Lease:    lease,
		Now:      time.Now,
		chunks:   make(map[storage.ChunkRef]*hotChunk),
		inflight: make(map[storage.ChunkRef]int64),
	}
}

var _ storage.HotStore = (*Hot)(nil)

func (h *Hot) now() int64 { return h.Now().UnixMilli() }

// Append implements storage.HotStore.
func (h *Hot) Append(_ context.Context, batch []storage.SeriesSamples) (storage.AppendResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var res storage.AppendResult
	now := h.now()
	for _, ss := range batch {
		for _, s := range ss.Samples {
			start := h.Layout.WindowStart(ss.ID, s.T)
			ref := storage.ChunkRef{Shard: h.Layout.Shard(ss.ID), Series: ss.ID, Start: start}
			due := h.Layout.Due(start)
			if _, busy := h.inflight[ref]; now >= due || busy {
				res.TooOld++
				continue
			}
			c := h.chunks[ref]
			if c != nil && h.MaxChunk > 0 && len(c.raw) >= h.MaxChunk {
				res.ChunkFull++
				continue
			}
			if c == nil {
				h.seq++
				c = &hotChunk{fragment: now*1000 + h.seq, due: due}
				h.chunks[ref] = c
			}
			c.raw = append(c.raw, s)
			res.Accepted++
		}
	}
	return res, nil
}

// Claim implements storage.HotStore.
func (h *Hot) Claim(_ context.Context, shard, limit int) ([]storage.ChunkRef, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var out []storage.ChunkRef
	for ref, deadline := range h.inflight {
		if len(out) < limit && ref.Shard == shard && deadline <= now {
			h.inflight[ref] = now + h.Lease.Milliseconds()
			out = append(out, ref)
		}
	}
	for ref, c := range h.chunks {
		if _, busy := h.inflight[ref]; len(out) < limit && !busy && ref.Shard == shard && c.due <= now {
			h.inflight[ref] = now + h.Lease.Milliseconds()
			out = append(out, ref)
		}
	}
	return out, nil
}

// Load implements storage.HotStore.
func (h *Hot) Load(_ context.Context, refs []storage.ChunkRef) (storage.LoadResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var res storage.LoadResult
	for _, ref := range refs {
		c := h.chunks[ref]
		if c == nil {
			res.Missing = append(res.Missing, ref)
			continue
		}
		res.Chunks = append(res.Chunks, storage.HotChunk{
			Ref: ref, Fragment: c.fragment, Size: size(c), Records: len(c.raw),
			Samples: chunk.Normalize(slices.Clone(c.raw)),
		})
	}
	return res, nil
}

// Complete implements storage.HotStore.
func (h *Hot) Complete(_ context.Context, chunks []storage.HotChunk) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, hc := range chunks {
		if c := h.chunks[hc.Ref]; c != nil && c.fragment == hc.Fragment && size(c) == hc.Size {
			delete(h.chunks, hc.Ref)
			delete(h.inflight, hc.Ref)
		}
	}
	return nil
}

// Release implements storage.HotStore.
func (h *Hot) Release(_ context.Context, refs []storage.ChunkRef) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ref := range refs {
		if h.chunks[ref] == nil {
			delete(h.inflight, ref)
		}
	}
	return nil
}

// Inflight returns the number of claimed chunks.
func (h *Hot) Inflight() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.inflight)
}

// Delete removes a chunk without completing it, to simulate data loss.
func (h *Hot) Delete(ref storage.ChunkRef) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.chunks, ref)
}

// Read implements storage.HotStore.
func (h *Hot) Read(_ context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[model.SeriesID][]model.Sample)
	for _, id := range ids {
		var all []model.Sample
		for _, w := range h.Layout.Windows(id, mint, maxt) {
			ref := storage.ChunkRef{Shard: h.Layout.Shard(id), Series: id, Start: w}
			if c := h.chunks[ref]; c != nil {
				all = append(all, c.raw...)
			}
		}
		if s := chunk.Filter(chunk.Normalize(all), mint, maxt); len(s) > 0 {
			out[id] = s
		}
	}
	return out, nil
}

// Len returns the number of chunks held.
func (h *Hot) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.chunks)
}

// Backlog implements storage.HotStore.
func (h *Hot) Backlog(context.Context) (storage.Backlog, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b storage.Backlog
	for ref, c := range h.chunks {
		if _, busy := h.inflight[ref]; busy {
			b.InflightChunks++
		} else {
			b.PendingChunks++
		}
		if b.OldestDue == 0 || c.due < b.OldestDue {
			b.OldestDue = c.due
		}
	}
	if b.OldestDue != 0 {
		b.OldestStart = b.OldestDue - h.Layout.ChunkRange - h.Layout.Grace
	}
	return b, nil
}

// Shards implements storage.HotStore.
func (h *Hot) Shards() int { return h.Layout.Shards }

// Ping implements storage.HotStore.
func (h *Hot) Ping(context.Context) error { return nil }

// Close implements storage.HotStore.
func (h *Hot) Close() error { return nil }

type coldKey struct {
	series   model.SeriesID
	start    int64
	fragment int64
	total    int
	digest   uint64
	part     int
}

// Cold is an in-memory storage.ColdStore.
type Cold struct {
	mu         sync.Mutex
	ChunkRange int64
	chunks     map[coldKey]storage.ColdChunk
	// FailWrites makes WriteChunk fail, to simulate an outage.
	FailWrites error
}

// NewCold creates a Cold store.
func NewCold(chunkRange int64) *Cold {
	return &Cold{ChunkRange: chunkRange, chunks: make(map[coldKey]storage.ColdChunk)}
}

var _ storage.ColdStore = (*Cold)(nil)

// WriteChunk implements storage.ColdStore.
func (c *Cold) WriteChunk(_ context.Context, ch storage.ColdChunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.FailWrites != nil {
		return c.FailWrites
	}
	c.chunks[coldKey{ch.Series, ch.Start, ch.Fragment, ch.Total, ch.Digest, ch.Part}] = ch
	return nil
}

// Len returns the number of chunks stored.
func (c *Cold) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.chunks)
}

// Read implements storage.ColdStore.
func (c *Cold) Read(_ context.Context, ids []model.SeriesID, mint, maxt int64) (map[model.SeriesID][]model.Sample, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]coldKey, 0, len(c.chunks))
	for k := range c.chunks {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b coldKey) int {
		// The clustering order of the Cassandra table, which stores the
		// digest as a signed bigint.
		return cmp.Or(cmp.Compare(a.start, b.start), cmp.Compare(a.fragment, b.fragment),
			cmp.Compare(a.total, b.total), cmp.Compare(int64(a.digest), int64(b.digest)), cmp.Compare(a.part, b.part))
	})
	out := make(map[model.SeriesID][]model.Sample)
	for _, id := range ids {
		// Concatenate the parts of each version, then merge versions in
		// order so that later and more complete versions win.
		var (
			sets    [][]model.Sample
			pending []model.Sample
			cur     *coldKey
		)
		for _, k := range keys {
			if k.series != id || k.start > maxt || k.start+c.ChunkRange <= mint {
				continue
			}
			if cur != nil && (cur.start != k.start || cur.fragment != k.fragment || cur.total != k.total || cur.digest != k.digest) {
				sets = append(sets, chunk.Filter(chunk.Normalize(pending), mint, maxt))
				pending = nil
			}
			ch := c.chunks[k]
			s, err := chunk.Decode(ch.Encoding, ch.Data)
			if err != nil {
				return nil, err
			}
			pending = append(pending, s...)
			cur = &k
		}
		if cur != nil {
			sets = append(sets, chunk.Filter(chunk.Normalize(pending), mint, maxt))
		}
		if s := chunk.Merge(sets...); len(s) > 0 {
			out[id] = s
		}
	}
	return out, nil
}

// Ping implements storage.ColdStore.
func (c *Cold) Ping(context.Context) error { return nil }

type posting struct {
	day         int32
	name, value string
}

// Index is an in-memory storage.IndexWriter and storage.IndexReader.
type Index struct {
	mu       sync.Mutex
	series   map[model.SeriesID]labels.Labels
	postings map[posting]map[model.SeriesID]struct{}
	values   map[posting]struct{}
	names    map[posting]struct{}
	// Writes counts index writes.
	Writes int
	// FailWrites makes writes fail, to simulate an outage.
	FailWrites error
}

// NewIndex creates an Index.
func NewIndex() *Index {
	return &Index{
		series:   make(map[model.SeriesID]labels.Labels),
		postings: make(map[posting]map[model.SeriesID]struct{}),
		values:   make(map[posting]struct{}),
		names:    make(map[posting]struct{}),
	}
}

var (
	_ storage.IndexWriter = (*Index)(nil)
	_ storage.IndexReader = (*Index)(nil)
)

// WriteSeries implements storage.IndexWriter.
func (x *Index) WriteSeries(_ context.Context, day int32, id model.SeriesID, lset labels.Labels) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.FailWrites != nil {
		return x.FailWrites
	}
	x.Writes++
	x.series[id] = lset
	lset.Range(func(l labels.Label) {
		p := posting{day, l.Name, l.Value}
		if x.postings[p] == nil {
			x.postings[p] = make(map[model.SeriesID]struct{})
		}
		x.postings[p][id] = struct{}{}
	})
	return nil
}

// WriteLabelValue implements storage.IndexWriter.
func (x *Index) WriteLabelValue(_ context.Context, day int32, name, value string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.FailWrites != nil {
		return x.FailWrites
	}
	x.Writes++
	x.values[posting{day, name, value}] = struct{}{}
	return nil
}

// WriteLabelName implements storage.IndexWriter.
func (x *Index) WriteLabelName(_ context.Context, day int32, name string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.FailWrites != nil {
		return x.FailWrites
	}
	x.Writes++
	x.names[posting{day: day, name: name}] = struct{}{}
	return nil
}

// Postings implements storage.IndexReader.
func (x *Index) Postings(_ context.Context, day int32, name, value string) ([]model.SeriesID, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	var out []model.SeriesID
	for id := range x.postings[posting{day, name, value}] {
		out = append(out, id)
	}
	return out, nil
}

// LabelValues implements storage.IndexReader.
func (x *Index) LabelValues(_ context.Context, day int32, name string) ([]string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	var out []string
	for p := range x.values {
		if p.day == day && p.name == name {
			out = append(out, p.value)
		}
	}
	slices.Sort(out)
	return out, nil
}

// LabelNames implements storage.IndexReader.
func (x *Index) LabelNames(_ context.Context, day int32) ([]string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	var out []string
	for p := range x.names {
		if p.day == day {
			out = append(out, p.name)
		}
	}
	slices.Sort(out)
	return out, nil
}

// SeriesLabels implements storage.IndexReader.
func (x *Index) SeriesLabels(_ context.Context, ids []model.SeriesID) (map[model.SeriesID]labels.Labels, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	out := make(map[model.SeriesID]labels.Labels)
	for _, id := range ids {
		if l, ok := x.series[id]; ok {
			out[id] = l
		}
	}
	return out, nil
}
