// Package flush moves closed chunks from the hot tier to the persistent
// tier, and compacts open chunks in the hot tier.
//
// The hand-off is at-least-once: a chunk is deleted from the hot tier only
// after it has been written to the cold tier, and a chunk claimed by a
// flusher that dies is claimed again once its lease expires. Cold writes
// are idempotent, so a chunk written twice is stored once.
//
// Compaction compresses the raw records of open chunks in place, so that
// they take less memory until they are flushed. It does not change the
// samples of a chunk, nor what is flushed.
package flush

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Options configures a Flusher.
type Options struct {
	PollInterval time.Duration
	BatchSize    int
	Concurrency  int
	// Lease bounds how long a batch may take; after it the chunks are
	// handed to another flusher anyway.
	Lease time.Duration
	// CompactionInterval is how often each shard is compacted, across all
	// flushers; zero disables compaction.
	CompactionInterval time.Duration
	Registerer         prometheus.Registerer
	Logger             *slog.Logger
}

// Flusher persists closed hot chunks.
type Flusher struct {
	hot  storage.HotStore
	cold storage.ColdStore
	opts Options
	log  *slog.Logger

	chunks      *prometheus.CounterVec
	samples     prometheus.Counter
	bytes       prometheus.Counter
	batchTime   prometheus.Histogram
	pending     prometheus.Gauge
	inflight    prometheus.Gauge
	quarantined prometheus.Gauge
	lagSeconds  prometheus.Gauge

	compactions      *prometheus.CounterVec
	compactInBytes   prometheus.Counter
	compactOutBytes  prometheus.Counter
	compactShardTime prometheus.Histogram
}

// New creates a Flusher.
func New(hot storage.HotStore, cold storage.ColdStore, opts Options) *Flusher {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	reg := promauto.With(opts.Registerer)
	return &Flusher{
		hot:  hot,
		cold: cold,
		opts: opts,
		log:  opts.Logger.With("component", "flusher"),
		chunks: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_flusher_chunks_total",
			Help: "Chunks processed by the flusher, by outcome.",
		}, []string{"outcome"}),
		samples: reg.NewCounter(prometheus.CounterOpts{
			Name: "xtsdb_flusher_samples_total",
			Help: "Samples written to the persistent tier.",
		}),
		bytes: reg.NewCounter(prometheus.CounterOpts{
			Name: "xtsdb_flusher_bytes_total",
			Help: "Compressed bytes written to the persistent tier.",
		}),
		batchTime: reg.NewHistogram(prometheus.HistogramOpts{
			Name:                        "xtsdb_flusher_batch_duration_seconds",
			Help:                        "Time to flush one batch of chunks.",
			Buckets:                     prometheus.DefBuckets,
			NativeHistogramBucketFactor: 1.1,
		}),
		pending: reg.NewGauge(prometheus.GaugeOpts{
			Name: "xtsdb_hot_pending_chunks",
			Help: "Chunks in the hot tier that are not claimed yet (open or closed).",
		}),
		inflight: reg.NewGauge(prometheus.GaugeOpts{
			Name: "xtsdb_hot_inflight_chunks",
			Help: "Chunks claimed by flushers and not completed yet.",
		}),
		quarantined: reg.NewGauge(prometheus.GaugeOpts{
			Name: "xtsdb_hot_quarantined_chunks",
			Help: "Undecodable chunks kept in quarantine in the hot tier (see `xtsdb quarantine`).",
		}),
		lagSeconds: reg.NewGauge(prometheus.GaugeOpts{
			Name: "xtsdb_flusher_lag_seconds",
			Help: "How long the oldest closed chunk (pending or in flight) has been waiting to be flushed.",
		}),
		compactions: reg.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_flusher_compaction_chunks_total",
			Help: "Open hot chunks processed by compaction, by outcome (compacted; kept: left raw because compaction would not help; changed: retried at the next compaction; failed: undecodable).",
		}, []string{"outcome"}),
		compactInBytes: reg.NewCounter(prometheus.CounterOpts{
			Name: "xtsdb_flusher_compaction_input_bytes_total",
			Help: "Bytes of raw records and earlier compacted parts replaced by compaction.",
		}),
		compactOutBytes: reg.NewCounter(prometheus.CounterOpts{
			Name: "xtsdb_flusher_compaction_output_bytes_total",
			Help: "Bytes of compacted parts written by compaction.",
		}),
		compactShardTime: reg.NewHistogram(prometheus.HistogramOpts{
			Name:                        "xtsdb_flusher_compaction_duration_seconds",
			Help:                        "Time to compact the open chunks of one shard.",
			Buckets:                     prometheus.DefBuckets,
			NativeHistogramBucketFactor: 1.1,
		}),
	}
}

// Run flushes until ctx is cancelled. Batches in progress are finished.
func (f *Flusher) Run(ctx context.Context) error {
	shards := make(chan int)
	var wg sync.WaitGroup
	for range f.opts.Concurrency {
		wg.Go(func() {
			for shard := range shards {
				f.flushShard(ctx, shard)
			}
		})
	}
	wg.Go(func() { f.reportBacklog(ctx) })
	if f.opts.CompactionInterval > 0 {
		wg.Go(func() { f.compactLoop(ctx) })
	}

	ticker := time.NewTicker(f.opts.PollInterval)
	defer ticker.Stop()
	f.log.Info("flusher started", "shards", f.hot.Shards(), "concurrency", f.opts.Concurrency,
		"compaction_interval", f.opts.CompactionInterval)
dispatch:
	for {
		// Visit shards in random order so that several flushers spread out.
		for _, shard := range rand.Perm(f.hot.Shards()) {
			select {
			case shards <- shard:
			case <-ctx.Done():
				break dispatch
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(shards)
	wg.Wait()
	f.log.Info("flusher stopped")
	return nil
}

// flushShard drains the closed chunks of a shard. It stops when the shard
// is empty or when a batch makes no progress, so that a worker never spins
// on chunks it cannot persist.
func (f *Flusher) flushShard(ctx context.Context, shard int) {
	for ctx.Err() == nil {
		refs, err := f.hot.Claim(ctx, shard, f.opts.BatchSize)
		if err != nil {
			if ctx.Err() == nil {
				f.log.Error("claim failed", "shard", shard, "err", err)
			}
			return
		}
		if len(refs) == 0 {
			return
		}
		if done := f.flushBatch(ctx, refs); done == 0 || len(refs) < f.opts.BatchSize {
			return
		}
	}
}

// compactLoop offers every shard for compaction regularly. The hot tier
// compacts a shard at most once per interval across all flushers, so the
// shards are offered ten times per interval, in random order, to compact
// each of them soon after its interval has passed.
func (f *Flusher) compactLoop(ctx context.Context) {
	tick := max(f.opts.CompactionInterval/10, 10*time.Millisecond)
	shards := make(chan int)
	var wg sync.WaitGroup
	for range f.opts.Concurrency {
		wg.Go(func() {
			for shard := range shards {
				f.compactShard(ctx, shard)
			}
		})
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
offer:
	for {
		for _, shard := range rand.Perm(f.hot.Shards()) {
			select {
			case shards <- shard:
			case <-ctx.Done():
				break offer
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			break offer
		}
	}
	close(shards)
	wg.Wait()
}

// compactShard compacts the open chunks of a shard, unless it was compacted
// less than an interval ago.
func (f *Flusher) compactShard(ctx context.Context, shard int) {
	start := time.Now()
	res, err := f.hot.Compact(ctx, shard, f.opts.CompactionInterval)
	if err != nil && ctx.Err() == nil {
		f.log.Warn("compaction failed; the shard is compacted again after the interval", "shard", shard, "err", err)
	}
	if res.Skipped {
		return
	}
	f.compactions.WithLabelValues("compacted").Add(float64(res.Compacted))
	f.compactions.WithLabelValues("kept").Add(float64(res.Kept))
	f.compactions.WithLabelValues("changed").Add(float64(res.Changed))
	f.compactions.WithLabelValues("failed").Add(float64(res.Failed))
	f.compactInBytes.Add(float64(res.BytesBefore))
	f.compactOutBytes.Add(float64(res.BytesAfter))
	if err == nil {
		f.compactShardTime.Observe(time.Since(start).Seconds())
	}
}

// CompactOnce offers every shard for compaction once. It is intended for
// tests and tools; production code uses Run.
func (f *Flusher) CompactOnce(ctx context.Context) {
	for shard := range f.hot.Shards() {
		f.compactShard(ctx, shard)
	}
}

// FlushOnce flushes every closed chunk once. It is intended for tests and
// tools; production code uses Run.
func (f *Flusher) FlushOnce(ctx context.Context) {
	for shard := range f.hot.Shards() {
		f.flushShard(ctx, shard)
	}
}

// completeTimeout bounds the final Redis call of a batch. It is independent
// of the lease: chunks that were persisted must be completed even if the
// batch took longer than the lease.
const completeTimeout = 30 * time.Second

// flushBatch persists and completes claimed chunks and returns how many
// were finished.
func (f *Flusher) flushBatch(ctx context.Context, refs []storage.ChunkRef) int {
	start := time.Now()
	// A started batch is finished during shutdown; leases protect the rest.
	work := context.WithoutCancel(ctx)
	leaseEnd := start.Add(f.opts.Lease)

	loadCtx, cancel := context.WithTimeout(work, f.opts.Lease)
	loaded, err := f.hot.Load(loadCtx, refs)
	cancel()
	if err != nil {
		f.log.Error("load failed", "chunks", len(refs), "err", err)
		f.chunks.WithLabelValues("error").Add(float64(len(refs)))
		return 0
	}
	chunks, missing := loaded.Chunks, loaded.Missing
	finished := loaded.Quarantined
	if loaded.Quarantined > 0 {
		f.chunks.WithLabelValues("quarantined").Add(float64(loaded.Quarantined))
	}
	if len(missing) > 0 {
		rctx, cancel := context.WithTimeout(work, completeTimeout)
		if err := f.hot.Release(rctx, missing); err != nil {
			f.log.Error("release failed", "chunks", len(missing), "err", err)
		} else {
			finished += len(missing)
		}
		cancel()
	}

	var (
		mu      sync.Mutex
		done    = make([]storage.HotChunk, 0, len(chunks))
		skipped int
	)
	g := new(errgroup.Group)
	g.SetLimit(16)
	for _, c := range chunks {
		// Once the lease has expired other flushers may take these chunks
		// over; do not start more work on them.
		if time.Now().After(leaseEnd) {
			skipped++
			continue
		}
		g.Go(func() error {
			pctx, cancel := context.WithTimeout(work, f.opts.Lease)
			defer cancel()
			if err := f.persist(pctx, c); err != nil {
				f.log.Warn("persisting chunk failed; it will be retried after the lease expires",
					"series", c.Ref.Series, "start", c.Ref.Start, "err", err)
				f.chunks.WithLabelValues("error").Inc()
				return nil
			}
			mu.Lock()
			done = append(done, c)
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
	if skipped > 0 {
		f.log.Warn("batch exceeded its lease; remaining chunks are left for re-claim", "skipped", skipped)
	}
	if len(done) == 0 {
		return finished
	}
	cctx, cancel := context.WithTimeout(work, completeTimeout)
	defer cancel()
	if err := f.hot.Complete(cctx, done); err != nil {
		// The chunks are persisted; they will be written again (idempotently)
		// after the lease expires.
		f.log.Error("complete failed", "chunks", len(done), "err", err)
		return finished
	}
	f.chunks.WithLabelValues("flushed").Add(float64(len(done)))
	f.batchTime.Observe(time.Since(start).Seconds())
	return finished + len(done)
}

// samplesPerPart and bytesPerPart bound the size of one cold-tier row, well
// below the Cassandra mutation size limit even for incompressible values and
// large native histograms.
const (
	samplesPerPart = 100_000
	bytesPerPart   = 4 << 20
)

func (f *Flusher) persist(ctx context.Context, c storage.HotChunk) error {
	n, err := WriteCold(ctx, f.cold, c.Ref.Series, c.Ref.Start, c.Fragment, c.Records, c.Samples)
	f.bytes.Add(float64(n))
	if err != nil {
		return err
	}
	f.samples.Add(float64(len(c.Samples)))
	return nil
}

// WriteCold writes one version of a chunk window to the cold tier, in parts
// of at most 100,000 samples and 4 MiB, and returns the number of compressed
// bytes written. samples must be sorted and de-duplicated; total is the
// number of records of the version (see storage.ColdChunk). Writing the same
// version again writes the same rows: parts are cut deterministically.
func WriteCold(ctx context.Context, cold storage.ColdStore, series model.SeriesID, start, fragment int64,
	total int, samples []model.Sample) (int, error) {
	if len(samples) == 0 {
		return 0, nil
	}
	digest := xxhash.Sum64(chunk.AppendRaw(nil, samples))
	written := 0
	for part, i := 0, 0; i < len(samples); part++ {
		n := min(samplesPerPart, len(samples)-i)
		enc, data, err := chunk.Encode(samples[i : i+n])
		for err == nil && len(data) > bytesPerPart && n > 1 {
			n /= 2
			enc, data, err = chunk.Encode(samples[i : i+n])
		}
		if err != nil {
			return written, err
		}
		p := samples[i : i+n]
		i += n
		err = cold.WriteChunk(ctx, storage.ColdChunk{
			Series:   series,
			Start:    start,
			Fragment: fragment,
			Total:    total,
			Digest:   digest,
			Part:     part,
			MaxT:     p[len(p)-1].T,
			Count:    len(p),
			Encoding: enc,
			Data:     data,
		})
		if err != nil {
			return written, err
		}
		written += len(data)
	}
	return written, nil
}

// reportBacklog periodically exports how much work is waiting.
func (f *Flusher) reportBacklog(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		b, err := f.hot.Backlog(ctx)
		if err == nil {
			f.pending.Set(float64(b.PendingChunks))
			f.inflight.Set(float64(b.InflightChunks))
			f.quarantined.Set(float64(b.QuarantinedChunks))
			lag := 0.0
			if b.OldestDue > 0 {
				lag = max(0, float64(time.Now().UnixMilli()-b.OldestDue)/1000)
			}
			f.lagSeconds.Set(lag)
		} else if ctx.Err() == nil {
			f.log.Warn("backlog check failed", "err", err)
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}
