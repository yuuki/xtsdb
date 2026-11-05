// Package flush moves closed chunks from the hot tier to the persistent
// tier.
//
// The hand-off is at-least-once: a chunk is deleted from the hot tier only
// after it has been written to the cold tier, and a chunk claimed by a
// flusher that dies is claimed again once its lease expires. Cold writes
// are idempotent, so a chunk written twice is stored once.
package flush

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/chunk"
	"github.com/yuuki/xtsdb/internal/storage"
)

// Options configures a Flusher.
type Options struct {
	PollInterval time.Duration
	BatchSize    int
	Concurrency  int
	// Lease bounds how long a batch may take; after it the chunks are
	// handed to another flusher anyway.
	Lease      time.Duration
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// Flusher persists closed hot chunks.
type Flusher struct {
	hot  storage.HotStore
	cold storage.ColdStore
	opts Options
	log  *slog.Logger

	chunks     *prometheus.CounterVec
	samples    prometheus.Counter
	bytes      prometheus.Counter
	batchTime  prometheus.Histogram
	pending    prometheus.Gauge
	inflight   prometheus.Gauge
	lagSeconds prometheus.Gauge
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
		lagSeconds: reg.NewGauge(prometheus.GaugeOpts{
			Name: "xtsdb_flusher_lag_seconds",
			Help: "How long the oldest closed chunk has been waiting to be flushed.",
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

	ticker := time.NewTicker(f.opts.PollInterval)
	defer ticker.Stop()
	f.log.Info("flusher started", "shards", f.hot.Shards(), "concurrency", f.opts.Concurrency)
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

// flushShard drains the closed chunks of a shard.
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
		f.flushBatch(ctx, refs)
		if len(refs) < f.opts.BatchSize {
			return
		}
	}
}

// FlushOnce flushes every closed chunk once. It is intended for tests and
// tools; production code uses Run.
func (f *Flusher) FlushOnce(ctx context.Context) {
	for shard := range f.hot.Shards() {
		f.flushShard(ctx, shard)
	}
}

func (f *Flusher) flushBatch(ctx context.Context, refs []storage.ChunkRef) {
	start := time.Now()
	// Let a started batch finish during shutdown, but never past its lease.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.opts.Lease)
	defer cancel()

	chunks, err := f.hot.Load(ctx, refs)
	if err != nil {
		f.log.Error("load failed", "chunks", len(refs), "err", err)
		f.chunks.WithLabelValues("error").Add(float64(len(refs)))
		return
	}
	var (
		mu   sync.Mutex
		done = make([]storage.HotChunk, 0, len(chunks))
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for _, c := range chunks {
		g.Go(func() error {
			if err := f.persist(gctx, c); err != nil {
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
	if len(done) == 0 {
		return
	}
	if err := f.hot.Complete(ctx, done); err != nil {
		// The chunks are persisted; they will be written again (idempotently)
		// after the lease expires.
		f.log.Error("complete failed", "chunks", len(done), "err", err)
		return
	}
	f.chunks.WithLabelValues("flushed").Add(float64(len(done)))
	f.batchTime.Observe(time.Since(start).Seconds())
}

func (f *Flusher) persist(ctx context.Context, c storage.HotChunk) error {
	if len(c.Samples) == 0 {
		return nil
	}
	data, err := chunk.EncodeXOR(c.Samples)
	if err != nil {
		return err
	}
	err = f.cold.WriteChunk(ctx, storage.ColdChunk{
		Series:   c.Ref.Series,
		Start:    c.Ref.Start,
		Fragment: c.Fragment,
		MaxT:     c.Samples[len(c.Samples)-1].T,
		Count:    len(c.Samples),
		Encoding: chunk.EncodingXOR,
		Data:     data,
	})
	if err != nil {
		return err
	}
	f.samples.Add(float64(len(c.Samples)))
	f.bytes.Add(float64(len(data)))
	return nil
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
