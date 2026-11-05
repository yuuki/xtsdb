// Package app wires the xtsdb components together for each role.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/yuuki/xtsdb/internal/config"
	"github.com/yuuki/xtsdb/internal/flush"
	"github.com/yuuki/xtsdb/internal/hot"
	"github.com/yuuki/xtsdb/internal/ingest"
	"github.com/yuuki/xtsdb/internal/model"
	"github.com/yuuki/xtsdb/internal/query"
	"github.com/yuuki/xtsdb/internal/server"
	"github.com/yuuki/xtsdb/internal/version"
)

// Target selects the roles a process runs.
type Target string

// Supported targets.
const (
	TargetAll      Target = "all"
	TargetIngester Target = "ingester"
	TargetFlusher  Target = "flusher"
	TargetQuerier  Target = "querier"
)

func (t Target) runs(role Target) bool { return t == TargetAll || t == role }

// ParseTarget validates a target name.
func ParseTarget(s string) (Target, error) {
	switch t := Target(s); t {
	case TargetAll, TargetIngester, TargetFlusher, TargetQuerier:
		return t, nil
	}
	return "", fmt.Errorf("unknown target %q", s)
}

// App is a running xtsdb process.
type App struct {
	cfg    *config.Config
	target Target
	logger *slog.Logger

	Registry *prometheus.Registry
	Server   *server.Server
	Graphite *ingest.GraphiteServer
	Flusher  *flush.Flusher
	Hot      *hot.Store

	// Persistent is the persistent tier and the index.
	Persistent *Persistent

	redis redis.UniversalClient
}

// New connects to the storage backends and builds the components of the
// target. The HTTP listener is opened so that its address is known.
func New(ctx context.Context, target Target, cfg *config.Config, logger *slog.Logger) (_ *App, err error) {
	a := &App{cfg: cfg, target: target, logger: logger, Registry: prometheus.NewRegistry()}
	defer func() {
		if err != nil {
			a.close()
		}
	}()
	a.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo(),
	)

	a.Hot, a.redis, err = OpenHot(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	a.Persistent, err = OpenPersistent(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}
	retention := cfg.Storage.Retention.D()
	cold, index := a.Persistent.Cold, a.Persistent.Index

	httpTLS, err := cfg.Server.TLS.Build()
	if err != nil {
		return nil, fmt.Errorf("server.tls: %w", err)
	}
	authFailures := server.AuthFailures(a.Registry)
	readAuth := server.NewAuthenticator("read", cfg.Server.Auth.Read, authFailures)
	writeAuth := server.NewAuthenticator("write", cfg.Server.Auth.Write, authFailures)
	a.Server = server.New(cfg.Server.HTTPListenAddress, a.Registry, server.Options{
		EnablePprof: cfg.Server.EnablePprof,
		TLS:         httpTLS,
		Ops:         readAuth.Wrap,
	}, logger.With("component", "http"))
	a.Server.AddCheck("redis", a.Hot.Ping)
	a.Server.AddCheck(a.Persistent.Backend, cold.Ping)
	a.Server.Mux.Handle("/", readAuth.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintf(w, "xtsdb %s (%s)\n", version.Version, target)
	})))

	if target.runs(TargetIngester) {
		w, err := ingest.NewWriter(a.Hot, index, ingest.WriterOptions{
			MaxSampleFuture: cfg.Ingester.MaxSampleFuture.D(),
			// Windows close chunk_range + grace after they start; allow a
			// minute of clock skew between ingesters and Redis.
			MaxSampleAge:          cfg.Storage.ChunkRange.D() + cfg.Storage.GracePeriod.D() + time.Minute,
			IndexCacheSize:        cfg.Ingester.IndexCacheSize,
			IndexWriteConcurrency: cfg.Ingester.IndexWriteConcurrency,
			Registerer:            a.Registry,
			Logger:                logger,
		})
		if err != nil {
			return nil, err
		}
		a.Server.Mux.Handle("POST /api/v1/write",
			a.Server.Instrument("write", writeAuth.Wrap(ingest.RemoteWriteHandler(w, ingest.RemoteWriteOptions{
				MaxBodySize:         cfg.Ingester.MaxRequestBodySize,
				MaxDecompressedSize: cfg.Ingester.MaxDecompressedSize,
				MaxInflight:         cfg.Ingester.MaxInflightRequests,
				Limits: ingest.Limits{
					MaxSeries:  cfg.Ingester.MaxSeriesPerRequest,
					MaxSamples: cfg.Ingester.MaxSamplesPerRequest,
				},
			}, logger))))
		if cfg.Ingester.GraphiteListenAddress != "" {
			a.Graphite = ingest.NewGraphiteServer(cfg.Ingester.GraphiteListenAddress, cfg.Ingester.GraphiteMaxConnections,
				w, a.Registry, logger)
			if a.Graphite.TLS, err = cfg.Ingester.GraphiteTLS.Build(); err != nil {
				return nil, fmt.Errorf("ingester.graphite_tls: %w", err)
			}
		}
	}

	if target.runs(TargetFlusher) {
		a.Flusher = flush.New(a.Hot, cold, flush.Options{
			PollInterval: cfg.Flusher.PollInterval.D(),
			BatchSize:    cfg.Flusher.BatchSize,
			Concurrency:  cfg.Flusher.Concurrency,
			Lease:        cfg.Flusher.LeaseDuration.D(),
			Registerer:   a.Registry,
			Logger:       logger,
		})
	}

	if target.runs(TargetQuerier) {
		q, err := query.NewQueryable(a.Hot, cold, index, query.Options{
			HotLookback:       cfg.Querier.HotLookback.D(),
			Retention:         retention,
			MaxQueryLookback:  cfg.Querier.MaxQueryLookback.D(),
			MaxSeriesPerQuery: cfg.Querier.MaxSeriesPerQuery,
			SeriesCacheSize:   cfg.Querier.SeriesCacheSize,
			ReadConcurrency:   cfg.Querier.ReadConcurrency,
		})
		if err != nil {
			return nil, err
		}
		api := query.NewAPI(q, query.APIOptions{
			LookbackDelta:        cfg.Querier.LookbackDelta.D(),
			Timeout:              cfg.Querier.Timeout.D(),
			MaxSamples:           cfg.Querier.MaxSamples,
			MaxConcurrentQueries: cfg.Querier.MaxConcurrent,
			Registerer:           a.Registry,
			Logger:               logger,
			BuildInfo:            version.Info(),
		})
		api.Register(a.Server.Mux, func(h http.Handler) http.Handler {
			return gzhttp.GzipHandler(a.Server.Instrument("query", readAuth.Wrap(h)))
		})
	}

	if err := a.Server.Listen(); err != nil {
		return nil, err
	}
	if a.Graphite != nil {
		if err := a.Graphite.Start(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Layout returns the storage layout of a configuration.
func Layout(cfg *config.Config) model.Layout {
	return model.Layout{
		Shards:     cfg.Storage.Shards,
		ChunkRange: cfg.Storage.ChunkRange.D().Milliseconds(),
		Grace:      cfg.Storage.GracePeriod.D().Milliseconds(),
	}
}

// OpenHot connects to Redis and opens the hot tier. The caller closes the
// returned client.
func OpenHot(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*hot.Store, redis.UniversalClient, error) {
	hot.SetLogger(logger)
	client, err := hot.NewClient(cfg.Redis)
	if err != nil {
		return nil, nil, err
	}
	s, err := hot.New(ctx, client, hot.Options{
		Prefix:        cfg.Storage.KeyPrefix,
		Layout:        Layout(cfg),
		Lease:         cfg.Flusher.LeaseDuration.D(),
		QuarantineTTL: cfg.Flusher.QuarantineTTL.D(),
		Logger:        logger.With("component", "redis"),
	})
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return s, client, nil
}

// Run serves until ctx is cancelled, then shuts down gracefully and
// releases all resources.
func (a *App) Run(ctx context.Context) error {
	defer a.close()
	timeout := a.cfg.Server.GracefulShutdownTimeout.D()
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return a.Server.Serve(gctx, timeout) })
	if a.Flusher != nil {
		g.Go(func() error { return a.Flusher.Run(gctx) })
	}
	if a.Graphite != nil {
		g.Go(func() error {
			<-gctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			a.Graphite.Stop(sctx)
			return nil
		})
	}
	a.logger.Info("xtsdb started", "target", a.target, "version", version.Version)
	err := g.Wait()
	a.logger.Info("xtsdb stopped")
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (a *App) close() {
	if a.Server != nil {
		_ = a.Server.Close()
	}
	a.Persistent.Close()
	if a.redis != nil {
		_ = a.redis.Close()
	}
}

func buildInfo() prometheus.Collector {
	info := version.Info()
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "xtsdb_build_info",
		Help: "Build information of the running xtsdb binary.",
		ConstLabels: prometheus.Labels{
			"version":   info["version"],
			"revision":  info["revision"],
			"goversion": info["goVersion"],
		},
	})
	g.Set(1)
	return g
}

// WaitReady blocks until all readiness checks pass or ctx expires. It is
// mostly useful for tests.
func (a *App) WaitReady(ctx context.Context) error {
	for {
		if a.Hot.Ping(ctx) == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
