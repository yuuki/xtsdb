// Package server provides the HTTP server shared by all xtsdb roles.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check reports whether a dependency is usable.
type Check func(ctx context.Context) error

// Server is an HTTP server with operational endpoints:
//
//	/metrics        Prometheus metrics
//	/-/healthy      liveness
//	/-/ready        readiness (all checks pass)
//	/debug/pprof/   profiling, when enabled
type Server struct {
	Mux    *http.ServeMux
	srv    *http.Server
	ln     net.Listener
	logger *slog.Logger

	mu     sync.Mutex
	checks map[string]Check

	duration *prometheus.HistogramVec
}

// Options configures a Server.
type Options struct {
	EnablePprof bool
	// TLS serves HTTPS when set.
	TLS *tls.Config
	// Ops wraps /metrics and /debug/pprof, for example to require
	// credentials. /-/healthy and /-/ready are never wrapped.
	Ops func(http.Handler) http.Handler
}

// New creates a server listening on addr.
func New(addr string, reg *prometheus.Registry, opts Options, logger *slog.Logger) *Server {
	ops := opts.Ops
	if ops == nil {
		ops = func(h http.Handler) http.Handler { return h }
	}
	s := &Server{
		Mux:    http.NewServeMux(),
		logger: logger,
		checks: make(map[string]Check),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "xtsdb_http_request_duration_seconds",
			Help:                        "HTTP request latency by handler and status code.",
			Buckets:                     prometheus.DefBuckets,
			NativeHistogramBucketFactor: 1.1,
		}, []string{"handler", "code", "method"}),
	}
	reg.MustRegister(s.duration)
	s.Mux.Handle("/metrics", ops(promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true})))
	s.Mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK\n"))
	})
	s.Mux.HandleFunc("/-/ready", s.ready)
	if opts.EnablePprof {
		s.Mux.Handle("/debug/pprof/", ops(http.HandlerFunc(pprof.Index)))
		s.Mux.Handle("/debug/pprof/cmdline", ops(http.HandlerFunc(pprof.Cmdline)))
		s.Mux.Handle("/debug/pprof/profile", ops(http.HandlerFunc(pprof.Profile)))
		s.Mux.Handle("/debug/pprof/symbol", ops(http.HandlerFunc(pprof.Symbol)))
		s.Mux.Handle("/debug/pprof/trace", ops(http.HandlerFunc(pprof.Trace)))
	}
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.Mux,
		TLSConfig:         opts.TLS,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       time.Minute, // whole request, including the body
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	return s
}

// Instrument wraps a handler with latency metrics.
func (s *Server) Instrument(name string, h http.Handler) http.Handler {
	return promhttp.InstrumentHandlerDuration(s.duration.MustCurryWith(prometheus.Labels{"handler": name}), h)
}

// AddCheck registers a readiness check.
func (s *Server) AddCheck(name string, c Check) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = c
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	s.mu.Lock()
	checks := make(map[string]Check, len(s.checks))
	for k, v := range s.checks {
		checks[k] = v
	}
	s.mu.Unlock()
	var errs []error
	for name, c := range checks {
		if err := c(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("Ready\n"))
}

// Listen opens the listener so that Addr is known before Serve.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}
	s.ln = ln
	return nil
}

// Addr returns the listen address. Listen must have been called.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Close immediately closes the listener and all connections.
func (s *Server) Close() error {
	err := s.srv.Close()
	if s.ln != nil {
		_ = s.ln.Close()
	}
	return err
}

// Serve serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Serve(ctx context.Context, shutdownTimeout time.Duration) error {
	if s.ln == nil {
		if err := s.Listen(); err != nil {
			return err
		}
	}
	errc := make(chan error, 1)
	useTLS := s.srv.TLSConfig != nil
	go func() {
		if useTLS {
			errc <- s.srv.ServeTLS(s.ln, "", "") // certificates come from TLSConfig
			return
		}
		errc <- s.srv.Serve(s.ln)
	}()
	s.logger.Info("HTTP server listening", "addr", s.ln.Addr(), "tls", useTLS)
	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
