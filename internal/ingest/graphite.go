package ingest

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/model"
)

// maxGraphiteSeconds is far beyond any sensible timestamp (year 5138) but
// small enough to convert to milliseconds without overflow.
const maxGraphiteSeconds = 1e11

// ParseGraphiteLine parses one line of the Graphite plaintext protocol:
//
//	<path>[;tag=value...] <value> [<timestamp>]
//
// The path becomes __name__ and tags become labels. A missing or negative
// timestamp means "now". It returns ok=false for NaN values, which Graphite
// uses to signal missing data.
func ParseGraphiteLine(line string, now time.Time) (Series, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 || len(fields) > 3 {
		return Series{}, false, fmt.Errorf("expected 2 or 3 fields, got %d", len(fields))
	}
	path, tags, _ := strings.Cut(fields[0], ";")
	if path == "" {
		return Series{}, false, errors.New("empty metric path")
	}
	b := labels.NewScratchBuilder(4)
	b.Add(model.MetricName, path)
	for tags != "" {
		var tag string
		tag, tags, _ = strings.Cut(tags, ";")
		name, value, ok := strings.Cut(tag, "=")
		if !ok || name == "" {
			return Series{}, false, fmt.Errorf("invalid tag %q", tag)
		}
		if name == model.MetricName {
			return Series{}, false, fmt.Errorf("tag %q is reserved", name)
		}
		b.Add(name, value)
	}
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return Series{}, false, fmt.Errorf("invalid value %q", fields[1])
	}
	if math.IsNaN(v) {
		return Series{}, false, nil
	}
	ts := now.UnixMilli()
	if len(fields) == 3 {
		sec, err := strconv.ParseFloat(fields[2], 64)
		// Reject values whose conversion to milliseconds would overflow.
		if err != nil || math.IsNaN(sec) || math.Abs(sec) > maxGraphiteSeconds {
			return Series{}, false, fmt.Errorf("invalid timestamp %q", fields[2])
		}
		if sec >= 0 {
			ts = int64(math.Round(sec * 1000))
		}
	}
	return Series{Labels: b.Labels(), Samples: []model.Sample{{T: ts, V: v}}}, true, nil
}

// GraphiteServer accepts the Graphite plaintext protocol over TCP and UDP.
type GraphiteServer struct {
	// TLS, when set before Start, serves TCP over TLS (with client
	// certificates if it requires them) and disables UDP, which cannot be
	// protected.
	TLS *tls.Config

	addr     string
	writer   *Writer
	logger   *slog.Logger
	maxConns int

	tcp     net.Listener
	udp     net.PacketConn
	udpData chan []byte
	closing atomic.Bool
	wg      sync.WaitGroup
	mu      sync.Mutex
	conns   map[net.Conn]struct{}

	lines       *prometheus.CounterVec
	connections *prometheus.CounterVec
}

// Tuning of Graphite ingestion.
const (
	graphiteBatchSize     = 1000
	graphiteFlushInterval = time.Second // longest time a line waits in a batch
	graphiteIdleTimeout   = 5 * time.Minute
	graphiteMaxLine       = 64 << 10
	graphiteUDPWorkers    = 4
	graphiteUDPQueue      = 10_000 // datagrams
	graphiteUDPReadBuffer = 8 << 20
)

// NewGraphiteServer creates a server listening on addr for both TCP and UDP.
// At most maxConns TCP connections are served at a time.
func NewGraphiteServer(addr string, maxConns int, w *Writer, reg prometheus.Registerer, logger *slog.Logger) *GraphiteServer {
	f := promauto.With(reg)
	return &GraphiteServer{
		addr:     addr,
		writer:   w,
		logger:   logger.With("component", "graphite"),
		maxConns: maxConns,
		conns:    make(map[net.Conn]struct{}),
		udpData:  make(chan []byte, graphiteUDPQueue),
		lines: f.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_graphite_lines_total",
			Help: "Graphite lines received, by outcome (parsed, invalid, nan, dropped).",
		}, []string{"outcome"}),
		connections: f.NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_graphite_connections_total",
			Help: "Graphite TCP connections, by outcome (accepted, rejected, tls_failed).",
		}, []string{"outcome"}),
	}
}

// Start opens the listeners and serves in the background.
func (g *GraphiteServer) Start() error {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", g.addr)
	if err != nil {
		return fmt.Errorf("graphite tcp: %w", err)
	}
	if g.TLS != nil {
		g.tcp = tls.NewListener(ln, g.TLS)
		g.logger.Info("listening for Graphite plaintext over TLS (UDP disabled)", "tcp", ln.Addr())
		g.wg.Add(1)
		go g.acceptLoop()
		return nil
	}
	pc, err := lc.ListenPacket(context.Background(), "udp", g.addr)
	if err != nil {
		ln.Close()
		return fmt.Errorf("graphite udp: %w", err)
	}
	if uc, ok := pc.(*net.UDPConn); ok {
		_ = uc.SetReadBuffer(graphiteUDPReadBuffer) // capped by net.core.rmem_max
	}
	g.tcp, g.udp = ln, pc
	g.logger.Info("listening for Graphite plaintext", "tcp", ln.Addr(), "udp", pc.LocalAddr())
	g.wg.Add(2)
	go g.acceptLoop()
	go g.udpLoop()
	for range graphiteUDPWorkers {
		g.wg.Add(1)
		go g.udpWorker()
	}
	return nil
}

// Addr returns the TCP listen address.
func (g *GraphiteServer) Addr() net.Addr { return g.tcp.Addr() }

// UDPAddr returns the UDP listen address, or nil when UDP is disabled.
func (g *GraphiteServer) UDPAddr() net.Addr {
	if g.udp == nil {
		return nil
	}
	return g.udp.LocalAddr()
}

// Stop closes the listeners, makes every connection write what it has
// buffered and close, and waits for that until ctx expires.
func (g *GraphiteServer) Stop(ctx context.Context) {
	if g.tcp == nil || !g.closing.CompareAndSwap(false, true) {
		return
	}
	g.tcp.Close()
	if g.udp != nil {
		g.udp.Close()
	}
	// Wake up blocked reads; serveConn sees closing and returns.
	g.mu.Lock()
	for c := range g.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	g.mu.Unlock()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		g.mu.Lock()
		for c := range g.conns {
			c.Close()
		}
		g.mu.Unlock()
		<-done
	}
}

func (g *GraphiteServer) acceptLoop() {
	defer g.wg.Done()
	sem := make(chan struct{}, g.maxConns)
	backoff := time.Duration(0)
	for {
		c, err := g.tcp.Accept()
		if err != nil {
			if g.closing.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			// Typically EMFILE: back off and keep serving, like net/http.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			g.logger.Warn("accept failed; retrying", "err", err, "backoff", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		select {
		case sem <- struct{}{}:
		default:
			g.connections.WithLabelValues("rejected").Inc()
			c.Close()
			continue
		}
		g.connections.WithLabelValues("accepted").Inc()
		g.mu.Lock()
		g.conns[c] = struct{}{}
		closing := g.closing.Load()
		g.mu.Unlock()
		if closing {
			_ = c.SetReadDeadline(time.Now())
		}
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			defer func() { <-sem }()
			if tc, ok := c.(*tls.Conn); ok && !g.handshake(tc) {
				g.mu.Lock()
				delete(g.conns, c)
				g.mu.Unlock()
				c.Close()
				return
			}
			g.serveConn(c)
			g.mu.Lock()
			delete(g.conns, c)
			g.mu.Unlock()
			c.Close()
		}()
	}
}

// serveConn reads lines and writes them in batches. A line waits at most
// graphiteFlushInterval in a batch, whether or not more data arrives.
// graphiteHandshakeTimeout bounds the TLS handshake, so that clients without
// a valid certificate cannot hold connection slots for the idle timeout
// (like ReadHeaderTimeout for HTTP). A variable for tests.
var graphiteHandshakeTimeout = 10 * time.Second

// handshake completes the TLS handshake within graphiteHandshakeTimeout.
func (g *GraphiteServer) handshake(c *tls.Conn) bool {
	ctx, cancel := context.WithTimeout(context.Background(), graphiteHandshakeTimeout)
	defer cancel()
	if err := c.HandshakeContext(ctx); err != nil {
		g.connections.WithLabelValues("tls_failed").Inc()
		g.logger.Debug("graphite TLS handshake failed", "remote", c.RemoteAddr(), "err", err)
		return false
	}
	return true
}

func (g *GraphiteServer) serveConn(c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	var (
		batch   []Series
		partial []byte
		since   time.Time // when the oldest line of the batch arrived
	)
	flush := func() {
		if len(batch) > 0 {
			g.write(batch)
			batch = batch[:0]
		}
	}
	add := func(line []byte) {
		if s, ok := g.parse(string(line)); ok {
			if len(batch) == 0 {
				since = time.Now()
			}
			batch = append(batch, s)
		}
	}
	for {
		if g.closing.Load() {
			flush()
			return
		}
		deadline := time.Now().Add(graphiteIdleTimeout)
		if len(batch) > 0 {
			deadline = since.Add(graphiteFlushInterval)
		}
		_ = c.SetReadDeadline(deadline)
		chunk, err := r.ReadSlice('\n')
		partial = append(partial, chunk...)
		switch {
		case err == nil:
			add(partial)
			partial = partial[:0]
			if len(batch) >= graphiteBatchSize || (len(batch) > 0 && time.Since(since) >= graphiteFlushInterval) {
				flush()
			}
		case errors.Is(err, bufio.ErrBufferFull):
			if len(partial) > graphiteMaxLine {
				g.lines.WithLabelValues("invalid").Inc()
				g.logger.Warn("closing connection with over-long line", "remote", c.RemoteAddr())
				flush()
				return
			}
		case isTimeout(err):
			if g.closing.Load() {
				flush()
				return
			}
			if len(batch) == 0 && len(partial) == 0 {
				return // idle
			}
			flush()
		default:
			// EOF or connection error: the last line may lack a newline.
			if len(partial) > 0 {
				add(partial)
			}
			flush()
			return
		}
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// udpLoop only copies datagrams off the socket so that the kernel buffer
// drains quickly; workers parse and write them. When the workers cannot keep
// up, datagrams are dropped and counted rather than blocking the socket.
func (g *GraphiteServer) udpLoop() {
	defer g.wg.Done()
	defer close(g.udpData)
	buf := make([]byte, 64<<10)
	for {
		n, _, err := g.udp.ReadFrom(buf)
		if err != nil {
			if g.closing.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			g.logger.Warn("udp read failed", "err", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		select {
		case g.udpData <- append([]byte(nil), buf[:n]...):
		default:
			g.lines.WithLabelValues("dropped").Add(float64(max(1, strings.Count(string(buf[:n]), "\n"))))
		}
	}
}

func (g *GraphiteServer) udpWorker() {
	defer g.wg.Done()
	var batch []Series
	t := time.NewTicker(graphiteFlushInterval)
	defer t.Stop()
	for {
		select {
		case d, ok := <-g.udpData:
			if !ok {
				g.write(batch)
				return
			}
			for line := range strings.Lines(string(d)) {
				if s, ok := g.parse(line); ok {
					batch = append(batch, s)
				}
			}
			if len(batch) >= graphiteBatchSize {
				g.write(batch)
				batch = nil
			}
		case <-t.C:
			g.write(batch)
			batch = nil
		}
	}
}

func (g *GraphiteServer) parse(line string) (Series, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Series{}, false
	}
	s, ok, err := ParseGraphiteLine(line, time.Now())
	switch {
	case err != nil:
		g.lines.WithLabelValues("invalid").Inc()
		g.logger.Debug("invalid graphite line", "line", line, "err", err)
	case !ok:
		g.lines.WithLabelValues("nan").Inc()
	default:
		g.lines.WithLabelValues("parsed").Inc()
	}
	return s, ok && err == nil
}

func (g *GraphiteServer) write(batch []Series) {
	if len(batch) == 0 {
		return
	}
	// Graphite has no way to report errors to the sender; log them.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := g.writer.Write(ctx, batch)
	if err != nil {
		g.logger.Error("graphite write failed", "series", len(batch), "err", err)
		return
	}
	if res.Rejected() > 0 {
		g.logger.Debug("graphite samples rejected", "reason", describeRejections(res))
	}
}
