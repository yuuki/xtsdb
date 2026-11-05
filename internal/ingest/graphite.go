package ingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/yuuki/xtsdb/internal/model"
)

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
		if err != nil || math.IsNaN(sec) || math.IsInf(sec, 0) {
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
	addr   string
	writer *Writer
	logger *slog.Logger

	tcp   net.Listener
	udp   net.PacketConn
	wg    sync.WaitGroup
	mu    sync.Mutex
	conns map[net.Conn]struct{}

	lines *prometheus.CounterVec
}

// Batching limits for Graphite connections.
const (
	graphiteBatchSize     = 1000
	graphiteFlushInterval = time.Second
	graphiteMaxLine       = 64 << 10
)

// NewGraphiteServer creates a server listening on addr for both TCP and UDP.
func NewGraphiteServer(addr string, w *Writer, reg prometheus.Registerer, logger *slog.Logger) *GraphiteServer {
	return &GraphiteServer{
		addr:   addr,
		writer: w,
		logger: logger.With("component", "graphite"),
		conns:  make(map[net.Conn]struct{}),
		lines: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "xtsdb_graphite_lines_total",
			Help: "Graphite lines received, by outcome.",
		}, []string{"outcome"}),
	}
}

// Start opens the listeners and serves in the background.
func (g *GraphiteServer) Start() error {
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("graphite tcp: %w", err)
	}
	pc, err := net.ListenPacket("udp", g.addr)
	if err != nil {
		ln.Close()
		return fmt.Errorf("graphite udp: %w", err)
	}
	g.tcp, g.udp = ln, pc
	g.logger.Info("listening for Graphite plaintext", "tcp", ln.Addr(), "udp", pc.LocalAddr())
	g.wg.Add(2)
	go g.acceptLoop()
	go g.udpLoop()
	return nil
}

// Addr returns the TCP listen address.
func (g *GraphiteServer) Addr() net.Addr { return g.tcp.Addr() }

// Stop closes the listeners and waits for open connections to drain until
// ctx expires, then closes them.
func (g *GraphiteServer) Stop(ctx context.Context) {
	if g.tcp == nil {
		return
	}
	g.tcp.Close()
	g.udp.Close()
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
	for {
		c, err := g.tcp.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				g.logger.Error("accept failed", "err", err)
			}
			return
		}
		g.mu.Lock()
		g.conns[c] = struct{}{}
		g.mu.Unlock()
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.serveConn(c)
			g.mu.Lock()
			delete(g.conns, c)
			g.mu.Unlock()
			c.Close()
		}()
	}
}

// serveConn reads lines and writes them in batches, flushing at least
// every graphiteFlushInterval.
func (g *GraphiteServer) serveConn(c net.Conn) {
	r := bufio.NewReaderSize(c, 64<<10)
	var (
		batch   []Series
		partial []byte
	)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		g.write(batch)
		batch = batch[:0]
	}
	for {
		_ = c.SetReadDeadline(time.Now().Add(graphiteFlushInterval))
		chunk, err := r.ReadSlice('\n')
		partial = append(partial, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(partial) > graphiteMaxLine {
				g.lines.WithLabelValues("invalid").Inc()
				g.logger.Debug("dropping connection with over-long line", "remote", c.RemoteAddr())
				flush()
				return
			}
			continue
		}
		if err == nil {
			if s, ok := g.parse(string(partial)); ok {
				batch = append(batch, s)
				if len(batch) >= graphiteBatchSize {
					flush()
				}
			}
			partial = partial[:0]
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			flush()
			continue
		}
		// EOF or connection error: the last line may lack a newline.
		if len(partial) > 0 {
			if s, ok := g.parse(string(partial)); ok {
				batch = append(batch, s)
			}
		}
		flush()
		return
	}
}

func (g *GraphiteServer) udpLoop() {
	defer g.wg.Done()
	buf := make([]byte, 64<<10)
	for {
		n, _, err := g.udp.ReadFrom(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				g.logger.Error("udp read failed", "err", err)
			}
			return
		}
		var batch []Series
		for line := range strings.Lines(string(buf[:n])) {
			if s, ok := g.parse(line); ok {
				batch = append(batch, s)
			}
		}
		g.write(batch)
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
