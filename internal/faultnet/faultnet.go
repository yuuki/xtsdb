// Package faultnet provides a TCP proxy for tests that injects network
// faults between a client and a server: stalled connections, lost replies
// and resets.
package faultnet

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// Mode is the fault the proxy injects.
type Mode int

const (
	// Pass forwards traffic in both directions.
	Pass Mode = iota
	// Stall holds traffic in both directions, as a network partition does:
	// requests time out, and what was held is delivered once the mode
	// changes back to Pass, as TCP retransmits after a partition heals.
	Stall
	// LoseReplies forwards requests to the server, which acts on them, and
	// resets the connection when the server replies: the client sees an
	// error for a request that took effect.
	LoseReplies
	// Reset resets every open connection, and every new one once it is
	// accepted.
	Reset
)

// Proxy forwards TCP connections from Addr to a target address.
type Proxy struct {
	ln     net.Listener
	target string

	mu      sync.Mutex
	mode    Mode
	changed chan struct{} // closed and replaced when the mode changes
	conns   map[net.Conn]struct{}
	closed  bool

	wg sync.WaitGroup
}

// New starts a proxy to target on a free port of 127.0.0.1. It is closed
// when the test ends.
func New(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target, changed: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr returns the address clients connect to.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// SetMode changes the fault injected from now on. Reset also resets the open
// connections at once.
func (p *Proxy) SetMode(m Mode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.mode = m
	close(p.changed)
	p.changed = make(chan struct{})
	if m == Reset {
		for c := range p.conns {
			reset(c)
		}
	}
}

// Close stops the proxy, closes every connection and waits for its
// goroutines.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	_ = p.ln.Close()
	for c := range p.conns {
		_ = c.Close()
	}
	close(p.changed)
	p.mu.Unlock()
	p.wg.Wait()
}

// state returns the mode and a channel closed when it changes.
func (p *Proxy) state() (Mode, chan struct{}, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode, p.changed, p.closed
}

// track records an open connection; it returns false once the proxy is
// closed.
func (p *Proxy) track(cs ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	for _, c := range cs {
		p.conns[c] = struct{}{}
	}
	return true
}

func (p *Proxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		delete(p.conns, c)
		_ = c.Close()
	}
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // closed
		}
		if mode, _, _ := p.state(); mode == Reset {
			reset(client)
			continue
		}
		server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			reset(client)
			continue
		}
		if !p.track(client, server) {
			_ = client.Close()
			_ = server.Close()
			return
		}
		var pair sync.WaitGroup
		pair.Add(2)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			pair.Wait()
			p.untrack(client, server)
		}()
		go func() { defer pair.Done(); p.forward(client, server, false) }()
		go func() { defer pair.Done(); p.forward(server, client, true) }()
	}
}

// forward copies src to dst under the current mode. reply tells whether src
// is the server.
func (p *Proxy) forward(src, dst net.Conn, reply bool) {
	// Closing both ends makes the other direction return too.
	defer func() { _ = src.Close(); _ = dst.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.deliver(src, dst, buf[:n], reply) {
			return
		}
		if err != nil {
			return
		}
	}
}

// deliver writes b to dst once the mode allows it, and reports whether the
// connection is still open.
func (p *Proxy) deliver(src, dst net.Conn, b []byte, reply bool) bool {
	for {
		mode, changed, closed := p.state()
		switch {
		case closed:
			return false
		case mode == Stall:
			<-changed
			continue
		case mode == Reset || (mode == LoseReplies && reply):
			reset(src)
			reset(dst)
			return false
		}
		_, err := dst.Write(b)
		return err == nil
	}
}

// reset closes c so that its peer sees a connection reset rather than the
// end of the stream.
func reset(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}
