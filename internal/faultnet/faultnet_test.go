package faultnet

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// echoServer echoes every line and counts the lines it received.
type echoServer struct {
	ln    net.Listener
	lines atomic.Int64
}

func newEchoServer(t *testing.T) *echoServer {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &echoServer{ln: ln}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					s.lines.Add(1)
					if _, err := io.WriteString(c, line); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); <-done })
	return s
}

func dial(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	c, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// roundTrip sends a line and waits up to timeout for its echo.
func roundTrip(c net.Conn, timeout time.Duration) (string, error) {
	if _, err := io.WriteString(c, "ping\n"); err != nil {
		return "", err
	}
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	return bufio.NewReader(c).ReadString('\n')
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestPass(t *testing.T) {
	s := newEchoServer(t)
	p := New(t, s.ln.Addr().String())
	if got, err := roundTrip(dial(t, p), 5*time.Second); err != nil || got != "ping\n" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A stalled connection delivers nothing until the partition heals, and then
// everything that was held.
func TestStall(t *testing.T) {
	s := newEchoServer(t)
	p := New(t, s.ln.Addr().String())
	c := dial(t, p)
	if _, err := roundTrip(c, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	p.SetMode(Stall)
	if _, err := roundTrip(c, 100*time.Millisecond); !isTimeout(err) {
		t.Fatalf("stalled request: %v, want a timeout", err)
	}
	if n := s.lines.Load(); n != 1 {
		t.Fatalf("the server received %d lines while stalled, want 1", n)
	}
	p.SetMode(Pass)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if got, err := bufio.NewReader(c).ReadString('\n'); err != nil || got != "ping\n" {
		t.Fatalf("after the partition healed: %q, %v", got, err)
	}
}

// A lost reply means the server acted on the request, and the client got an
// error instead of the reply.
func TestLoseReplies(t *testing.T) {
	s := newEchoServer(t)
	p := New(t, s.ln.Addr().String())
	c := dial(t, p)
	p.SetMode(LoseReplies)
	if got, err := roundTrip(c, 5*time.Second); err == nil || isTimeout(err) {
		t.Fatalf("lost reply: %q, %v, want a connection error", got, err)
	}
	if n := s.lines.Load(); n != 1 {
		t.Fatalf("the server received %d lines, want 1", n)
	}
}

func TestReset(t *testing.T) {
	s := newEchoServer(t)
	p := New(t, s.ln.Addr().String())
	open := dial(t, p)
	if _, err := roundTrip(open, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	p.SetMode(Reset)
	if _, err := roundTrip(open, 5*time.Second); err == nil || isTimeout(err) {
		t.Fatalf("open connection: %v, want a reset", err)
	}
	// The reset can reach a new connection before or after connect returns.
	if c, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.Addr()); err == nil {
		_, err = roundTrip(c, 5*time.Second)
		c.Close()
		if err == nil || isTimeout(err) {
			t.Fatalf("new connection: %v, want a reset", err)
		}
	}
	p.SetMode(Pass)
	if got, err := roundTrip(dial(t, p), 5*time.Second); err != nil || got != "ping\n" {
		t.Fatalf("after the resets: %q, %v", got, err)
	}
}

// Close returns while connections are stalled, and leaves no goroutines
// behind (checked by goleak).
func TestCloseWhileStalled(t *testing.T) {
	s := newEchoServer(t)
	p := New(t, s.ln.Addr().String())
	c := dial(t, p)
	p.SetMode(Stall)
	if _, err := io.WriteString(c, "ping\n"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	p.SetMode(Pass) // no effect once closed
}
