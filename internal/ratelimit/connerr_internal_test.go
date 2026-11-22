package ratelimit

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis listens on loopback and hands every connection to handle,
// counting accepts. It is what sits at a Redis address when Redis isn't
// answering properly: a load balancer or proxy in front of a dead Redis,
// or a Redis refusing clients.
func fakeRedis(t *testing.T, handle func(net.Conn)) (addr string, accepts *atomic.Int64) {
	t.Helper()
	accepts = new(atomic.Int64)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go handle(c)
		}
	}()
	return ln.Addr().String(), accepts
}

// The ways an endpoint can accept a connection and then close it.
var (
	acceptClose = func(c net.Conn) { c.Close() }
	// Reads the first command (HELLO, or the TLS ClientHello), then resets.
	acceptReadReset = func(c net.Conn) {
		c.Read(make([]byte, 64))
		c.(*net.TCPConn).SetLinger(0)
		c.Close()
	}
	// A Redis at its maxclients limit writes this and closes.
	maxClients = func(c net.Conn) {
		c.Write([]byte("-ERR max number of clients reached\r\n"))
		time.Sleep(time.Millisecond)
		c.Close()
	}
	// A stalled Redis: reads, never answers.
	stall = func(c net.Conn) { io.Copy(io.Discard, c) }
)

// settle waits until n has stopped changing for 300 ms, at most 5 s, and
// returns it.
func settle(n *atomic.Int64) int64 {
	last := n.Load()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		time.Sleep(300 * time.Millisecond)
		cur := n.Load()
		if cur == last {
			break
		}
		last = cur
	}
	return last
}

func connLimiter(t *testing.T, url string, cooldown time.Duration) (*Limiter, *redis.Client) {
	t.Helper()
	client, err := NewClient(url, 100*time.Millisecond, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	l, err := New(Config{Client: client, PerMinute: 1_000_000, Timeout: 100 * time.Millisecond,
		BreakerCooldown: cooldown, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return l, client
}

// isConnError against what go-redis really returns, not hand-built errors:
// the shapes differ by platform, TLS and where in the handshake it broke.
func TestIsConnError(t *testing.T) {
	refused := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		return addr
	}
	addr := func(h func(net.Conn)) string { a, _ := fakeRedis(t, h); return a }
	tests := []struct {
		name string
		url  string
		want bool
	}{
		{"accept and close", "redis://" + addr(acceptClose), true},
		{"reset after HELLO", "redis://" + addr(acceptReadReset), true},
		{"TLS accept and close", "rediss://" + addr(acceptClose) + "?skip_verify=true", true},
		{"TLS reset in handshake", "rediss://" + addr(acceptReadReset) + "?skip_verify=true", true},
		// What an outage looks like: these count from the first one.
		{"stalled", "redis://" + addr(stall), false},
		{"refused", "redis://" + refused(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.url, 100*time.Millisecond, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			err = client.Ping(context.Background()).Err()
			if err == nil {
				t.Fatal("ping succeeded")
			}
			if got := isConnError(err); got != tt.want {
				t.Errorf("isConnError(%T %v) = %v, want %v", err, err, got, tt.want)
			}
		})
	}
	for _, err := range []error{nil, errors.New("ERR something"), context.DeadlineExceeded,
		&net.OpError{Op: "dial", Net: "tcp", Err: io.EOF}} {
		if isConnError(err) {
			t.Errorf("isConnError(%v) = true, want false", err)
		}
	}
}

// An endpoint that accepts and then closes every connection: each failure
// is a connection error, but past the slack they are an outage. The breaker
// opens within connSlack+BreakerThreshold calls, and then stops the
// connections too.
func TestBreaker_AcceptThenClose(t *testing.T) {
	shapes := []struct {
		name   string
		handle func(net.Conn)
		tls    bool
	}{
		{"close", acceptClose, false},
		{"reset", acceptReadReset, false},
		{"maxclients", maxClients, false},
		{"TLS close", acceptClose, true},
		{"TLS reset", acceptReadReset, true},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			addr, accepts := fakeRedis(t, s.handle)
			url := "redis://" + addr
			if s.tls {
				url = "rediss://" + addr + "?skip_verify=true"
			}
			l, client := connLimiter(t, url, time.Minute)
			bound := l.breaker.connSlack + BreakerThreshold
			for i := range bound {
				if _, err := l.Allow(context.Background(), "client"); errors.Is(err, ErrCircuitOpen) {
					t.Logf("opened after %d calls", i)
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !l.BreakerOpen() {
				t.Fatalf("breaker still closed after %d calls (%d accepts)", bound, accepts.Load())
			}
			tries := int64(client.Options().MaxRetries + 1)
			if n := accepts.Load(); n > tries*int64(bound) {
				t.Errorf("%d accepts for %d calls, want at most %d", n, bound, tries*int64(bound))
			}
			// Dials started before it opened may still be retrying (go-redis
			// tries a failed dial up to 5 times, in the background); those
			// are bounded, and not what this checks.
			before := settle(accepts)
			for range 20 {
				if _, err := l.Allow(context.Background(), "client"); !errors.Is(err, ErrCircuitOpen) {
					t.Fatalf("open breaker: err = %v, want ErrCircuitOpen", err)
				}
			}
			if n := settle(accepts) - before; n != 0 {
				t.Errorf("%d accepts while open, want 0", n)
			}
		})
	}
}

// The breaker opened on a stall; after the cooldown the endpoint accepts
// and closes. Probes that get a connection error are released only within
// the slack, then each restarts the cooldown: over a second of steady
// requests the connections stay bounded and the breaker stays open.
func TestBreaker_ProbeConnErrorsRearm(t *testing.T) {
	const cooldown = 300 * time.Millisecond
	var closing atomic.Bool
	addr, accepts := fakeRedis(t, func(c net.Conn) {
		if closing.Load() {
			acceptClose(c)
			return
		}
		stall(c)
	})
	l, client := connLimiter(t, "redis://"+addr, cooldown)
	for range BreakerThreshold {
		l.Allow(context.Background(), "client")
	}
	if !l.BreakerOpen() {
		t.Fatal("stalls did not open the breaker")
	}
	time.Sleep(cooldown)
	closing.Store(true)
	before := accepts.Load()
	const window = time.Second
	for start := time.Now(); time.Since(start) < window; {
		l.Allow(context.Background(), "client")
		time.Sleep(time.Millisecond)
	}
	// The slack's worth of released probes, then one probe per cooldown.
	tries := int64(client.Options().MaxRetries + 1)
	limit := tries * int64(l.breaker.connSlack+1+int(window/cooldown)+1)
	if n := accepts.Load() - before; n > limit {
		t.Errorf("%d accepts in %v, want at most %d", n, window, limit)
	}
	if !l.BreakerOpen() {
		t.Error("breaker closed against an endpoint that closes every connection")
	}
}

// The slack is what dead pooled connections can account for: each failed
// call uses up MaxRetries+1 of them.
func TestNew_ConnSlack(t *testing.T) {
	l, client := connLimiter(t, "redis://127.0.0.1:1", time.Minute) // nothing listens
	opts := client.Options()
	if opts.PoolSize != 64 || opts.MaxRetries != 1 {
		t.Fatalf("PoolSize %d, MaxRetries %d; want 64, 1", opts.PoolSize, opts.MaxRetries)
	}
	if l.breaker.connSlack != 32 {
		t.Errorf("connSlack = %d, want 32 (64 connections / 2 tries)", l.breaker.connSlack)
	}
}
