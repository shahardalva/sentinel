package ratelimit_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/shahardalva/sentinel/internal/devcert"
	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// coldDial is how long coldRedis takes to set up each new connection:
// longer than a call's DefaultTimeout, well under PoolDialTimeout.
const coldDial = 300 * time.Millisecond

// coldRedis returns a rediss:// URL for a TLS proxy to target whose every
// TLS handshake waits coldDial first, like a cold dial (DNS, TCP and TLS
// on a slow network). A local TCP connect is instant whatever the server
// does, so the handshake is the part of a dial a test can slow down; the
// dial timeout covers it, the call's context does not get to wait for it.
func coldRedis(t *testing.T, target string) string {
	t.Helper()
	return tlsRedis(t, target, coldDial)
}

// tlsRedis is coldRedis with a handshake delay of delay.
func tlsRedis(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	url, _ := tlsRedisConns(t, target, delay)
	return url
}

// tlsRedisConns is tlsRedis, also returning how many client connections
// the proxy has open. It closes one as soon as its Redis connection ends.
func tlsRedisConns(t *testing.T, target string, delay time.Duration) (string, *atomic.Int64) {
	t.Helper()
	open := new(atomic.Int64)
	b, err := devcert.Generate([]string{"127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(b.ServerCert, b.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
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
			open.Add(1)
			go func() {
				defer open.Add(-1)
				defer c.Close()
				time.Sleep(delay)
				tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}})
				if tc.Handshake() != nil {
					return
				}
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				go func() { io.Copy(up, tc); up.Close() }()
				io.Copy(tc, up)
			}()
		}
	}()
	return "rediss://" + ln.Addr().String() + "?skip_verify=true", open
}

// warmUp runs WarmUp for n connections with a generous bound and fails the
// test unless all n are ready.
func warmUp(t *testing.T, client *redis.Client, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if ready, err := ratelimit.WarmUp(ctx, client, n); ready != n || err != nil {
		t.Fatalf("WarmUp = %d, %v; want %d, nil", ready, err, n)
	}
}

// A burst of calls right after startup, each with the real 50 ms budget,
// against a Redis whose connections take longer than that to set up: once
// WarmUp has run, none of them has to dial, so none fails.
func TestWarmUp_FirstCallsAfterStartupSucceed(t *testing.T) {
	mr := miniredis.RunT(t)
	client, err := ratelimit.NewClient(coldRedis(t, mr.Addr()), ratelimit.DefaultTimeout, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	l, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: 1000})
	if err != nil {
		t.Fatal(err)
	}
	const n = ratelimit.WarmConns
	warmUp(t, client, n)

	// As many at once as were warmed: each needs a connection of its own,
	// and a cold one can't be set up within the 50 ms.
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := l.Allow(context.Background(), "client")
			errs <- err
		}()
	}
	var failed int
	for range n {
		if err := <-errs; err != nil {
			failed++
			t.Log(err)
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d calls after warm-up failed, want 0", failed, n)
	}
	if l.BreakerOpen() {
		t.Error("breaker open after the burst")
	}
}

// Warm-up runs before any request, so a Redis slower than a call's timeout
// to answer (a cold cache, a busy host at startup) doesn't fail it: its
// reads get PoolDialTimeout, not the 50 ms.
func TestWarmUp_ReadsNotCappedByCallTimeout(t *testing.T) {
	mr := miniredis.RunT(t)
	delay := new(atomic.Int64)
	delay.Store(int64(3 * ratelimit.DefaultTimeout))
	client, err := ratelimit.NewClient("redis://"+slowRedis(t, mr.Addr(), delay), ratelimit.DefaultTimeout, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	warmUp(t, client, 4)
}

// WarmUp sets up at most the pool's size: holding more would wait for
// connections that can't exist until ctx ends.
func TestWarmUp_CappedByPoolSize(t *testing.T) {
	mr := miniredis.RunT(t)
	client, err := ratelimit.NewClient("redis://"+mr.Addr(), ratelimit.DefaultTimeout, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if ready, err := ratelimit.WarmUp(ctx, client, 16); ready != 4 || err != nil {
		t.Errorf("WarmUp(16) on a pool of 4 = %d, %v; want 4, nil", ready, err)
	}
	if n := client.PoolStats().TotalConns; n != 4 {
		t.Errorf("pool has %d connections, want 4", n)
	}
}

// Only what WarmUp sets up: the pool doesn't keep idle connections of its
// own, which would double the warm-up and start dials at construction.
func TestWarmUp_LeavesExactlyN(t *testing.T) {
	mr := miniredis.RunT(t)
	client, err := ratelimit.NewClient(coldRedis(t, mr.Addr()), ratelimit.DefaultTimeout, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	warmUp(t, client, 8)
	if st := client.PoolStats(); st.TotalConns != 8 || st.IdleConns != 8 {
		t.Errorf("pool: %d connections, %d idle; want 8, 8", st.TotalConns, st.IdleConns)
	}
}

func TestNewClient_Options(t *testing.T) {
	client, err := ratelimit.NewClient("redis://127.0.0.1:1", 0, 4) // nothing listens
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	opts := client.Options()
	if opts.PoolSize != 4 || opts.MinIdleConns != 0 {
		t.Errorf("PoolSize %d, MinIdleConns %d; want 4, 0", opts.PoolSize, opts.MinIdleConns)
	}
	// The call's budget stays the client's timeout; only dials get longer.
	if opts.DialTimeout != ratelimit.PoolDialTimeout || opts.ReadTimeout != ratelimit.DefaultTimeout {
		t.Errorf("DialTimeout %v, ReadTimeout %v; want %v, %v",
			opts.DialTimeout, opts.ReadTimeout, ratelimit.PoolDialTimeout, ratelimit.DefaultTimeout)
	}
	// In auto mode, connections handshaking at once in WarmUp race in
	// go-redis (see NewClient).
	if m := opts.MaintNotificationsConfig; m == nil || m.Mode != maintnotifications.ModeDisabled {
		t.Errorf("maintenance notifications %+v, want disabled", m)
	}
}

// WarmUp gives up when ctx ends and says how many connections it got.
func TestWarmUp_Bounded(t *testing.T) {
	mr := miniredis.RunT(t)
	client, err := ratelimit.NewClient(coldRedis(t, mr.Addr()), ratelimit.DefaultTimeout, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), coldDial/3) // shorter than any dial
	defer cancel()
	start := time.Now()
	ready, err := ratelimit.WarmUp(ctx, client, ratelimit.WarmConns)
	if ready != 0 || err == nil {
		t.Errorf("WarmUp = %d, %v; want 0 and an error", ready, err)
	}
	if d := time.Since(start); d > coldDial {
		t.Errorf("WarmUp took %v, want it to stop at its context's deadline", d)
	}
}
