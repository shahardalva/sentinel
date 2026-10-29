package ratelimit_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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
			go func() {
				defer c.Close()
				time.Sleep(coldDial)
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
	return "rediss://" + ln.Addr().String() + "?skip_verify=true"
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
	want := ratelimit.MinIdleConns
	if n := client.Options().MinIdleConns; n != want {
		t.Errorf("MinIdleConns = %d, want %d", n, want)
	}
	l, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: 1000})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready, err := ratelimit.WarmUp(ctx, client)
	if ready != want || err != nil {
		t.Errorf("WarmUp = %d, %v; want %d, nil", ready, err, want)
	}

	// As many at once as the pool keeps warm: each needs a connection of
	// its own, and a cold one can't be set up within the 50 ms.
	errs := make(chan error, want)
	for range want {
		go func() {
			_, err := l.Allow(context.Background(), "client")
			errs <- err
		}()
	}
	var failed int
	for range want {
		if err := <-errs; err != nil {
			failed++
			t.Log(err)
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d calls after warm-up failed, want 0", failed, want)
	}
	if l.BreakerOpen() {
		t.Error("breaker open after the burst")
	}
}

// The pool caps MinIdleConns, so a small pool is warmed to its size.
func TestNewClient_MinIdleConnsWithinPool(t *testing.T) {
	client, err := ratelimit.NewClient("redis://127.0.0.1:1", 0, 4) // nothing listens
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	opts := client.Options()
	if opts.MinIdleConns != 4 || opts.PoolSize != 4 {
		t.Errorf("MinIdleConns %d, PoolSize %d; want 4, 4", opts.MinIdleConns, opts.PoolSize)
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
	client, err := ratelimit.NewClient(coldRedis(t, mr.Addr()), ratelimit.DefaultTimeout, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), coldDial/3) // shorter than any dial
	defer cancel()
	start := time.Now()
	ready, err := ratelimit.WarmUp(ctx, client)
	if ready != 0 || err == nil {
		t.Errorf("WarmUp = %d, %v; want 0 and an error", ready, err)
	}
	if d := time.Since(start); d > coldDial {
		t.Errorf("WarmUp took %v, want it to stop at its context's deadline", d)
	}
}
