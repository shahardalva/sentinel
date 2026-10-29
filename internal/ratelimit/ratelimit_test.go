package ratelimit_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// t0 is a fixed Redis clock, so token counts and waits are exact.
var t0 = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

// newLimiter returns a limiter of perMinute backed by a fresh miniredis
// whose clock is frozen at t0.
func newLimiter(t *testing.T, perMinute int) (*ratelimit.Limiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(t0)
	return limiterFor(t, "redis://"+mr.Addr(), perMinute, time.Second), mr
}

func limiterFor(t *testing.T, url string, perMinute int, timeout time.Duration) *ratelimit.Limiter {
	t.Helper()
	client, err := ratelimit.NewClient(url, timeout, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	l, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: perMinute, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func allow(t *testing.T, l *ratelimit.Limiter, client string) (allowed bool, remaining int64, retry time.Duration) {
	t.Helper()
	res, err := l.Allow(context.Background(), client)
	if err != nil {
		t.Fatalf("Allow(%q): %v", client, err)
	}
	return res.Allowed, res.Remaining, res.RetryAfter
}

func TestAllow_UnderLimit(t *testing.T) {
	l, _ := newLimiter(t, 5)
	for want := int64(4); want >= 0; want-- {
		ok, remaining, _ := allow(t, l, "client")
		if !ok || remaining != want {
			t.Fatalf("Allow = %v, remaining %d; want true, %d", ok, remaining, want)
		}
	}
}

func TestAllow_OverLimit(t *testing.T) {
	l, _ := newLimiter(t, 60) // one token per second
	for range 60 {
		if ok, _, _ := allow(t, l, "client"); !ok {
			t.Fatal("denied within the burst")
		}
	}
	ok, remaining, retry := allow(t, l, "client")
	if ok || remaining != 0 {
		t.Fatalf("61st request: allowed %v, remaining %d; want denied, 0", ok, remaining)
	}
	if retry != time.Second {
		t.Errorf("RetryAfter = %v, want 1s", retry)
	}
}

func TestAllow_Refills(t *testing.T) {
	l, mr := newLimiter(t, 60)
	for range 60 {
		allow(t, l, "client")
	}

	mr.SetTime(t0.Add(500 * time.Millisecond))
	if ok, _, retry := allow(t, l, "client"); ok || retry != 500*time.Millisecond {
		t.Errorf("after 0.5s: allowed %v, RetryAfter %v; want denied, 500ms", ok, retry)
	}

	mr.SetTime(t0.Add(time.Second))
	if ok, remaining, _ := allow(t, l, "client"); !ok || remaining != 0 {
		t.Errorf("after 1s: allowed %v, remaining %d; want allowed, 0", ok, remaining)
	}
	if ok, _, _ := allow(t, l, "client"); ok {
		t.Error("second request after 1s allowed; only one token had refilled")
	}

	// The bucket refills to capacity and no further.
	mr.SetTime(t0.Add(10 * time.Minute))
	if ok, remaining, _ := allow(t, l, "client"); !ok || remaining != 59 {
		t.Errorf("after 10m: allowed %v, remaining %d; want allowed, 59", ok, remaining)
	}
}

// A Redis clock that steps back, as after a failover to a replica whose
// clock is behind, must not stall refills until it catches up.
func TestAllow_ClockGoesBack(t *testing.T) {
	l, mr := newLimiter(t, 60)
	for range 60 {
		allow(t, l, "client")
	}
	back := t0.Add(-10 * time.Minute)
	mr.SetTime(back)
	if ok, _, retry := allow(t, l, "client"); ok || retry != time.Second {
		t.Errorf("clock stepped back: allowed %v, RetryAfter %v; want denied, 1s (no time passed)", ok, retry)
	}
	mr.SetTime(back.Add(time.Second))
	if ok, _, _ := allow(t, l, "client"); !ok {
		t.Error("1s after the step back: denied; the bucket should refill from the new clock")
	}
}

func TestAllow_SeparateClients(t *testing.T) {
	l, _ := newLimiter(t, 3)
	for range 3 {
		allow(t, l, "a")
	}
	if ok, _, _ := allow(t, l, "a"); ok {
		t.Fatal("a: allowed past its limit")
	}
	if ok, remaining, _ := allow(t, l, "b"); !ok || remaining != 2 {
		t.Errorf("b: allowed %v, remaining %d; want allowed, 2 (its own bucket)", ok, remaining)
	}
}

func TestAllow_KeyExpires(t *testing.T) {
	l, mr := newLimiter(t, 60)
	allow(t, l, "client")
	keys := mr.Keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "sentinel:ratelimit:") {
		t.Fatalf("keys = %q", keys)
	}
	// A full refill takes a minute; the key must not outlive it by much.
	if ttl := mr.TTL(keys[0]); ttl <= 0 || ttl > time.Minute+2*time.Second {
		t.Errorf("TTL = %v, want about a minute", ttl)
	}
}

func TestAllow_RedisDown(t *testing.T) {
	l, mr := newLimiter(t, 60)
	mr.Close()
	if _, err := l.Allow(context.Background(), "client"); err == nil {
		t.Fatal("Allow with Redis down: no error")
	}
}

// blackHole accepts connections and reads from them but never answers, like
// a Redis that has stalled.
func blackHole(t *testing.T) string {
	t.Helper()
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
			t.Cleanup(func() { c.Close() })
			go io.Copy(io.Discard, c)
		}
	}()
	return ln.Addr().String()
}

func TestAllow_SlowRedisTimesOut(t *testing.T) {
	const timeout = ratelimit.DefaultTimeout
	l := limiterFor(t, "redis://"+blackHole(t), 60, timeout)
	start := time.Now()
	_, err := l.Allow(context.Background(), "client")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Allow against a stalled Redis: no error")
	}
	if elapsed < timeout || elapsed > 2*timeout {
		t.Errorf("Allow took %v with a %v timeout", elapsed, timeout)
	}
}

// slowRedis forwards to target, holding back each reply chunk while
// *delay is set. Connection setup is delayed too, so callers warm up a
// connection before setting it.
func slowRedis(t *testing.T, target string, delay *atomic.Int64) string {
	t.Helper()
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
			up, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			t.Cleanup(func() { c.Close(); up.Close() })
			go io.Copy(up, c)
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := up.Read(buf)
					if n > 0 {
						time.Sleep(time.Duration(delay.Load()))
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						c.Close()
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// More calls at once than the pool has connections: the extra ones give up
// with ErrBusy, and since Redis itself answered, the breaker stays closed.
func TestAllow_PoolBusyDoesNotTripBreaker(t *testing.T) {
	const timeout = 100 * time.Millisecond
	mr := miniredis.RunT(t)
	delay := new(atomic.Int64)
	client, err := ratelimit.NewClient("redis://"+slowRedis(t, mr.Addr(), delay), timeout, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	if n := client.Options().PoolSize; n != 1 {
		t.Fatalf("PoolSize = %d, want 1", n)
	}
	l, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: 1000, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Allow(context.Background(), "client"); err != nil {
		t.Fatalf("warm-up: %v", err)
	}

	delay.Store(int64(70 * time.Millisecond)) // each call holds the one connection this long
	const n = 4 * ratelimit.BreakerThreshold
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := l.Allow(context.Background(), "client")
			errs <- err
		}()
	}
	var busy int
	for range n {
		if err := <-errs; errors.Is(err, ratelimit.ErrBusy) {
			busy++
		}
	}
	if busy == 0 {
		t.Error("no call got ErrBusy; the test did not exhaust the pool")
	}

	delay.Store(0)
	if _, err := l.Allow(context.Background(), "client"); err != nil {
		t.Errorf("after the burst: err = %v, want the breaker still closed", err)
	}
}

func TestRouteClientLogs(t *testing.T) {
	logs := new(syncBuffer)
	ratelimit.RouteClientLogs(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { ratelimit.RouteClientLogs(slog.New(slog.NewTextHandler(io.Discard, nil))) })
	// NewClient's dials run in the background for up to PoolDialTimeout an
	// attempt, so a failed dial would log after the test; this client
	// fails its one dial attempt within the Ping.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, DialerRetries: 1}) // nothing listens
	defer client.Close()
	client.Ping(context.Background())
	if out := logs.String(); !strings.Contains(out, `"level":"DEBUG"`) || !strings.Contains(out, `"component":"go-redis"`) {
		t.Errorf("go-redis output not routed to the logger at DEBUG: %q", out)
	}
}

func TestNew_Validates(t *testing.T) {
	client, err := ratelimit.NewClient("redis://127.0.0.1:1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := ratelimit.New(ratelimit.Config{Client: client}); err == nil {
		t.Error("New with PerMinute 0: no error")
	}
	if _, err := ratelimit.New(ratelimit.Config{PerMinute: 60}); err == nil {
		t.Error("New with no client: no error")
	}
	if _, err := ratelimit.NewClient("http://example.com", 0, 0); err == nil {
		t.Error("NewClient with an http URL: no error")
	}
}
