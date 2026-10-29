package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// redisConfig returns the gateway's config for a Redis at addr, with the
// default SENTINEL_MAX_CONCURRENT, so the pool has room for WarmConns.
func redisConfig(t *testing.T, addr string) config {
	t.Helper()
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY":  "sk",
		"SENTINEL_API_KEYS":  "k1",
		"SENTINEL_REDIS_URL": "redis://" + addr,
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cfg.redis.Close() })
	return cfg
}

// shortWarmUp sets redisWarmUpTimeout to d for the test.
func shortWarmUp(t *testing.T, d time.Duration) {
	old := redisWarmUpTimeout
	redisWarmUpTimeout = d
	t.Cleanup(func() { redisWarmUpTimeout = old })
}

// Startup sets up WarmConns connections before serving, each past its
// handshake, and says so.
func TestStartRateLimit_WarmsPool(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := redisConfig(t, mr.Addr())
	var buf bytes.Buffer
	before := mr.CommandCount()
	startRateLimit(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)), cfg)

	// The ping's HELLO and PING, then a PING on every warmed connection
	// and a HELLO on each new one: the ping's connection is reused.
	if n, want := mr.CommandCount()-before, 2*ratelimit.WarmConns; n < want {
		t.Errorf("Redis got %d commands during startup, want at least %d", n, want)
	}
	if n := mr.CurrentConnectionCount(); n != ratelimit.WarmConns {
		t.Errorf("Redis has %d connections after startup, want %d", n, ratelimit.WarmConns)
	}
	out := buf.String()
	want := "ready=" + strconv.Itoa(ratelimit.WarmConns)
	if !strings.Contains(out, `msg="redis pool warm"`) || !strings.Contains(out, want) {
		t.Errorf("log missing the warm-up line with %s:\n%s", want, out)
	}
	if strings.Contains(out, "level=WARN") {
		t.Errorf("warning on a healthy Redis:\n%s", out)
	}
}

// A warm-up that can't finish within its bound is a warning with the
// count, and startup goes on: the fail mode covers the rest.
func TestStartRateLimit_WarmUpIncomplete(t *testing.T) {
	const served = 4
	shortWarmUp(t, 200*time.Millisecond)
	mr := miniredis.RunT(t)
	cfg := redisConfig(t, servesFirst(t, mr.Addr(), served, nil))
	var buf bytes.Buffer
	start := time.Now()
	startRateLimit(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	// The bound ends it, not a read timeout (PoolDialTimeout).
	if d := time.Since(start); d > ratelimit.PoolDialTimeout {
		t.Errorf("startup took %v, want about the 200 ms bound", d)
	}

	out := buf.String()
	for _, want := range []string{
		`level=WARN msg="redis pool warm-up incomplete"`,
		"ready=" + strconv.Itoa(served), "want=" + strconv.Itoa(ratelimit.WarmConns),
		"fail_mode=open", "context deadline exceeded",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "redis not reachable") || strings.Contains(out, `msg="redis pool warm"`) {
		t.Errorf("want only the warm-up warning:\n%s", out)
	}
}

// A signal during the warm-up ends it at once instead of after its bound,
// and is logged as an interruption, not a warning.
func TestStartRateLimit_SignalStopsWarmUp(t *testing.T) {
	shortWarmUp(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mr := miniredis.RunT(t)
	// The signal arrives once the warm-up is waiting on Redis.
	cfg := redisConfig(t, servesFirst(t, mr.Addr(), 1, cancel))
	var buf bytes.Buffer
	start := time.Now()
	startRateLimit(ctx, slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	if d := time.Since(start); d >= ratelimit.PoolDialTimeout/2 {
		t.Errorf("startup took %v after the signal, want it to stop at once", d)
	}
	out := buf.String()
	if !strings.Contains(out, `level=INFO msg="startup interrupted" step="redis warm-up"`) {
		t.Errorf("want an INFO interruption for the warm-up:\n%s", out)
	}
	if strings.Contains(out, "level=WARN") {
		t.Errorf("a signal logged as a warning:\n%s", out)
	}
}

// A signal during the startup ping: an interruption, not "redis not
// reachable at startup" (which make bench-gateway rejects), and no
// warm-up after it.
func TestStartRateLimit_SignalDuringPing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mr := miniredis.RunT(t)
	// Nothing is served; the signal arrives once the ping waits on Redis.
	cfg := redisConfig(t, servesFirst(t, mr.Addr(), 0, cancel))
	var buf bytes.Buffer
	startRateLimit(ctx, slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	out := buf.String()
	if !strings.Contains(out, `level=INFO msg="startup interrupted" step="redis ping"`) {
		t.Errorf("want an INFO interruption for the ping:\n%s", out)
	}
	if strings.Contains(out, "level=WARN") || strings.Contains(out, "redis pool warm") {
		t.Errorf("want neither a warning nor a warm-up:\n%s", out)
	}
}

// A pool smaller than WarmConns is warmed to its size: asking WarmUp for
// more would hold the pool and wait out the whole bound.
func TestStartRateLimit_WarmsAtMostPoolSize(t *testing.T) {
	shortWarmUp(t, time.Second)
	mr := miniredis.RunT(t)
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY":       "sk",
		"SENTINEL_API_KEYS":       "k1",
		"SENTINEL_REDIS_URL":      "redis://" + mr.Addr(),
		"SENTINEL_MAX_CONCURRENT": "4",
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cfg.redis.Close() })
	var buf bytes.Buffer
	startRateLimit(context.Background(), slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	if out := buf.String(); !strings.Contains(out, `msg="redis pool warm" ready=4`) {
		t.Errorf("want ready=4 for a pool of 4:\n%s", out)
	}
	if n := mr.CurrentConnectionCount(); n != 4 {
		t.Errorf("Redis has %d connections, want 4", n)
	}
}

// servesFirst returns the address of a proxy to target that serves only
// the first n connections to send anything; later ones are read and never
// answered, and onStall, if not nil, runs when each of them sends. The
// startup ping comes first, so it is always served.
func servesFirst(t *testing.T, target string, n int64, onStall func()) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var count atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 4096)
				m, err := c.Read(buf)
				if err != nil {
					return
				}
				if count.Add(1) > n {
					if onStall != nil {
						onStall()
					}
					io.Copy(io.Discard, c)
					return
				}
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				if _, err := up.Write(buf[:m]); err != nil {
					return
				}
				go func() { io.Copy(up, c); up.Close() }()
				io.Copy(c, up)
			}()
		}
	}()
	return ln.Addr().String()
}
