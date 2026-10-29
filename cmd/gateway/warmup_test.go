package main

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/shahardalva/sentinel/internal/ratelimit"
)

// redisConfig returns the gateway's config for a Redis at addr, with the
// default SENTINEL_MAX_CONCURRENT, so the pool keeps MinIdleConns warm.
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

// Startup sets up the pool's idle connections before serving, and says so.
func TestLogRateLimit_WarmsPool(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := redisConfig(t, mr.Addr())
	var buf bytes.Buffer
	logRateLimit(slog.New(slog.NewTextHandler(&buf, nil)), cfg)

	out := buf.String()
	want := "ready=" + strconv.Itoa(ratelimit.MinIdleConns)
	if !strings.Contains(out, `msg="redis pool warm"`) || !strings.Contains(out, want) {
		t.Errorf("log missing the warm-up line with %s:\n%s", want, out)
	}
	if strings.Contains(out, "level=WARN") {
		t.Errorf("warning on a healthy Redis:\n%s", out)
	}
	if n := mr.CurrentConnectionCount(); n < ratelimit.MinIdleConns {
		t.Errorf("Redis has %d connections after startup, want at least %d", n, ratelimit.MinIdleConns)
	}
}

// A warm-up that can't finish is a warning with the count, and startup
// goes on: the fail mode covers the rest.
func TestLogRateLimit_WarmUpIncomplete(t *testing.T) {
	const served = 4
	mr := miniredis.RunT(t)
	cfg := redisConfig(t, servesFirst(t, mr.Addr(), served))
	var buf bytes.Buffer
	logRateLimit(slog.New(slog.NewTextHandler(&buf, nil)), cfg)

	out := buf.String()
	for _, want := range []string{
		`level=WARN msg="redis pool warm-up incomplete"`,
		"ready=" + strconv.Itoa(served), "want=" + strconv.Itoa(ratelimit.MinIdleConns), "fail_mode=open",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "redis not reachable") || strings.Contains(out, `msg="redis pool warm"`) {
		t.Errorf("want only the warm-up warning:\n%s", out)
	}
}

// servesFirst returns the address of a proxy to target that serves only
// the first n connections to send anything; later ones are read and never
// answered. Connections the pool dials but doesn't use yet send nothing,
// so the startup ping is always served.
func servesFirst(t *testing.T, target string, n int64) string {
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
