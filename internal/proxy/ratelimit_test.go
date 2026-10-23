package proxy_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
	"github.com/shahardalva/sentinel/internal/ratelimit"
)

const otherKey = "second-client-secret"

// redisT0 is the frozen Redis clock, so token counts and waits are exact.
var redisT0 = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

// rateLimited is a gateway whose handler is rate limited through a real
// ratelimit.Limiter, and an upstream that counts the requests reaching it.
type rateLimited struct {
	gw    http.Handler
	logs  *bytes.Buffer
	mr    *miniredis.Miniredis
	calls *atomic.Int64
}

// newRateLimited builds a rateLimited gateway allowing perMinute requests
// per key, with the Redis clock frozen. redisURL overrides the miniredis
// address, for a Redis that misbehaves.
func newRateLimited(t *testing.T, perMinute int, failClosed bool, redisURL string, timeout time.Duration) *rateLimited {
	t.Helper()
	calls := new(atomic.Int64)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1","type":"message"}`)
	}))
	t.Cleanup(upstream.Close)

	mr := miniredis.RunT(t)
	mr.SetTime(redisT0)
	if redisURL == "" {
		redisURL = "redis://" + mr.Addr()
	}
	client, err := ratelimit.NewClient(redisURL, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	limiter, err := ratelimit.New(ratelimit.Config{Client: client, PerMinute: perMinute, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	p, err := anthropic.New(anthropic.Config{APIKey: upstreamKey, BaseURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	h := proxy.NewHandler(p, logger, proxy.Options{RateLimiter: limiter, RateLimitFailClosed: failClosed})
	mux := http.NewServeMux()
	mux.Handle("POST /v1/messages", proxy.RequireKey([]string{gatewayKey, otherKey}, logger, h))
	return &rateLimited{gw: mux, logs: &logs, mr: mr, calls: calls}
}

func postKey(h http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("X-Api-Key", key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// logLines parses every JSON log line in logs.
func logLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("parse log line %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	return lines
}

func lastLog(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	lines := logLines(t, logs)
	return lines[len(lines)-1]
}

func TestRateLimit_UnderLimit(t *testing.T) {
	rl := newRateLimited(t, 3, false, "", time.Second)
	for want := 2.0; want >= 0; want-- {
		rec := postKey(rl.gw, gatewayKey)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		line := lastLog(t, rl.logs)
		if line["rate_limit_remaining"] != want {
			t.Errorf("log rate_limit_remaining = %v, want %v", line["rate_limit_remaining"], want)
		}
		if _, ok := line["rate_limit_us"]; !ok {
			t.Error("log missing rate_limit_us")
		}
	}
}

func TestRateLimit_OverLimit(t *testing.T) {
	rl := newRateLimited(t, 2, false, "", time.Second) // one token per 30s
	postKey(rl.gw, gatewayKey)
	postKey(rl.gw, gatewayKey)

	rec := postKey(rl.gw, gatewayKey)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if typ := errorType(t, rec); typ != "rate_limit_error" {
		t.Errorf("error type = %q, want rate_limit_error", typ)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After = %q, want 30", ra)
	}
	if c := rec.Header().Get("Connection"); c != "close" {
		t.Errorf("Connection = %q, want close", c)
	}
	if n := rl.calls.Load(); n != 2 {
		t.Errorf("upstream calls = %d, want 2", n)
	}
	line := lastLog(t, rl.logs)
	if line["status"] != float64(429) || line["level"] != "WARN" || line["phase"] != "rate_limit" ||
		line["rate_limit_remaining"] != 0.0 {
		t.Errorf("log = %v, want WARN 429 in phase rate_limit with 0 remaining", line)
	}
}

func TestRateLimit_Refills(t *testing.T) {
	rl := newRateLimited(t, 60, false, "", time.Second)
	for range 60 {
		postKey(rl.gw, gatewayKey)
	}
	if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	rl.mr.SetTime(redisT0.Add(2 * time.Second)) // two tokens
	for i := range 2 {
		if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusOK {
			t.Fatalf("request %d after refill: status = %d, want 200", i, rec.Code)
		}
	}
	if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusTooManyRequests {
		t.Errorf("third request after 2s: status = %d, want 429", rec.Code)
	}
}

func TestRateLimit_SeparateKeys(t *testing.T) {
	rl := newRateLimited(t, 1, false, "", time.Second)
	if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusOK {
		t.Fatalf("first key: status = %d, want 200", rec.Code)
	}
	if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("first key again: status = %d, want 429", rec.Code)
	}
	if rec := postKey(rl.gw, otherKey); rec.Code != http.StatusOK {
		t.Errorf("second key: status = %d, want 200 from its own bucket", rec.Code)
	}
}

func TestRateLimit_RedisKeyIsHashed(t *testing.T) {
	rl := newRateLimited(t, 60, false, "", time.Second)
	postKey(rl.gw, gatewayKey)
	sum := sha256.Sum256([]byte(gatewayKey))
	want := "sentinel:ratelimit:" + hex.EncodeToString(sum[:])
	keys := rl.mr.Keys()
	if len(keys) != 1 || keys[0] != want {
		t.Fatalf("Redis keys = %q, want [%q]", keys, want)
	}
	if strings.Contains(rl.mr.Dump(), gatewayKey) {
		t.Error("raw client key stored in Redis")
	}
}

// A limited request is answered before its body is read: with only part of
// the body sent, the client still gets its 429 and the connection closes.
func TestRateLimit_RejectsBeforeReadingBody(t *testing.T) {
	rl := newRateLimited(t, 1, false, "", time.Second)
	postKey(rl.gw, gatewayKey)
	srv := httptest.NewServer(rl.gw)
	defer srv.Close()
	resp := sendPartial(t, srv, gatewayKey, 1<<20)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
}

func TestRateLimit_RedisDownFailOpen(t *testing.T) {
	rl := newRateLimited(t, 60, false, "", time.Second)
	rl.mr.Close()
	for range 3 {
		if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (fail open)", rec.Code)
		}
	}
	if n := rl.calls.Load(); n != 3 {
		t.Errorf("upstream calls = %d, want 3", n)
	}
	var warns int
	for _, line := range logLines(t, rl.logs) {
		if strings.HasPrefix(line["msg"].(string), "rate limiter unavailable") {
			warns++
			if line["level"] != "WARN" {
				t.Errorf("limiter warning level = %v, want WARN", line["level"])
			}
		}
		if line["msg"] == "request" {
			if _, ok := line["rate_limit_remaining"]; ok {
				t.Errorf("request log has rate_limit_remaining with no decision: %v", line)
			}
		}
	}
	if warns != 1 {
		t.Errorf("limiter warnings = %d, want 1 for three failures in a row", warns)
	}
}

func TestRateLimit_RedisDownFailClosed(t *testing.T) {
	rl := newRateLimited(t, 60, true, "", time.Second)
	rl.mr.Close()
	rec := postKey(rl.gw, gatewayKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (fail closed)", rec.Code)
	}
	if typ := errorType(t, rec); typ != "api_error" {
		t.Errorf("error type = %q, want api_error", typ)
	}
	if rec.Header().Get("Retry-After") == "" || rec.Header().Get("Connection") != "close" {
		t.Errorf("headers = %v, want Retry-After and Connection: close", rec.Header())
	}
	if n := rl.calls.Load(); n != 0 {
		t.Errorf("upstream calls = %d, want 0", n)
	}
	line := lastLog(t, rl.logs)
	if line["level"] != "ERROR" || line["phase"] != "rate_limit" ||
		!strings.HasPrefix(line["error"].(string), "rate limiter unavailable") {
		t.Errorf("log = %v, want ERROR in phase rate_limit", line)
	}
}

// stalledRedis accepts connections but never answers.
func stalledRedis(t *testing.T) string {
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
	return "redis://" + ln.Addr().String()
}

func TestRateLimit_SlowRedisTimesOut(t *testing.T) {
	const timeout = 100 * time.Millisecond
	for _, failClosed := range []bool{false, true} {
		rl := newRateLimited(t, 60, failClosed, stalledRedis(t), timeout)
		start := time.Now()
		rec := postKey(rl.gw, gatewayKey)
		elapsed := time.Since(start)
		want := http.StatusOK
		if failClosed {
			want = http.StatusServiceUnavailable
		}
		if rec.Code != want {
			t.Errorf("failClosed=%v: status = %d, want %d", failClosed, rec.Code, want)
		}
		if elapsed > 10*timeout {
			t.Errorf("failClosed=%v: request took %v with a %v Redis timeout", failClosed, elapsed, timeout)
		}
		line := lastLog(t, rl.logs)
		if us, _ := line["rate_limit_us"].(float64); us < float64(timeout.Microseconds()) {
			t.Errorf("failClosed=%v: rate_limit_us = %v, want at least the timeout", failClosed, line["rate_limit_us"])
		}
	}
}

// Once Redis has failed BreakerThreshold times in a row, the fail mode
// applies without waiting on Redis.
func TestRateLimit_OpenBreakerSkipsRedis(t *testing.T) {
	const timeout = 100 * time.Millisecond
	rl := newRateLimited(t, 60, true, stalledRedis(t), timeout)
	for i := range ratelimit.BreakerThreshold {
		if rec := postKey(rl.gw, gatewayKey); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status = %d, want 503", i+1, rec.Code)
		}
	}
	start := time.Now()
	rec := postKey(rl.gw, gatewayKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if d := time.Since(start); d > timeout/2 {
		t.Errorf("request took %v with the breaker open; it should skip Redis", d)
	}
	line := lastLog(t, rl.logs)
	if !strings.Contains(line["error"].(string), "circuit open") {
		t.Errorf("log error = %v, want it to mention the open circuit", line["error"])
	}
}
