package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseMetricsAddr(t *testing.T) {
	const api = "127.0.0.1:8081"
	tests := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "", want: defaultMetricsAddr},
		{in: "off", want: ""},
		{in: " off ", want: ""},
		{in: "127.0.0.1:9091", want: "127.0.0.1:9091"},
		{in: "0.0.0.0:9091", want: "0.0.0.0:9091"},
		{in: ":9100", want: ":9100"},
		{in: "[::1]:9091", want: "[::1]:9091"},
		{in: "127.0.0.1:0", want: "127.0.0.1:0"},
		{in: "OFF", wantErr: true},
		{in: "false", wantErr: true},
		{in: "localhost", wantErr: true},
		{in: "127.0.0.1:metrics", wantErr: true},
		{in: "127.0.0.1:70000", wantErr: true},
		{in: "http://127.0.0.1:9091", wantErr: true},
		{in: "127.0.0.1:8081", wantErr: true}, // the API's port
		{in: "0.0.0.0:8081", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseMetricsAddr(tt.in, api)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("parseMetricsAddr(%q) = %q, %v; want %q, error %v", tt.in, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestLoadConfig_MetricsAddr(t *testing.T) {
	base := map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}
	with := func(v string) map[string]string {
		m := map[string]string{"SENTINEL_METRICS_ADDR": v}
		for k, val := range base {
			m[k] = val
		}
		return m
	}
	cfg, err := loadConfig(env(base))
	if err != nil || cfg.metricsAddr != "127.0.0.1:9091" {
		t.Errorf("default: metricsAddr %q, %v; want 127.0.0.1:9091", cfg.metricsAddr, err)
	}
	if cfg, err := loadConfig(env(with("off"))); err != nil || cfg.metricsAddr != "" {
		t.Errorf("off: metricsAddr %q, %v; want empty", cfg.metricsAddr, err)
	}
	wantConfigErr(t, with("localhost"), "SENTINEL_METRICS_ADDR")
	wantConfigErr(t, with("127.0.0.1:8081"), "SENTINEL_METRICS_ADDR")
}

// syncBuffer is a bytes.Buffer safe for a logger writing from the servers'
// goroutines while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the JSON log lines written so far.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// running is a gateway started by startServe.
type running struct {
	logs        *syncBuffer
	api         string // host:port of the API listener
	metrics     string // host:port of the metrics listener; empty if off
	done        chan error
	stop        context.CancelFunc
	msgsInOrder []string
}

// startServe runs serve for vars, with both listeners on free ports unless
// vars says otherwise, and waits for the API's "listening" line.
func startServe(t *testing.T, vars map[string]string) *running {
	t.Helper()
	all := map[string]string{
		"ANTHROPIC_API_KEY":     "sk",
		"ANTHROPIC_BASE_URL":    "http://127.0.0.1:1", // never reached
		"SENTINEL_API_KEYS":     "client-key",
		"SENTINEL_ADDR":         "127.0.0.1:0",
		"SENTINEL_METRICS_ADDR": "127.0.0.1:0",
	}
	for k, v := range vars {
		all[k] = v
	}
	cfg, err := loadConfig(env(all))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	r := &running{logs: &syncBuffer{}, done: make(chan error, 1), stop: stop}
	go func() { r.done <- serve(ctx, slog.New(slog.NewJSONHandler(r.logs, nil)), cfg) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
			t.Error("serve did not return after its context was cancelled")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-r.done:
			t.Fatalf("serve returned early: %v", err)
		default:
		}
		r.msgsInOrder = nil
		for _, line := range r.logs.lines(t) {
			msg, _ := line["msg"].(string)
			r.msgsInOrder = append(r.msgsInOrder, msg)
			addr, _ := line["addr"].(string)
			switch msg {
			case "metrics listening":
				r.metrics = addr
			case "listening":
				r.api = addr
			}
		}
		if r.api != "" {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(`serve never logged "listening"`)
	return nil
}

func get(t *testing.T, method, url string, header http.Header) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestServe_MetricsOnTheirOwnListener(t *testing.T) {
	r := startServe(t, nil)
	if r.metrics == "" || r.metrics == r.api {
		t.Fatalf("metrics addr %q, API addr %q; want two listeners", r.metrics, r.api)
	}
	// The metrics listener binds first, so "listening" is the last word on
	// startup, as make bench-gateway expects.
	mi, ai := -1, -1
	for i, msg := range r.msgsInOrder {
		switch msg {
		case "metrics listening":
			mi = i
		case "listening":
			ai = i
		}
	}
	if mi < 0 || ai < mi {
		t.Errorf("startup log order %v: want \"metrics listening\" before \"listening\"", r.msgsInOrder)
	}

	// A bad key, so the scrape below has something to count.
	if code, _ := get(t, http.MethodPost, "http://"+r.api+"/v1/messages", http.Header{"X-Api-Key": {"wrong"}}); code != http.StatusUnauthorized {
		t.Fatalf("API with a bad key: %d, want 401", code)
	}

	code, body := get(t, http.MethodGet, "http://"+r.metrics+"/metrics", nil)
	if code != http.StatusOK {
		t.Fatalf("GET /metrics on the metrics listener: %d, want 200", code)
	}
	for _, want := range []string{
		"sentinel_auth_rejections_total{",
		"sentinel_admission_limit{",
		"sentinel_admission_in_use{",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape has no %s:\n%s", want, body)
		}
	}
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "sentinel_auth_rejections_total{") && !strings.HasSuffix(line, "} 1") {
			t.Errorf("auth rejections: %s, want 1", line)
		}
	}
	if strings.Contains(body, "sentinel_ratelimit_breaker_open") {
		t.Error("breaker gauge served with rate limiting off")
	}

	// Never on the API port, and nothing else on the metrics port.
	if code, _ := get(t, http.MethodGet, "http://"+r.api+"/metrics", nil); code != http.StatusNotFound {
		t.Errorf("GET /metrics on the API port: %d, want 404", code)
	}
	if code, _ := get(t, http.MethodGet, "http://"+r.metrics+"/v1/messages", nil); code != http.StatusNotFound {
		t.Errorf("GET /v1/messages on the metrics port: %d, want 404", code)
	}
	if code, _ := get(t, http.MethodPost, "http://"+r.metrics+"/metrics", nil); code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics: %d, want 405", code)
	}

	r.stop()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("serve after shutdown = %v, want nil", err)
		}
		r.done <- err // for the cleanup
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
	if _, err := net.DialTimeout("tcp", r.metrics, time.Second); err == nil {
		t.Error("metrics listener still accepting after shutdown")
	}
}

func TestServe_MetricsOff(t *testing.T) {
	r := startServe(t, map[string]string{"SENTINEL_METRICS_ADDR": "off"})
	if r.metrics != "" {
		t.Fatalf("metrics listening on %s with SENTINEL_METRICS_ADDR=off", r.metrics)
	}
	var disabled bool
	for _, msg := range r.msgsInOrder {
		disabled = disabled || msg == "metrics disabled: SENTINEL_METRICS_ADDR is off"
	}
	if !disabled {
		t.Errorf("no metrics disabled line in %v", r.msgsInOrder)
	}
	if code, _ := get(t, http.MethodGet, "http://"+r.api+"/metrics", nil); code != http.StatusNotFound {
		t.Errorf("GET /metrics on the API port: %d, want 404", code)
	}
	// The API still works without a Recorder.
	if code, _ := get(t, http.MethodPost, "http://"+r.api+"/v1/messages", http.Header{"X-Api-Key": {"wrong"}}); code != http.StatusUnauthorized {
		t.Errorf("API with a bad key: %d, want 401", code)
	}
}

// make bench-gateway greps the JSON log for "msg":"listening" to know the
// API port is up; the metrics line must never match it.
func TestListenMetrics(t *testing.T) {
	var buf bytes.Buffer
	ln, err := listenMetrics(slog.New(slog.NewJSONHandler(&buf, nil)), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	out := buf.String()
	if !strings.Contains(out, `"msg":"metrics listening","addr":"`+ln.Addr().String()+`"`) {
		t.Errorf("log = %q, want a metrics listening line with the bound addr", out)
	}
	if strings.Contains(out, `"msg":"listening"`) {
		t.Errorf("metrics line %q matches make bench-gateway's grep for the API", out)
	}
}

// A metrics address that can't be bound stops startup before the API
// listens.
func TestServe_MetricsBindFailureStopsStartup(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY":     "sk",
		"SENTINEL_API_KEYS":     "k",
		"SENTINEL_ADDR":         "127.0.0.1:0",
		"SENTINEL_METRICS_ADDR": taken.Addr().String(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	done := make(chan error, 1)
	go func() { done <- serve(context.Background(), slog.New(slog.NewJSONHandler(logs, nil)), cfg) }()
	select {
	case err := <-done:
		if err == nil || !strings.HasPrefix(err.Error(), "SENTINEL_METRICS_ADDR") {
			t.Errorf("serve = %v, want an error naming SENTINEL_METRICS_ADDR", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve kept running with its metrics address taken")
	}
	for _, line := range logs.lines(t) {
		if m := line["msg"]; m == "listening" || m == "metrics listening" {
			t.Errorf("logged %q though the metrics bind failed", m)
		}
	}
}
