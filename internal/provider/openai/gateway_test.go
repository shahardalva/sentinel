package openai_test

// These tests run the OpenAI provider behind proxy.RequireKey and
// proxy.NewHandler, as cmd/gateway wires it, from outside package proxy.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/provider/openai"
	"github.com/shahardalva/sentinel/internal/proxy"
)

const (
	reqBody     = `{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}`
	upstreamKey = "upstream-secret"
	gatewayKey  = "gateway-client-secret"
	// For tests that never reach upstream; loopback, so it passes validation.
	unusedUpstream = "http://127.0.0.1:1/v1"
)

// newGateway wires the authenticated handler to an OpenAI provider pointed
// at upstreamURL, and logs JSON to logs.
func newGateway(t *testing.T, upstreamURL string, opts proxy.Options, logs io.Writer) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	p, err := openai.New(openai.Config{APIKey: upstreamKey, BaseURL: upstreamURL})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat/completions", proxy.RequireKey([]string{gatewayKey}, p, logger, proxy.NewHandler(p, logger, opts)))
	return mux
}

// post sends reqBody with key as an OpenAI SDK does, as a bearer token.
func post(h http.Handler, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGateway_StreamsChunksAsTheyArrive(t *testing.T) {
	const first = `data: {"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hel"}}]}`
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, first+"\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `data: {"object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`+"\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	srv := httptest.NewServer(newGateway(t, upstream.URL+"/v1", proxy.Options{}, io.Discard))
	defer srv.Close()

	// Cancelling the client request on exit unblocks both servers if the
	// test fails while the stream is still open.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+gatewayKey)

	firstLine := make(chan string, 1)
	rest := make(chan string, 1)
	go func() {
		resp, err := srv.Client().Do(req)
		if err != nil {
			firstLine <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)
		line, _ := br.ReadString('\n')
		firstLine <- line
		b, _ := io.ReadAll(br)
		rest <- string(b)
	}()

	select {
	case line := <-firstLine:
		if line != first+"\n" {
			t.Fatalf("first line = %q, want %q", line, first+"\n")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk not delivered while the upstream stream was still open")
	}
	close(release)
	if got := <-rest; !strings.HasSuffix(got, "data: [DONE]\n\n") {
		t.Errorf("rest of stream = %q, want it to end with data: [DONE]", got)
	}
}

func TestGateway_RequestIDs(t *testing.T) {
	gotAuth := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth <- r.Header.Get("Authorization")
		w.Header().Set("X-Request-Id", "req_upstream_1")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"chat.completion"}`)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	rec := post(newGateway(t, upstream.URL+"/v1", proxy.Options{}, &logs), gatewayKey)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"object":"chat.completion"}` {
		t.Fatalf("response = %d %q", rec.Code, rec.Body)
	}
	if auth := <-gotAuth; auth != "Bearer "+upstreamKey {
		t.Errorf("upstream Authorization = %q, want the gateway's upstream key", auth)
	}

	ids := rec.Header().Values("X-Request-Id")
	if len(ids) != 1 || ids[0] == "" || ids[0] == "req_upstream_1" {
		t.Fatalf("X-Request-Id = %q, want only the gateway's own", ids)
	}
	var line map[string]any
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("parse log %q: %v", logs.String(), err)
	}
	if line["request_id"] != ids[0] || line["upstream_request_id"] != "req_upstream_1" ||
		line["provider"] != "openai" || line["model"] != "gpt-test" {
		t.Errorf("log = %v, want request_id %q, upstream_request_id req_upstream_1, provider openai, model gpt-test", line, ids[0])
	}
}

// fixedLimiter is a RateLimiter with a fixed answer.
type fixedLimiter proxy.RateLimit

func (l fixedLimiter) Allow(context.Context, string) (proxy.RateLimit, error) {
	return proxy.RateLimit(l), nil
}

func TestGateway_ErrorBodies(t *testing.T) {
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`)
	}))
	defer rejecting.Close()

	// A full Admission: every request finds the cap reached.
	full := proxy.NewAdmission(1)
	if !full.TryAcquire() {
		t.Fatal("could not take the only slot")
	}
	defer full.Release()

	tests := []struct {
		name       string
		upstream   string
		opts       proxy.Options
		key        string
		status     int
		retryAfter string
		body       string
	}{
		{
			name: "401 wrong key", upstream: unusedUpstream, key: "wrong",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"invalid x-api-key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`,
		},
		{
			name: "429 rate limited", upstream: unusedUpstream, key: gatewayKey,
			opts:   proxy.Options{RateLimiter: fixedLimiter{RetryAfter: 1500 * time.Millisecond}},
			status: http.StatusTooManyRequests, retryAfter: "2",
			body: `{"error":{"message":"rate limit exceeded","type":"requests","param":null,"code":"rate_limit_exceeded"}}`,
		},
		{
			name: "503 overloaded", upstream: unusedUpstream, key: gatewayKey,
			opts:   proxy.Options{Admission: full},
			status: http.StatusServiceUnavailable, retryAfter: "1",
			body: `{"error":{"message":"gateway is at capacity","type":"server_error","param":null,"code":"overloaded"}}`,
		},
		{
			// The upstream's own 401 body would blame the client's key.
			name: "502 upstream rejected the gateway's key", upstream: rejecting.URL + "/v1", key: gatewayKey,
			status: http.StatusBadGateway,
			body:   `{"error":{"message":"upstream authentication failed","type":"server_error","param":null,"code":null}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(newGateway(t, tt.upstream, tt.opts, io.Discard), tt.key)
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d", rec.Code, tt.status)
			}
			if got := rec.Body.String(); got != tt.body+"\n" {
				t.Errorf("body = %q, want %q", got, tt.body+"\n")
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}
			if ra := rec.Header().Get("Retry-After"); ra != tt.retryAfter {
				t.Errorf("Retry-After = %q, want %q", ra, tt.retryAfter)
			}
		})
	}
}
