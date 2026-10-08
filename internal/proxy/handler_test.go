package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

const (
	reqBody     = `{"model":"claude-sonnet-5-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	upstreamKey = "upstream-secret"
	gatewayKey  = "gateway-client-secret"
)

// newGateway wires the authenticated handler to an Anthropic provider pointed
// at upstreamURL and captures log output.
func newGateway(t *testing.T, upstreamURL string, opts proxy.Options) (http.Handler, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	p := anthropic.New(anthropic.Config{APIKey: upstreamKey, BaseURL: upstreamURL})
	mux := http.NewServeMux()
	mux.Handle("POST /v1/messages", proxy.RequireKey([]string{gatewayKey}, logger, proxy.NewHandler(p, logger, opts)))
	return mux, &logs
}

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("X-Api-Key", gatewayKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func logLine(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(logs.Bytes(), &m); err != nil {
		t.Fatalf("parse log %q: %v", logs.String(), err)
	}
	return m
}

func errorType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Type  string `json:"type"`
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Type != "error" {
		t.Fatalf("not an error body: %q", rec.Body.String())
	}
	return e.Error.Type
}

func TestHandler_PassesResponseThrough(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		header map[string]string
	}{
		{"success", http.StatusOK, `{"id":"msg_1","type":"message"}`, map[string]string{"Request-Id": "req_up_1"}},
		{"upstream error", http.StatusTooManyRequests, `{"type":"error","error":{"type":"rate_limit_error"}}`, map[string]string{"Retry-After": "7"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotBody []byte
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotBody, _ = io.ReadAll(r.Body)
				for k, v := range tt.header {
					w.Header().Set(k, v)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				io.WriteString(w, tt.body)
			}))
			defer upstream.Close()

			gw, logs := newGateway(t, upstream.URL, proxy.Options{})
			rec := post(gw, reqBody)

			if string(gotBody) != reqBody {
				t.Errorf("upstream body = %q, want %q", gotBody, reqBody)
			}
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d", rec.Code, tt.status)
			}
			if rec.Body.String() != tt.body {
				t.Errorf("body = %q, want %q", rec.Body.String(), tt.body)
			}
			for k, v := range tt.header {
				if got := rec.Header().Get(k); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
			reqID := rec.Header().Get("X-Request-Id")
			if reqID == "" {
				t.Fatal("missing X-Request-Id")
			}

			line := logLine(t, logs)
			if line["request_id"] != reqID {
				t.Errorf("log request_id = %v, want %q", line["request_id"], reqID)
			}
			if line["model"] != "claude-sonnet-5-5" {
				t.Errorf("log model = %v", line["model"])
			}
			if line["status"] != float64(tt.status) {
				t.Errorf("log status = %v", line["status"])
			}
			for _, k := range []string{"latency_ms", "read_body_ms", "upstream_ttfb_ms", "stream_ms"} {
				if _, ok := line[k].(float64); !ok {
					t.Errorf("log %s missing: %v", k, line)
				}
			}
			if line["level"] != "INFO" || line["error"] != nil || line["phase"] != nil {
				t.Errorf("log = %v, want INFO without error or phase", line)
			}
		})
	}
}

func TestHandler_StreamsEventsAsTheyArrive(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: one\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "event: two\ndata: {}\n\n")
	}))
	defer upstream.Close()

	gw, _ := newGateway(t, upstream.URL, proxy.Options{})
	srv := httptest.NewServer(gw)
	defer srv.Close()

	// Cancelling the client request on exit unblocks both servers if the test
	// fails while the stream is still open.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Api-Key", gatewayKey)

	first := make(chan string, 1)
	rest := make(chan string, 1)
	go func() {
		resp, err := srv.Client().Do(req)
		if err != nil {
			first <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)
		line, _ := br.ReadString('\n')
		first <- line
		b, _ := io.ReadAll(br)
		rest <- string(b)
	}()

	select {
	case line := <-first:
		if line != "event: one\n" {
			t.Fatalf("first line = %q, want %q", line, "event: one\n")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first event not delivered while the upstream stream was still open")
	}
	close(release)
	if got := <-rest; !strings.Contains(got, "event: two\n") {
		t.Errorf("rest of stream = %q, want event two", got)
	}
}

func TestHandler_UpstreamUnreachable(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	url := upstream.URL
	upstream.Close()

	gw, logs := newGateway(t, url, proxy.Options{})
	rec := post(gw, reqBody)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if typ := errorType(t, rec); typ != "api_error" {
		t.Errorf("error type = %q", typ)
	}
	line := logLine(t, logs)
	if line["level"] != "ERROR" || line["phase"] != "upstream" {
		t.Errorf("log = %v, want ERROR in phase upstream", line)
	}
	if msg, _ := line["error"].(string); !strings.HasPrefix(msg, "anthropic: forward: ") {
		t.Errorf("log error = %q, want provider-wrapped error", msg)
	}
	if _, ok := line["stream_ms"]; ok {
		t.Errorf("stream_ms logged for a request that never streamed: %v", line)
	}
}

func TestHandler_MidStreamFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: one\n\n")
		w.(http.Flusher).Flush()
		// Drop the connection without ending the chunked body.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		conn.Close()
	}))
	defer upstream.Close()

	gw, logs := newGateway(t, upstream.URL, proxy.Options{})
	rec := post(gw, reqBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (sent before the failure)", rec.Code)
	}
	line := logLine(t, logs)
	if line["level"] != "ERROR" || line["phase"] != "stream" || line["error"] == nil {
		t.Errorf("log = %v, want ERROR in phase stream with error", line)
	}
	if line["status"] != float64(http.StatusOK) {
		t.Errorf("log status = %v, want 200", line["status"])
	}
}

func TestHandler_ClientDisconnect(t *testing.T) {
	started := make(chan struct{})
	upstreamCanceled := make(chan struct{})
	stop := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// net/http only watches for the peer closing once the body is consumed.
		io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
			close(upstreamCanceled)
		case <-stop: // test failed; let upstream.Close return
		}
	}))
	defer upstream.Close()
	defer close(stop)

	gw, logs := newGateway(t, upstream.URL, proxy.Options{})
	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("X-Api-Key", gatewayKey)

	done := make(chan struct{})
	go func() {
		gw.ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-started
	cancel() // the client goes away while waiting on upstream

	select {
	case <-upstreamCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream request was not cancelled")
	}
	<-done

	line := logLine(t, logs)
	if line["status"] != float64(499) || line["level"] != "WARN" || line["phase"] != "upstream" {
		t.Errorf("log = %v, want status 499, WARN, phase upstream", line)
	}
}

func TestHandler_BodyReadError(t *testing.T) {
	gw, logs := newGateway(t, "http://unused", proxy.Options{})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", iotest.ErrReader(errors.New("connection reset")))
	req.Header.Set("X-Api-Key", gatewayKey)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if typ := errorType(t, rec); typ != "invalid_request_error" {
		t.Errorf("error type = %q", typ)
	}
	line := logLine(t, logs)
	if line["level"] != "WARN" || line["phase"] != "read_body" {
		t.Errorf("log = %v, want WARN in phase read_body", line)
	}
	if _, ok := line["upstream_ttfb_ms"]; ok {
		t.Errorf("upstream_ttfb_ms logged for a request never sent upstream: %v", line)
	}
}

func TestHandler_UpstreamTimeout(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer upstream.Close()
	defer close(release) // runs first, so Close doesn't wait on the stuck handler

	gw, _ := newGateway(t, upstream.URL, proxy.Options{HeaderTimeout: 50 * time.Millisecond})
	rec := post(gw, reqBody)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if typ := errorType(t, rec); typ != "timeout_error" {
		t.Errorf("error type = %q", typ)
	}
}

func TestHandler_LongActiveStreamCompletes(t *testing.T) {
	// Runs ~4x longer than either timeout; must not be cut while data flows.
	const events = 20
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := range events {
			fmt.Fprintf(w, "event: e%d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	gw, logs := newGateway(t, upstream.URL, proxy.Options{
		HeaderTimeout: 50 * time.Millisecond,
		IdleTimeout:   50 * time.Millisecond,
	})
	rec := post(gw, reqBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if want := fmt.Sprintf("event: e%d\n", events-1); !strings.Contains(rec.Body.String(), want) {
		t.Errorf("stream cut short: %q", rec.Body.String())
	}
	if line := logLine(t, logs); line["error"] != nil {
		t.Errorf("log error = %v, want none", line["error"])
	}
}

func TestHandler_StalledStreamAborted(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: one\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done() // stall until the gateway gives up
	}))
	defer upstream.Close()

	gw, logs := newGateway(t, upstream.URL, proxy.Options{IdleTimeout: 50 * time.Millisecond})

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- post(gw, reqBody) }()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stalled stream was not aborted")
	}

	if rec.Body.String() != "event: one\n\n" {
		t.Errorf("body = %q, want only the first event", rec.Body.String())
	}
	if line := logLine(t, logs); !strings.Contains(fmt.Sprint(line["error"]), "idle") {
		t.Errorf("log error = %v, want idle timeout", line["error"])
	}
}

func TestHandler_SlowRequestBody(t *testing.T) {
	gw, _ := newGateway(t, "http://unused", proxy.Options{BodyTimeout: 50 * time.Millisecond})
	srv := httptest.NewServer(gw)
	defer srv.Close()

	// The client sends part of the body, then nothing.
	pr, pw := io.Pipe()
	defer pw.Close()
	go io.WriteString(pw, `{"model":`)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/messages", pr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Api-Key", gatewayKey)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.StatusCode)
	}
}

func TestHandler_BodyTooLarge(t *testing.T) {
	called := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer upstream.Close()

	gw, _ := newGateway(t, upstream.URL, proxy.Options{MaxBodyBytes: 10})
	rec := post(gw, reqBody)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if typ := errorType(t, rec); typ != "request_too_large" {
		t.Errorf("error type = %q", typ)
	}
	if called {
		t.Error("upstream was called")
	}
}

func TestHandler_ConcurrencyCap(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	defer close(release) // runs first if the test fails before releasing

	gw, logs := newGateway(t, upstream.URL, proxy.Options{MaxConcurrent: 1})

	first := make(chan int, 1)
	go func() { first <- post(gw, reqBody).Code }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached upstream")
	}

	rec := post(gw, reqBody) // the only slot is taken
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if typ := errorType(t, rec); typ != "overloaded_error" {
		t.Errorf("error type = %q", typ)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After")
	}
	if !strings.Contains(logs.String(), `"msg":"overloaded"`) {
		t.Errorf("no overloaded log line:\n%s", logs)
	}

	release <- struct{}{}
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", code)
	}

	// The slot is freed once a request finishes.
	go func() { <-started; release <- struct{}{} }()
	if code := post(gw, reqBody).Code; code != http.StatusOK {
		t.Errorf("request after release: status = %d, want 200", code)
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	gw, _ := newGateway(t, "http://unused", proxy.Options{})
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/messages", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHandler_FiltersResponseHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Request-Id", "req_up_1")
		h.Set("Anthropic-Ratelimit-Requests-Remaining", "49")
		h.Set("Anthropic-Organization-Id", "org-secret")
		h.Set("Set-Cookie", "__cf_bm=abc")
		h.Set("Cf-Ray", "8f00-TLV")
		h.Set("Server", "cloudflare")
		h.Set("X-Request-Id", "spoofed")
		io.WriteString(w, "{}")
	}))
	defer upstream.Close()

	gw, _ := newGateway(t, upstream.URL, proxy.Options{})
	rec := post(gw, reqBody)

	for _, k := range []string{"Content-Type", "Request-Id", "Anthropic-Ratelimit-Requests-Remaining"} {
		if rec.Header().Get(k) == "" {
			t.Errorf("allowed header %s dropped", k)
		}
	}
	for _, k := range []string{"Anthropic-Organization-Id", "Set-Cookie", "Cf-Ray", "Server"} {
		if v := rec.Header().Get(k); v != "" {
			t.Errorf("header %s forwarded: %q", k, v)
		}
	}
	if v := rec.Header().Values("X-Request-Id"); len(v) != 1 || v[0] == "spoofed" {
		t.Errorf("X-Request-Id = %v, want gateway-generated id", v)
	}
}

// stubProvider shows the handler depends only on the Provider interface.
type stubProvider struct {
	resp  *http.Response
	allow func(string) bool
}

func (s stubProvider) Name() string { return "stub" }
func (s stubProvider) Forward(context.Context, *proxy.Request) (*http.Response, error) {
	return s.resp, nil
}
func (s stubProvider) AllowResponseHeader(name string) bool { return s.allow(name) }

func TestHandler_UsesProviderHeaderAllowlist(t *testing.T) {
	h := http.Header{}
	h.Set("X-Kept", "yes")
	h.Set("X-Dropped", "no")
	h.Set("X-Request-Id", "spoofed")
	p := stubProvider{
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     h,
			Body:       io.NopCloser(strings.NewReader("ok")),
		},
		// Even a provider that allows X-Request-Id can't override the gateway's.
		allow: func(name string) bool { return name == "X-Kept" || name == "X-Request-Id" },
	}
	handler := proxy.NewHandler(p, slog.New(slog.DiscardHandler), proxy.Options{})

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader("{}")))

	if rec.Header().Get("X-Kept") != "yes" {
		t.Error("allowed header dropped")
	}
	if v := rec.Header().Get("X-Dropped"); v != "" {
		t.Errorf("disallowed header forwarded: %q", v)
	}
	if v := rec.Header().Values("X-Request-Id"); len(v) != 1 || v[0] == "spoofed" {
		t.Errorf("X-Request-Id = %v, want gateway-generated id", v)
	}
}

func TestHandler_TruncatesLoggedModel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	// 127 ASCII bytes then a 3-byte rune that straddles the 128-byte cap.
	long := strings.Repeat("a", 127) + "€" + strings.Repeat("b", 1<<20)
	gw, logs := newGateway(t, upstream.URL, proxy.Options{})
	post(gw, `{"model":"`+long+`"}`)

	model, _ := logLine(t, logs)["model"].(string)
	if want := strings.Repeat("a", 127); model != want {
		t.Errorf("logged model = %d bytes %q..., want the 127-byte prefix", len(model), model[:min(len(model), 10)])
	}
}
