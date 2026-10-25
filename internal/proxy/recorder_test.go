package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

// fakeRecorder keeps what it is told.
type fakeRecorder struct {
	mu       sync.Mutex
	requests []proxy.RequestInfo
	auth     int
}

func (f *fakeRecorder) RecordRequest(_ context.Context, info proxy.RequestInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, info)
}

func (f *fakeRecorder) RecordAuthRejection(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth++
}

// only returns the one RequestInfo recorded, failing unless there is
// exactly one.
func (f *fakeRecorder) only(t *testing.T) proxy.RequestInfo {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Fatalf("recorded %d requests, want exactly 1: %+v", len(f.requests), f.requests)
	}
	return f.requests[0]
}

// recordedGateway is newGateway with rec as the Recorder of both the
// Handler and the Auth in front of it.
func recordedGateway(t *testing.T, upstreamURL string, opts proxy.Options, rec *fakeRecorder) (http.Handler, *bytes.Buffer) {
	t.Helper()
	p, err := anthropic.New(anthropic.Config{APIKey: upstreamKey, BaseURL: upstreamURL})
	if err != nil {
		t.Fatal(err)
	}
	return recordedGatewayFor(p, opts, rec)
}

// recordedGatewayFor is recordedGateway in front of any Provider.
func recordedGatewayFor(p proxy.Provider, opts proxy.Options, rec *fakeRecorder) (http.Handler, *bytes.Buffer) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	opts.Recorder = rec
	mux := http.NewServeMux()
	mux.Handle("POST /v1/messages", proxy.NewAuth([]string{gatewayKey}, logger, rec).Require(p, proxy.NewHandler(p, logger, opts)))
	return mux, &logs
}

// statusUpstream answers every request with status and an Anthropic error
// body.
func statusUpstream(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, `{"type":"error","error":{"type":"api_error","message":"upstream says no"}}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// hangingUpstream never answers, until the test ends.
func hangingUpstream(t *testing.T) string {
	t.Helper()
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(done); srv.Close() })
	return srv.URL
}

func okUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"msg_1","type":"message"}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

var allowAll = wireLimiter{lim: proxy.RateLimit{Allowed: true, Remaining: 9}}

const allStages = proxy.StageRateLimit | proxy.StageReadBody | proxy.StageUpstreamTTFB | proxy.StageStream

func TestRecorder_EachPathRecordsOnce(t *testing.T) {
	tests := []struct {
		name     string
		upstream func(t *testing.T) string
		opts     func() proxy.Options
		// unknownLength sends the body chunked, so only reading it can
		// find it too large.
		unknownLength bool
		provider      proxy.Provider    // instead of Anthropic on upstream
		body          io.Reader         // instead of reqBody
		header        map[string]string // added to the request
		// cancelAfter cancels the request's context after this long, as a
		// client going away; negative means before the request starts.
		cancelAfter time.Duration
		want        proxy.RequestInfo // durations aren't compared
	}{
		{
			name:     "success",
			upstream: func(t *testing.T) string { return okUpstream(t).URL },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want:     proxy.RequestInfo{Status: 200, RateLimit: proxy.RateLimitAllowed, Ran: allStages},
		},
		{
			name:     "no rate limiter",
			upstream: func(t *testing.T) string { return okUpstream(t).URL },
			opts:     func() proxy.Options { return proxy.Options{} },
			want:     proxy.RequestInfo{Status: 200, RateLimit: proxy.RateLimitNotChecked, Ran: allStages &^ proxy.StageRateLimit},
		},
		{
			name:     "413 declared",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll, MaxBodyBytes: 8} },
			want:     proxy.RequestInfo{Status: 413, Failed: true, Phase: proxy.PhaseReadBody, RateLimit: proxy.RateLimitNotChecked},
		},
		{
			name:          "413 read",
			upstream:      func(*testing.T) string { return unusedUpstream },
			opts:          func() proxy.Options { return proxy.Options{RateLimiter: allowAll, MaxBodyBytes: 8} },
			unknownLength: true,
			want: proxy.RequestInfo{Status: 413, Failed: true, Phase: proxy.PhaseReadBody, RateLimit: proxy.RateLimitAllowed,
				Ran: proxy.StageRateLimit | proxy.StageReadBody},
		},
		{
			name:     "429",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts: func() proxy.Options {
				return proxy.Options{RateLimiter: wireLimiter{lim: proxy.RateLimit{RetryAfter: time.Second}}}
			},
			want: proxy.RequestInfo{Status: 429, Failed: true, Phase: proxy.PhaseRateLimit, RateLimit: proxy.RateLimitLimited, Ran: proxy.StageRateLimit},
		},
		{
			name:     "503 fail closed",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts: func() proxy.Options {
				return proxy.Options{RateLimiter: wireLimiter{err: errors.New("redis down")}, RateLimitFailClosed: true}
			},
			want: proxy.RequestInfo{Status: 503, Failed: true, Phase: proxy.PhaseRateLimit, RateLimit: proxy.RateLimitFailClosed, Ran: proxy.StageRateLimit},
		},
		{
			name:     "fail open",
			upstream: func(t *testing.T) string { return okUpstream(t).URL },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: wireLimiter{err: errors.New("redis down")}} },
			want:     proxy.RequestInfo{Status: 200, RateLimit: proxy.RateLimitFailOpen, Ran: allStages},
		},
		{
			name:     "503 admit",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts: func() proxy.Options {
				adm := proxy.NewAdmission(1)
				adm.TryAcquire() // the only slot, held for the whole test
				return proxy.Options{RateLimiter: allowAll, Admission: adm}
			},
			want: proxy.RequestInfo{Status: 503, Failed: true, Phase: proxy.PhaseAdmit, RateLimit: proxy.RateLimitAllowed,
				Ran: proxy.StageRateLimit | proxy.StageReadBody},
		},
		{
			name:     "502",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want: proxy.RequestInfo{Status: 502, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			name: "504",
			upstream: func(t *testing.T) string {
				done := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					select {
					case <-done:
					case <-r.Context().Done():
					}
				}))
				t.Cleanup(func() { close(done); srv.Close() })
				return srv.URL
			},
			opts: func() proxy.Options {
				return proxy.Options{RateLimiter: allowAll, HeaderTimeout: 50 * time.Millisecond}
			},
			want: proxy.RequestInfo{Status: 504, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			name:     "400 unreadable body",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			body:     iotest.ErrReader(errors.New("connection reset")),
			want: proxy.RequestInfo{Status: 400, Failed: true, Phase: proxy.PhaseReadBody, RateLimit: proxy.RateLimitAllowed,
				Ran: proxy.StageRateLimit | proxy.StageReadBody},
		},
		{
			name:     "408 body too slow",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			body:     iotest.ErrReader(os.ErrDeadlineExceeded), // what the read deadline produces
			want: proxy.RequestInfo{Status: 408, Failed: true, Phase: proxy.PhaseReadBody, RateLimit: proxy.RateLimitAllowed,
				Ran: proxy.StageRateLimit | proxy.StageReadBody},
		},
		{
			// Rejected before the rate limit check.
			name:     "415",
			upstream: func(*testing.T) string { return unusedUpstream },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			header:   map[string]string{"Content-Encoding": "gzip"},
			want:     proxy.RequestInfo{Status: 415, Failed: true, Phase: proxy.PhaseReadBody, RateLimit: proxy.RateLimitNotChecked},
		},
		{
			// The limiter reports the client's own cancellation: no decision.
			name:        "499 during the rate limit check",
			upstream:    func(*testing.T) string { return unusedUpstream },
			opts:        func() proxy.Options { return proxy.Options{RateLimiter: wireLimiter{err: context.Canceled}} },
			cancelAfter: -1,
			want: proxy.RequestInfo{Status: 499, Failed: true, Phase: proxy.PhaseRateLimit, RateLimit: proxy.RateLimitNotChecked,
				Ran: proxy.StageRateLimit},
		},
		{
			name:        "499 waiting for upstream",
			upstream:    func(t *testing.T) string { return hangingUpstream(t) },
			opts:        func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			cancelAfter: 30 * time.Millisecond,
			want: proxy.RequestInfo{Status: 499, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			// The 200 is sent; the failure keeps it, with phase stream.
			name: "stream broken after its 200",
			upstream: func(t *testing.T) string {
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "event: ping\ndata: {}\n\n")
					w.(http.Flusher).Flush()
					panic(http.ErrAbortHandler) // drops the connection mid-stream
				}))
				srv.Config.ErrorLog = log.New(io.Discard, "", 0)
				srv.Start()
				t.Cleanup(srv.Close)
				return srv.URL
			},
			opts: func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want: proxy.RequestInfo{Status: 200, Failed: true, Phase: proxy.PhaseStream, RateLimit: proxy.RateLimitAllowed, Ran: allStages},
		},
		{
			name:     "502 upstream rejects the gateway's key (401)",
			upstream: func(t *testing.T) string { return statusUpstream(t, http.StatusUnauthorized) },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want: proxy.RequestInfo{Status: 502, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			name:     "502 upstream forbids the gateway's key (403)",
			upstream: func(t *testing.T) string { return statusUpstream(t, http.StatusForbidden) },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want: proxy.RequestInfo{Status: 502, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			name: "502 provider breaks its contract",
			provider: stubProvider{resp: &proxy.Response{
				Status: http.StatusNotModified, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")),
			}},
			opts: func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want: proxy.RequestInfo{Status: 502, Failed: true, Phase: proxy.PhaseUpstream, RateLimit: proxy.RateLimitAllowed,
				Ran: allStages &^ proxy.StageStream},
		},
		{
			// The upstream's own error, relayed: not a gateway failure.
			name:     "upstream 5xx passed through",
			upstream: func(t *testing.T) string { return statusUpstream(t, http.StatusInternalServerError) },
			opts:     func() proxy.Options { return proxy.Options{RateLimiter: allowAll} },
			want:     proxy.RequestInfo{Status: 500, RateLimit: proxy.RateLimitAllowed, Ran: allStages},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			var gw http.Handler
			want := tt.want
			if tt.provider != nil {
				gw, _ = recordedGatewayFor(tt.provider, tt.opts(), rec)
				want.Upstream, want.Protocol = "stub", "stub"
			} else {
				gw, _ = recordedGateway(t, tt.upstream(t), tt.opts(), rec)
				want.Upstream, want.Protocol = "anthropic", "anthropic"
			}
			var r io.Reader = strings.NewReader(reqBody)
			if tt.body != nil {
				r = tt.body
			}
			if tt.unknownLength {
				r = io.MultiReader(r) // hides the length from NewRequest
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch {
			case tt.cancelAfter < 0:
				cancel()
			case tt.cancelAfter > 0:
				time.AfterFunc(tt.cancelAfter, cancel)
			}
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/messages", r)
			req.Header.Set("X-Api-Key", gatewayKey)
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			gw.ServeHTTP(w, req)
			// A 499 sends nothing; the recorder keeps its default.
			if tt.want.Status != 499 && w.Code != tt.want.Status {
				t.Fatalf("status %d, want %d: %s", w.Code, tt.want.Status, w.Body)
			}

			got := rec.only(t)
			durations := got
			got.RateLimitDuration, got.ReadBody, got.UpstreamTTFB, got.Stream, got.Total = 0, 0, 0, 0, 0
			if got != want {
				t.Errorf("RequestInfo = %+v\nwant           %+v", got, want)
			}
			checkDurations(t, durations)
			if rec.auth != 0 {
				t.Errorf("auth rejections = %d, want 0", rec.auth)
			}
		})
	}
}

// checkDurations checks that a stage that didn't run has no duration, and
// that the stages fit in the total. A fast request may measure 0: Windows'
// monotonic clock ticks coarsely, which is why Ran says what ran rather
// than durations above zero.
func checkDurations(t *testing.T, info proxy.RequestInfo) {
	t.Helper()
	for _, s := range []struct {
		stage proxy.Stages
		d     time.Duration
	}{
		{proxy.StageRateLimit, info.RateLimitDuration},
		{proxy.StageReadBody, info.ReadBody},
		{proxy.StageUpstreamTTFB, info.UpstreamTTFB},
		{proxy.StageStream, info.Stream},
	} {
		if !info.Ran.Has(s.stage) && s.d != 0 {
			t.Errorf("stage %b didn't run but lasted %v", s.stage, s.d)
		}
	}
	if sum := info.RateLimitDuration + info.ReadBody + info.UpstreamTTFB + info.Stream; info.Total < 0 || sum > info.Total {
		t.Errorf("stages sum to %v, total %v", sum, info.Total)
	}
}

func TestRecorder_Stream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := range 3 {
			io.WriteString(w, "event: ping\ndata: {}\n\n")
			w.(http.Flusher).Flush()
			if i < 2 {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}))
	defer upstream.Close()
	rec := &fakeRecorder{}
	gw, _ := recordedGateway(t, upstream.URL, proxy.Options{RateLimiter: allowAll}, rec)
	if w := post(gw, reqBody); w.Code != http.StatusOK || strings.Count(w.Body.String(), "event: ping") != 3 {
		t.Fatalf("status %d, body %q", w.Code, w.Body)
	}
	got := rec.only(t)
	if got.Status != 200 || got.Failed || got.Phase != "" || got.Ran != allStages {
		t.Errorf("RequestInfo = %+v, want a 200 through every stage", got)
	}
	// The relay waited for two 20 ms gaps.
	if got.Stream < 30*time.Millisecond {
		t.Errorf("Stream = %v, want the time the events took", got.Stream)
	}
	checkDurations(t, got)
}

// The request log line is written first and unchanged; the Recorder sees
// the same total as latency_ms.
func TestRecorder_SameTotalAsLog(t *testing.T) {
	rec := &fakeRecorder{}
	gw, logs := recordedGateway(t, okUpstream(t).URL, proxy.Options{}, rec)
	post(gw, reqBody)
	line := logLine(t, logs)
	got := rec.only(t)
	if ms, _ := line["latency_ms"].(float64); int64(ms) != got.Total.Milliseconds() {
		t.Errorf("latency_ms %v, Total %v", line["latency_ms"], got.Total)
	}
}

func TestRecorder_AuthRejection(t *testing.T) {
	rec := &fakeRecorder{}
	gw, logs := recordedGateway(t, unusedUpstream, proxy.Options{RateLimiter: allowAll}, rec)
	for _, key := range []string{"", "wrong-1", "wrong-2", "wrong-3"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
		if key != "" {
			req.Header.Set("X-Api-Key", key)
		}
		w := httptest.NewRecorder()
		gw.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("key %q: status %d, want 401", key, w.Code)
		}
	}
	if rec.auth != 4 {
		t.Errorf("auth rejections = %d, want 4", rec.auth)
	}
	if len(rec.requests) != 0 {
		t.Errorf("a 401 reached the Handler's Recorder: %+v", rec.requests)
	}
	// The WARN is rate-limited: one line for four rejections.
	if n := strings.Count(logs.String(), `"msg":"unauthorized"`); n != 1 {
		t.Errorf("logged %d unauthorized lines, want 1:\n%s", n, logs)
	}
	if strings.Contains(logs.String(), "wrong-") {
		t.Error("a presented key reached the log")
	}
}

// One Auth guards every route, so a flood spread across routes is still
// one line per interval.
func TestAuth_WarnSharedAcrossRoutes(t *testing.T) {
	var logs bytes.Buffer
	rec := &fakeRecorder{}
	auth := proxy.NewAuth([]string{gatewayKey}, slog.New(slog.NewJSONHandler(&logs, nil)), rec)
	mux := http.NewServeMux()
	for _, route := range []string{"/v1/messages", "/xai/v1/chat/completions"} {
		mux.Handle("POST "+route, auth.Require(anthropicErrors, http.NotFoundHandler()))
	}
	for i := range 6 {
		route := "/v1/messages"
		if i%2 == 1 {
			route = "/xai/v1/chat/completions"
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, route, nil))
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d, want 401", route, w.Code)
		}
	}
	if n := strings.Count(logs.String(), `"msg":"unauthorized"`); n != 1 {
		t.Errorf("logged %d unauthorized lines across two routes, want 1:\n%s", n, logs.String())
	}
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &line); err != nil || line["suppressed"] != float64(0) {
		t.Errorf("first line = %v (%v), want suppressed 0", line, err)
	}
	if rec.auth != 6 {
		t.Errorf("auth rejections = %d, want 6", rec.auth)
	}
}

// RequireKey, the single-route form, still works without a Recorder.
func TestRequireKey_NilRecorder(t *testing.T) {
	h := proxy.RequireKey([]string{gatewayKey}, anthropicErrors, slog.New(slog.DiscardHandler), http.NotFoundHandler())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/messages", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}
