package anthropic

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
)

func mustNew(t *testing.T, cfg Config) *Provider {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// The full base-URL rule is tested in package upstream; this checks New
// applies it, after defaulting an empty BaseURL.
func TestNew_ValidatesBaseURL(t *testing.T) {
	for _, u := range []string{"", "https://api.anthropic.com", "http://127.0.0.1:9000"} {
		if _, err := New(Config{BaseURL: u}); err != nil {
			t.Errorf("New(%q) = %v, want ok", u, err)
		}
	}
	for _, u := range []string{"http://api.anthropic.com", "https://user:pass@api.anthropic.com"} {
		if _, err := New(Config{BaseURL: u}); err == nil {
			t.Errorf("New(%q) = ok, want error", u)
		}
	}
}

func TestForward(t *testing.T) {
	tests := []struct {
		name          string
		clientVersion string
		wantVersion   string
		beta          []string
	}{
		{"default version", "", DefaultVersion, nil},
		{"client version passed through", "2024-01-01", "2024-01-01", nil},
		{"beta passed through", "", DefaultVersion, []string{"feature-a", "feature-b,feature-c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The handler runs on the server's goroutine, and *http.Request
			// must not be used after it returns, so copy what we need and
			// hand it over on a channel (which also orders the accesses).
			type captured struct {
				method, path string
				header       http.Header
				body         []byte
			}
			seen := make(chan captured, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				seen <- captured{r.Method, r.URL.Path, r.Header.Clone(), body}
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			p := mustNew(t, Config{APIKey: "server-key", BaseURL: upstream.URL + "/"})
			h := http.Header{}
			h.Set("X-Api-Key", "client-key")
			h.Set("Authorization", "Bearer client-token")
			if tt.clientVersion != "" {
				h.Set("Anthropic-Version", tt.clientVersion)
			}
			for _, b := range tt.beta {
				h.Add("Anthropic-Beta", b)
			}
			resp, err := p.Forward(t.Context(), &proxy.Request{Body: []byte(`{"model":"m"}`), Header: h})
			if err != nil {
				t.Fatalf("Forward: %v", err)
			}
			resp.Body.Close()
			got := <-seen

			if got.method != http.MethodPost || got.path != "/v1/messages" {
				t.Errorf("request = %s %s, want POST /v1/messages", got.method, got.path)
			}
			if v := got.header.Values("X-Api-Key"); len(v) != 1 || v[0] != "server-key" {
				t.Errorf("x-api-key = %v, want [server-key]", v)
			}
			if v := got.header.Get("Authorization"); v != "" {
				t.Errorf("authorization leaked upstream: %q", v)
			}
			if v := got.header.Get("Anthropic-Version"); v != tt.wantVersion {
				t.Errorf("anthropic-version = %q, want %q", v, tt.wantVersion)
			}
			if v := got.header.Values("Anthropic-Beta"); !slices.Equal(v, tt.beta) {
				t.Errorf("anthropic-beta = %q, want %q", v, tt.beta)
			}
			if v := got.header.Get("Content-Type"); v != "application/json" {
				t.Errorf("content-type = %q", v)
			}
			if string(got.body) != `{"model":"m"}` {
				t.Errorf("body = %q", got.body)
			}
		})
	}
}

func TestForward_WrapsErrors(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	url := upstream.URL
	upstream.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := mustNew(t, Config{BaseURL: url}).Forward(ctx, &proxy.Request{Header: http.Header{}})
	if err == nil || !strings.HasPrefix(err.Error(), "anthropic: forward: ") {
		t.Fatalf("err = %v, want wrapped with provider context", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false; wrapping must keep the cause: %v", err)
	}
}

// The full TLS setup is tested in package upstream; this checks New's
// default client gets it.
func TestNew_TLSFloor(t *testing.T) {
	tr, ok := mustNew(t, Config{}).client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("default client has no *http.Transport")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Errorf("TLSClientConfig = %+v, want MinVersion >= TLS 1.2", tr.TLSClientConfig)
	}
}

func TestNew_ConnectionPool(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want int
	}{
		{"default", Config{}, proxy.DefaultMaxConcurrent},
		{"configured", Config{MaxIdleConns: 300}, 300},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, ok := mustNew(t, tt.cfg).client.Transport.(*http.Transport)
			if !ok {
				t.Fatal("default client has no *http.Transport")
			}
			if tr.MaxIdleConnsPerHost != tt.want || tr.MaxIdleConns != tt.want {
				t.Errorf("idle conns = %d per host, %d total; want %d",
					tr.MaxIdleConnsPerHost, tr.MaxIdleConns, tt.want)
			}
			if tr == http.DefaultTransport {
				t.Error("shares http.DefaultTransport; must be a clone")
			}
		})
	}
}

func TestNew_ExtraRootCAsKeepHTTPSRule(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	_, err := New(Config{BaseURL: "http://api.anthropic.com", ExtraRootCAs: []*x509.Certificate{srv.Certificate()}})
	if err == nil {
		t.Fatal("plain-http base URL accepted with ExtraRootCAs set")
	}
}

func TestNew_ExtraRootCAsIgnoredWithClient(t *testing.T) {
	client := &http.Client{}
	p := mustNew(t, Config{Client: client, ExtraRootCAs: []*x509.Certificate{{}}})
	if p.client != client || client.Transport != nil {
		t.Error("caller's Client was replaced or modified")
	}
}

func TestAllowResponseHeader(t *testing.T) {
	allowed := []string{
		"Content-Type", "content-type", "Retry-After", "Request-Id", "X-Should-Retry",
		"anthropic-ratelimit-requests-limit", "Anthropic-Ratelimit-Tokens-Reset",
	}
	for _, h := range allowed {
		if !allowResponseHeader(h) {
			t.Errorf("%s dropped, want allowed", h)
		}
	}
	denied := []string{
		"Anthropic-Organization-Id", "Set-Cookie", "Server", "Cf-Ray", "Via",
		"Connection", "Transfer-Encoding", "Content-Length", "X-Request-Id",
	}
	for _, h := range denied {
		if allowResponseHeader(h) {
			t.Errorf("%s allowed, want dropped", h)
		}
	}
}

func TestForward_Response(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Request-Id", "req_up_1")
		w.Header().Set("Retry-After", "3")
		w.Header().Set("Anthropic-Organization-Id", "org-secret")
		w.Header().Set("Set-Cookie", "__cf_bm=abc")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "body")
	}))
	defer upstream.Close()

	resp, err := mustNew(t, Config{BaseURL: upstream.URL}).Forward(t.Context(), &proxy.Request{Header: http.Header{}})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	defer resp.Body.Close()

	if resp.Status != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.Status)
	}
	if resp.RequestID != "req_up_1" {
		t.Errorf("RequestID = %q, want req_up_1", resp.RequestID)
	}
	if resp.Header.Get("Retry-After") != "3" {
		t.Error("allowed header Retry-After dropped")
	}
	for _, k := range []string{"Anthropic-Organization-Id", "Set-Cookie", "Date"} {
		if v := resp.Header.Get(k); v != "" {
			t.Errorf("header %s returned: %q", k, v)
		}
	}
	if b, _ := io.ReadAll(resp.Body); string(b) != "body" {
		t.Errorf("body = %q", b)
	}
}

func TestForward_RejectsInvalidVersion(t *testing.T) {
	var called atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
	}))
	defer upstream.Close()
	p := mustNew(t, Config{BaseURL: upstream.URL})

	for _, v := range []string{"latest", "2023-6-1", "2023-13-01", "2023-06-01x", "2023-06-01\x00"} {
		h := http.Header{}
		h["Anthropic-Version"] = []string{v}
		_, err := p.Forward(t.Context(), &proxy.Request{Header: h})
		if _, ok := errors.AsType[*proxy.RequestError](err); !ok {
			t.Errorf("version %q: err = %v, want *proxy.RequestError", v, err)
		}
	}
	if called.Load() {
		t.Error("upstream called for an invalid version")
	}
}

func TestForward_RejectsInvalidBeta(t *testing.T) {
	var called atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
	}))
	defer upstream.Close()
	p := mustNew(t, Config{BaseURL: upstream.URL})

	for _, v := range []string{"a\r\nX-Injected: 1", "a\nb", "a\rb", "a\x00b", "a\x7fb"} {
		h := http.Header{}
		h["Anthropic-Beta"] = []string{"ok-beta", v}
		_, err := p.Forward(t.Context(), &proxy.Request{Header: h})
		if _, ok := errors.AsType[*proxy.RequestError](err); !ok {
			t.Errorf("beta %q: err = %v, want *proxy.RequestError", v, err)
		}
	}
	if called.Load() {
		t.Error("upstream called for an invalid beta")
	}

	// A tab is a legal header character and must still pass.
	h := http.Header{}
	h.Set("Anthropic-Beta", "a,\tb")
	resp, err := p.Forward(t.Context(), &proxy.Request{Header: h})
	if err != nil {
		t.Fatalf("beta with tab: %v", err)
	}
	resp.Body.Close()
}

// TestNew_ErrorText pins New's error messages, which reach the operator via
// the gateway's startup error.
func TestNew_ErrorText(t *testing.T) {
	tests := []struct{ url, want string }{
		{"https://user:pass@api.anthropic.com", `anthropic: base URL for https://api.anthropic.com must not contain credentials`},
		{"http://api.anthropic.com", `anthropic: base URL for http://api.anthropic.com must use https (http is allowed only for loopback hosts)`},
		{"https://api.anthropic.com/\x7f", `anthropic: base URL is not a valid URL`},
	}
	for _, tt := range tests {
		if _, err := New(Config{BaseURL: tt.url}); err == nil || err.Error() != tt.want {
			t.Errorf("New(%q) = %v, want %q", tt.url, err, tt.want)
		}
	}
}

func TestFormatError(t *testing.T) {
	tests := []struct {
		kind proxy.ErrorKind
		want string
	}{
		{proxy.ErrInvalidRequest, "invalid_request_error"},
		{proxy.ErrAuthentication, "authentication_error"},
		{proxy.ErrRateLimit, "rate_limit_error"},
		{proxy.ErrRequestTooLarge, "request_too_large"},
		{proxy.ErrTimeout, "timeout_error"},
		{proxy.ErrOverloaded, "overloaded_error"},
		{proxy.ErrAPI, "api_error"},
		{proxy.ErrorKind(0), "api_error"},  // zero value: no kind set
		{proxy.ErrorKind(-1), "api_error"}, // unknown
	}
	for _, tt := range tests {
		ct, body := (&Provider{}).FormatError(http.StatusTeapot, tt.kind, `say "hi"`)
		if ct != "application/json" {
			t.Errorf("kind %d: content type = %q", tt.kind, ct)
		}
		// The trailing newline is part of the bytes clients have always
		// received.
		want := `{"error":{"message":"say \"hi\"","type":"` + tt.want + `"},"type":"error"}` + "\n"
		if string(body) != want {
			t.Errorf("kind %d: body = %q, want %q", tt.kind, body, want)
		}
		// So is encoding/json's default HTML escaping.
		if _, body := FormatError(0, tt.kind, "<b>"); bytes.ContainsAny(body, "<>") {
			t.Errorf("kind %d: body = %q, want < and > escaped", tt.kind, body)
		}
	}
}
