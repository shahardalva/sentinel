package openai

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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
// applies it, after defaulting an empty BaseURL, and prefixes its errors.
func TestNew_ValidatesBaseURL(t *testing.T) {
	for _, u := range []string{"", "https://api.openai.com/v1", "http://127.0.0.1:11434/v1"} {
		if _, err := New(Config{BaseURL: u}); err != nil {
			t.Errorf("New(%q) = %v, want ok", u, err)
		}
	}
	_, err := New(Config{BaseURL: "http://api.openai.com/v1"})
	want := `openai: base URL for http://api.openai.com must use https (http is allowed only for loopback hosts)`
	if err == nil || err.Error() != want {
		t.Errorf("New(plain http) = %v, want %q", err, want)
	}
}

func TestNew_DefaultEndpoint(t *testing.T) {
	if got, want := mustNew(t, Config{}).endpoint, "https://api.openai.com/v1/chat/completions"; got != want {
		t.Errorf("endpoint = %q, want %q", got, want)
	}
}

// transportHeaders are the ones Go's HTTP client adds on its own, not taken
// from the client's request.
var transportHeaders = []string{"Accept-Encoding", "Content-Length", "User-Agent"}

func TestForward(t *testing.T) {
	for _, suffix := range []string{"/v1", "/v1/"} {
		t.Run("base URL ending "+suffix, func(t *testing.T) {
			// Copied on the server's goroutine; the channel orders the read.
			type captured struct {
				method, path string
				header       http.Header
				body         []byte
			}
			seen := make(chan captured, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				seen <- captured{r.Method, r.URL.Path, r.Header.Clone(), body}
			}))
			defer upstream.Close()

			p := mustNew(t, Config{APIKey: "server-key", BaseURL: upstream.URL + suffix})
			h := http.Header{}
			h.Set("Authorization", "Bearer client-token")
			h.Set("X-Api-Key", "client-key")
			h.Set("Content-Type", "text/plain")
			h.Set("OpenAI-Organization", "org-client")
			h.Set("OpenAI-Project", "proj-client")
			h.Set("OpenAI-Beta", "assistants=v2")
			h.Set("X-Stainless-Lang", "python")
			h.Set("X-Stainless-Package-Version", "1.0.0")
			h.Set("X-Custom", "anything")
			resp, err := p.Forward(t.Context(), &proxy.Request{Body: []byte(`{"model":"m"}`), Header: h})
			if err != nil {
				t.Fatalf("Forward: %v", err)
			}
			resp.Body.Close()
			got := <-seen

			if got.method != http.MethodPost || got.path != "/v1/chat/completions" {
				t.Errorf("request = %s %s, want POST /v1/chat/completions", got.method, got.path)
			}
			if v := got.header.Values("Authorization"); !slices.Equal(v, []string{"Bearer server-key"}) {
				t.Errorf("authorization = %q, want [Bearer server-key]", v)
			}
			if v := got.header.Values("Content-Type"); !slices.Equal(v, []string{"application/json"}) {
				t.Errorf("content-type = %q, want [application/json]", v)
			}
			for k := range got.header {
				if k != "Authorization" && k != "Content-Type" && !slices.Contains(transportHeaders, k) {
					t.Errorf("header %s = %q reached upstream", k, got.header[k])
				}
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
	if err == nil || !strings.HasPrefix(err.Error(), "openai: forward: ") {
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

func TestAllowResponseHeader(t *testing.T) {
	allowed := []string{
		"Content-Type", "content-type", "Retry-After", "Retry-After-Ms", "retry-after-ms",
		"x-ratelimit-limit-requests", "X-Ratelimit-Remaining-Tokens", "X-RateLimit-Reset-Requests",
	}
	for _, h := range allowed {
		if !allowResponseHeader(h) {
			t.Errorf("%s dropped, want allowed", h)
		}
	}
	denied := []string{
		"Openai-Organization", "Openai-Project", "Openai-Processing-Ms", "Openai-Version",
		"Set-Cookie", "Cf-Ray", "Cf-Cache-Status", "X-Request-Id", "Server", "Via",
		"Connection", "Transfer-Encoding", "Content-Length", "Strict-Transport-Security",
	}
	for _, h := range denied {
		if allowResponseHeader(h) {
			t.Errorf("%s allowed, want dropped", h)
		}
	}
}

func TestForward_Response(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "req_up_1")
		w.Header().Set("Retry-After", "3")
		w.Header().Set("Retry-After-Ms", "2500")
		w.Header().Set("X-Ratelimit-Remaining-Requests", "0")
		w.Header().Set("Openai-Organization", "org-secret")
		w.Header().Set("Openai-Project", "proj-secret")
		w.Header().Set("Openai-Processing-Ms", "12")
		w.Header().Set("Openai-Version", "2020-10-01")
		w.Header().Set("Set-Cookie", "__cf_bm=abc")
		w.Header().Set("Cf-Ray", "abc-TLV")
		w.Header().Set("Content-Type", "application/json")
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
	want := http.Header{
		"Content-Type":                   {"application/json"},
		"Retry-After":                    {"3"},
		"Retry-After-Ms":                 {"2500"},
		"X-Ratelimit-Remaining-Requests": {"0"},
	}
	if len(resp.Header) != len(want) {
		t.Errorf("headers = %v, want %v", resp.Header, want)
	}
	for k, v := range want {
		if !slices.Equal(resp.Header[k], v) {
			t.Errorf("header %s = %q, want %q", k, resp.Header[k], v)
		}
	}
	// Not an OpenAI error, so replaced; see errorbody_test.go.
	if b, _ := io.ReadAll(resp.Body); !strings.Contains(string(b), "upstream error") {
		t.Errorf("body = %q, want the gateway's replacement", b)
	}
}

func TestFormatError(t *testing.T) {
	tests := []struct {
		kind      proxy.ErrorKind
		typ, code string // code is JSON: null or a quoted string
	}{
		{proxy.ErrInvalidRequest, "invalid_request_error", `null`},
		{proxy.ErrAuthentication, "invalid_request_error", `"invalid_api_key"`},
		{proxy.ErrRateLimit, "requests", `"rate_limit_exceeded"`},
		{proxy.ErrRequestTooLarge, "invalid_request_error", `"request_too_large"`},
		{proxy.ErrTimeout, "timeout_error", `null`},
		{proxy.ErrOverloaded, "server_error", `"overloaded"`},
		{proxy.ErrAPI, "server_error", `null`},
		{proxy.ErrorKind(0), "server_error", `null`},  // zero value: no kind set
		{proxy.ErrorKind(-1), "server_error", `null`}, // unknown
	}
	for _, tt := range tests {
		ct, body := (&Provider{}).FormatError(http.StatusTeapot, tt.kind, `say "hi"`)
		if ct != "application/json" {
			t.Errorf("kind %d: content type = %q", tt.kind, ct)
		}
		want := `{"error":{"message":"say \"hi\"","type":"` + tt.typ + `","param":null,"code":` + tt.code + `}}` + "\n"
		if string(body) != want {
			t.Errorf("kind %d: body = %q, want %q", tt.kind, body, want)
		}
		if _, body := FormatError(0, tt.kind, "<b>"); bytes.ContainsAny(body, "<>") {
			t.Errorf("kind %d: body = %q, want < and > escaped", tt.kind, body)
		}
	}
}
