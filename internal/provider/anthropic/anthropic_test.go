package anthropic

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
)

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

			p := New(Config{APIKey: "server-key", BaseURL: upstream.URL + "/"})
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
	_, err := New(Config{BaseURL: url}).Forward(ctx, &proxy.Request{Header: http.Header{}})
	if err == nil || !strings.HasPrefix(err.Error(), "anthropic: forward: ") {
		t.Fatalf("err = %v, want wrapped with provider context", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false; wrapping must keep the cause: %v", err)
	}
}

func TestNew_ConnectionPool(t *testing.T) {
	tr, ok := New(Config{}).client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("default client has no *http.Transport")
	}
	if tr.MaxIdleConnsPerHost != maxIdleConns || tr.MaxIdleConns != maxIdleConns {
		t.Errorf("idle conns = %d per host, %d total; want %d",
			tr.MaxIdleConnsPerHost, tr.MaxIdleConns, maxIdleConns)
	}
	if tr == http.DefaultTransport {
		t.Error("shares http.DefaultTransport; must be a clone")
	}
}

func TestAllowResponseHeader(t *testing.T) {
	p := New(Config{})
	allowed := []string{
		"Content-Type", "content-type", "Retry-After", "Request-Id", "X-Should-Retry",
		"anthropic-ratelimit-requests-limit", "Anthropic-Ratelimit-Tokens-Reset",
	}
	for _, h := range allowed {
		if !p.AllowResponseHeader(h) {
			t.Errorf("%s dropped, want allowed", h)
		}
	}
	denied := []string{
		"Anthropic-Organization-Id", "Set-Cookie", "Server", "Cf-Ray", "Via",
		"Connection", "Transfer-Encoding", "Content-Length", "X-Request-Id",
	}
	for _, h := range denied {
		if p.AllowResponseHeader(h) {
			t.Errorf("%s allowed, want dropped", h)
		}
	}
}
