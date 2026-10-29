package proxy_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

var anthropicErrors = proxy.ErrorFormatterFunc(anthropic.FormatError)

func TestRequireKey(t *testing.T) {
	tests := []struct {
		name   string
		header map[string]string
		want   int
	}{
		{"no credentials", nil, http.StatusUnauthorized},
		{"wrong x-api-key", map[string]string{"X-Api-Key": "nope"}, http.StatusUnauthorized},
		{"wrong bearer", map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		{"empty bearer", map[string]string{"Authorization": "Bearer "}, http.StatusUnauthorized},
		{"non-bearer scheme", map[string]string{"Authorization": "Basic key-one"}, http.StatusUnauthorized},
		{"valid x-api-key", map[string]string{"X-Api-Key": "key-one"}, http.StatusOK},
		{"valid bearer", map[string]string{"Authorization": "Bearer key-one"}, http.StatusOK},
		{"lowercase bearer", map[string]string{"Authorization": "bearer key-one"}, http.StatusOK},
		{"uppercase bearer", map[string]string{"Authorization": "BEARER key-one"}, http.StatusOK},
		{"bearer without token", map[string]string{"Authorization": "Bearer"}, http.StatusUnauthorized},
		{"bearer-like scheme", map[string]string{"Authorization": "Bearerx key-one"}, http.StatusUnauthorized},
		{"second key", map[string]string{"X-Api-Key": "key-two"}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen http.Header
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen = r.Header
			})
			h := proxy.RequireKey([]string{"key-one", "key-two"}, anthropicErrors, slog.New(slog.DiscardHandler), next)

			req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if tt.want == http.StatusUnauthorized {
				if typ := errorType(t, rec); typ != "authentication_error" {
					t.Errorf("error type = %q", typ)
				}
				if c := rec.Header().Get("Connection"); c != "close" {
					t.Errorf("Connection = %q, want close", c)
				}
				if seen != nil {
					t.Error("next called for rejected request")
				}
				return
			}
			if seen.Get("X-Api-Key") != "" || seen.Get("Authorization") != "" {
				t.Errorf("credentials passed to next: %v", seen)
			}
		})
	}
}

func TestRequireKey_NoKeysRejectsAll(t *testing.T) {
	h := proxy.RequireKey([]string{"", ""}, anthropicErrors, slog.New(slog.DiscardHandler), http.NotFoundHandler())
	for _, key := range []string{"", "anything"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		req.Header.Set("X-Api-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("key %q: status = %d, want 401", key, rec.Code)
		}
	}
}

func TestGateway_UnauthenticatedPartialBody(t *testing.T) {
	gw, _ := newGateway(t, unusedUpstream, proxy.Options{})
	srv := httptest.NewServer(gw)
	defer srv.Close()

	// Headers declare 1000 bytes; the client sends 9 and then nothing.
	resp := sendPartial(t, srv, "wrong-key", 1000)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestGateway_CredentialsNotLeaked(t *testing.T) {
	seen := make(chan http.Header, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.WriteHeader(http.StatusInternalServerError) // exercise the ERROR log path
	}))
	defer upstream.Close()

	gw, logs := newGateway(t, upstream.URL, proxy.Options{})

	ok := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	ok.Header.Set("Authorization", "Bearer "+gatewayKey)
	gw.ServeHTTP(httptest.NewRecorder(), ok)

	bad := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	bad.Header.Set("X-Api-Key", "wrong-client-secret")
	gw.ServeHTTP(httptest.NewRecorder(), bad)

	h := <-seen
	if got := h.Get("X-Api-Key"); got != upstreamKey {
		t.Errorf("upstream x-api-key = %q, want the gateway's own key", got)
	}
	if got := h.Get("Authorization"); got != "" {
		t.Errorf("client Authorization forwarded upstream: %q", got)
	}
	for _, secret := range []string{upstreamKey, gatewayKey, "wrong-client-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("logs contain credential %q:\n%s", secret, logs)
		}
	}
}

// unavailableKeys is a KeySource that can't vouch for its keys.
type unavailableKeys struct{}

func (unavailableKeys) Keys() *proxy.KeySet { return proxy.UnavailableKeySet() }

// A source that can't vouch for its keys makes every request a 503,
// whatever it presents: the key may well be valid, so a 401 would tell the
// client to fix what isn't broken. It is not an auth rejection.
func TestAuth_UnavailableKeysAre503(t *testing.T) {
	rec := &fakeRecorder{}
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	h := proxy.NewAuthSource(unavailableKeys{}, slog.New(slog.DiscardHandler), rec).Require(anthropicErrors, next)
	for _, key := range []string{"", "any-key"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		if key != "" {
			req.Header.Set("X-Api-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("key %q: status %d, want 503", key, w.Code)
		}
		if typ := errorType(t, w); typ != "api_error" {
			t.Errorf("key %q: error type %q, want api_error", key, typ)
		}
		if c := w.Header().Get("Connection"); c != "close" {
			t.Errorf("key %q: Connection = %q, want close", key, c)
		}
	}
	if called {
		t.Error("next ran without verified keys")
	}
	if rec.auth != 0 {
		t.Errorf("%d auth rejections recorded, want 0: no key was found wrong", rec.auth)
	}
}
