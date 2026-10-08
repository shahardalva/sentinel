package anthropic

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shahardalva/sentinel/internal/proxy"
)

func TestForward(t *testing.T) {
	tests := []struct {
		name          string
		clientVersion string
		wantVersion   string
	}{
		{"default version", "", DefaultVersion},
		{"client version passed through", "2024-01-01", "2024-01-01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *http.Request
			var gotBody []byte
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r
				gotBody, _ = io.ReadAll(r.Body)
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
			resp, err := p.Forward(t.Context(), &proxy.Request{Body: []byte(`{"model":"m"}`), Header: h})
			if err != nil {
				t.Fatalf("Forward: %v", err)
			}
			resp.Body.Close()

			if got.Method != http.MethodPost || got.URL.Path != "/v1/messages" {
				t.Errorf("request = %s %s, want POST /v1/messages", got.Method, got.URL.Path)
			}
			if v := got.Header.Values("X-Api-Key"); len(v) != 1 || v[0] != "server-key" {
				t.Errorf("x-api-key = %v, want [server-key]", v)
			}
			if v := got.Header.Get("Authorization"); v != "" {
				t.Errorf("authorization leaked upstream: %q", v)
			}
			if v := got.Header.Get("Anthropic-Version"); v != tt.wantVersion {
				t.Errorf("anthropic-version = %q, want %q", v, tt.wantVersion)
			}
			if v := got.Header.Get("Content-Type"); v != "application/json" {
				t.Errorf("content-type = %q", v)
			}
			if string(gotBody) != `{"model":"m"}` {
				t.Errorf("body = %q", gotBody)
			}
		})
	}
}
