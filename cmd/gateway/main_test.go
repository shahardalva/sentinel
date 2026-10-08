package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestRun_RequiresEnv(t *testing.T) {
	tests := []struct {
		name, upstreamKey, clientKeys, wantErr string
	}{
		{"no upstream key", "", "k1", "ANTHROPIC_API_KEY"},
		{"no client keys", "sk", "", "SENTINEL_API_KEYS"},
		{"blank client keys", "sk", " , ,", "SENTINEL_API_KEYS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", tt.upstreamKey)
			t.Setenv("SENTINEL_API_KEYS", tt.clientKeys)
			err := run(slog.New(slog.DiscardHandler))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run() = %v, want error mentioning %s", err, tt.wantErr)
			}
		})
	}
}

func TestCloseUnmatched(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {})
	h := closeUnmatched(mux)

	tests := []struct {
		method, path string
		wantStatus   int
		wantConn     string
	}{
		{http.MethodPost, "/v1/messages", http.StatusOK, ""},
		{http.MethodGet, "/v1/messages", http.StatusMethodNotAllowed, "close"},
		{http.MethodPost, "/nope", http.StatusNotFound, "close"},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, strings.NewReader("{}")))
		if rec.Code != tt.wantStatus || rec.Header().Get("Connection") != tt.wantConn {
			t.Errorf("%s %s: status %d, Connection %q; want %d, %q",
				tt.method, tt.path, rec.Code, rec.Header().Get("Connection"), tt.wantStatus, tt.wantConn)
		}
	}
}

func TestParseKeys(t *testing.T) {
	got := parseKeys(" a ,b,, ,c ")
	if want := []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("parseKeys = %q, want %q", got, want)
	}
	if got := parseKeys(""); len(got) != 0 {
		t.Errorf("parseKeys(\"\") = %q, want empty", got)
	}
}
