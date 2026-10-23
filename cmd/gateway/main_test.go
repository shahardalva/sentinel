package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/devcert"
	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/proxy"
)

func TestRun_RequiresEnv(t *testing.T) {
	tests := []struct {
		name, upstreamKey, clientKeys, wantErr string
	}{
		{"no upstream key", "", "k1", "ANTHROPIC_API_KEY"},
		{"no client keys", "sk", "", "SENTINEL_API_KEYS"},
		{"blank client keys", "sk", " , ,", "SENTINEL_API_KEYS"},
		{"plain-http base URL", "sk", "k1", "ANTHROPIC_BASE_URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", tt.upstreamKey)
			t.Setenv("SENTINEL_API_KEYS", tt.clientKeys)
			t.Setenv("ANTHROPIC_BASE_URL", "http://api.anthropic.com")
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

func TestRun_RejectsInvalidMaxConcurrent(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk")
	t.Setenv("SENTINEL_API_KEYS", "k1")
	t.Setenv("SENTINEL_MAX_CONCURRENT", "0")
	err := run(slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "SENTINEL_MAX_CONCURRENT") {
		t.Fatalf("run() = %v, want error mentioning SENTINEL_MAX_CONCURRENT", err)
	}
}

// tlsUpstream starts an HTTPS server on 127.0.0.1 using b's server
// certificate.
func tlsUpstream(t *testing.T, b *devcert.Bundle) *httptest.Server {
	t.Helper()
	cert, err := tls.X509KeyPair(b.ServerCert, b.ServerKey)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"type":"message"}`))
	}))
	ts.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts
}

func writeFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func generate(t *testing.T) *devcert.Bundle {
	t.Helper()
	b, err := devcert.Generate([]string{"localhost", "127.0.0.1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// forward sends one request through a provider that trusts the CAs in
// caFile, to an upstream presenting a certificate from served.
func forward(t *testing.T, caFile string, served *devcert.Bundle) error {
	t.Helper()
	cas, err := loadCAFile(caFile)
	if err != nil {
		t.Fatalf("loadCAFile: %v", err)
	}
	ts := tlsUpstream(t, served)
	p, err := anthropic.New(anthropic.Config{APIKey: "sk", BaseURL: ts.URL, ExtraRootCAs: cas})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Forward(t.Context(), &proxy.Request{Body: []byte("{}"), Header: http.Header{}})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	return nil
}

func TestUpstreamCA_TrustsServerSignedByCA(t *testing.T) {
	b := generate(t)
	if err := forward(t, writeFile(t, b.CACert), b); err != nil {
		t.Fatalf("Forward: %v", err)
	}
}

func TestUpstreamCA_RejectsUnknownCA(t *testing.T) {
	trusted, other := generate(t), generate(t)
	err := forward(t, writeFile(t, trusted.CACert), other)
	var verr *tls.CertificateVerificationError
	if !errors.As(err, &verr) {
		t.Fatalf("Forward = %v, want a certificate verification error", err)
	}
}

func TestRun_RejectsBadCAFile(t *testing.T) {
	b := generate(t)
	// A PEM block of the right type whose contents don't parse.
	junk := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("junk")})
	tests := []struct{ name, path string }{
		{"missing", filepath.Join(t.TempDir(), "nope.pem")},
		{"not PEM", writeFile(t, []byte("not a certificate"))},
		{"invalid certificate", writeFile(t, junk)},
		{"valid then invalid certificate", writeFile(t, append(slices.Clone(b.CACert), junk...))},
		{"private key", writeFile(t, append(slices.Clone(b.CACert), b.ServerKey...))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ANTHROPIC_API_KEY", "sk")
			t.Setenv("SENTINEL_API_KEYS", "k1")
			t.Setenv("SENTINEL_UPSTREAM_CA_FILE", tt.path)
			err := run(slog.New(slog.DiscardHandler))
			if err == nil || !strings.Contains(err.Error(), "SENTINEL_UPSTREAM_CA_FILE") {
				t.Fatalf("run() = %v, want error mentioning SENTINEL_UPSTREAM_CA_FILE", err)
			}
		})
	}
}

func TestRun_CAFileKeepsHTTPSRule(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk")
	t.Setenv("SENTINEL_API_KEYS", "k1")
	t.Setenv("SENTINEL_UPSTREAM_CA_FILE", writeFile(t, generate(t).CACert))
	t.Setenv("ANTHROPIC_BASE_URL", "http://example.com")
	err := run(slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "ANTHROPIC_BASE_URL") {
		t.Fatalf("run() = %v, want error mentioning ANTHROPIC_BASE_URL", err)
	}
}

func TestLoadCAFile(t *testing.T) {
	a, b := generate(t), generate(t)
	path := writeFile(t, append(slices.Clone(a.CACert), b.CACert...))
	certs, err := loadCAFile(path)
	if err != nil || len(certs) != 2 {
		t.Fatalf("loadCAFile = %d certs, %v; want 2, nil", len(certs), err)
	}
}

func TestLogCAs(t *testing.T) {
	b := generate(t)
	ca, err := loadCAFile(writeFile(t, b.CACert))
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := loadCAFile(writeFile(t, b.ServerCert))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		certs     []*x509.Certificate
		baseURL   string
		wantWarns []string
	}{
		{"CA, custom upstream", ca, "https://localhost:9090", nil},
		{"CA, default upstream", ca, "", []string{"default Anthropic API"}},
		{"CA, explicit default upstream", ca, "https://api.anthropic.com/", []string{"default Anthropic API"}},
		{"non-CA, custom upstream", leaf, "https://localhost:9090", []string{"non-CA certificate"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			logCAs(slog.New(slog.NewTextHandler(&buf, nil)), "ca.pem", tt.certs, tt.baseURL)
			var warns []string
			for line := range strings.Lines(buf.String()) {
				if strings.Contains(line, "level=WARN") {
					warns = append(warns, line)
				}
			}
			if len(warns) != len(tt.wantWarns) {
				t.Fatalf("warnings = %q, want %d", warns, len(tt.wantWarns))
			}
			for i, want := range tt.wantWarns {
				if !strings.Contains(warns[i], want) {
					t.Errorf("warning %d = %q, want it to mention %q", i, warns[i], want)
				}
			}
			if !strings.Contains(buf.String(), "certs=1") {
				t.Errorf("log %q missing cert count", buf.String())
			}
		})
	}
}

func TestParseMaxConcurrent(t *testing.T) {
	if got, err := parseMaxConcurrent(""); err != nil || got != proxy.DefaultMaxConcurrent {
		t.Errorf("parseMaxConcurrent(\"\") = %d, %v; want %d", got, err, proxy.DefaultMaxConcurrent)
	}
	if got, err := parseMaxConcurrent("256"); err != nil || got != 256 {
		t.Errorf("parseMaxConcurrent(\"256\") = %d, %v; want 256", got, err)
	}
	for _, s := range []string{"0", "-1", "abc", "1.5", "12x", " "} {
		if got, err := parseMaxConcurrent(s); err == nil {
			t.Errorf("parseMaxConcurrent(%q) = %d, want error", s, got)
		}
	}
}
