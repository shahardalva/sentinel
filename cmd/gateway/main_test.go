package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"maps"
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

// env returns a getenv that reads only from vars, so tests don't see the
// real environment.
func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

// wantConfigErr checks that loadConfig fails with an error starting with
// the name of the variable at fault.
func wantConfigErr(t *testing.T, vars map[string]string, name string) {
	t.Helper()
	_, err := loadConfig(env(vars))
	if err == nil || !strings.HasPrefix(err.Error(), name) {
		t.Fatalf("loadConfig() = %v, want error starting with %s", err, name)
	}
}

func TestLoadConfig(t *testing.T) {
	caFile := writeFile(t, generate(t).CACert)
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY":         "sk",
		"SENTINEL_API_KEYS":         "k1, k2",
		"SENTINEL_ADDR":             "127.0.0.1:9999",
		"SENTINEL_MAX_CONCURRENT":   "32",
		"SENTINEL_UPSTREAM_CA_FILE": caFile,
		"ANTHROPIC_BASE_URL":        "https://localhost:9090",
	}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if !slices.Equal(cfg.clientKeys, []string{"k1", "k2"}) || cfg.addr != "127.0.0.1:9999" ||
		cfg.maxConcurrent != 32 || cfg.caFile != caFile || len(cfg.extraCAs) != 1 ||
		cfg.baseURL != "https://localhost:9090" || cfg.upstream == nil {
		t.Errorf("loadConfig = %+v", cfg)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.addr != defaultAddr || cfg.maxConcurrent != proxy.DefaultMaxConcurrent ||
		cfg.caFile != "" || cfg.extraCAs != nil || cfg.baseURL != "" || cfg.upstream == nil {
		t.Errorf("loadConfig = %+v", cfg)
	}
}

func TestLoadConfig_RequiresEnv(t *testing.T) {
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
			wantConfigErr(t, map[string]string{
				"ANTHROPIC_API_KEY":  tt.upstreamKey,
				"SENTINEL_API_KEYS":  tt.clientKeys,
				"ANTHROPIC_BASE_URL": "http://api.anthropic.com",
			}, tt.wantErr)
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

func TestLoadConfig_RejectsInvalidMaxConcurrent(t *testing.T) {
	wantConfigErr(t, map[string]string{
		"ANTHROPIC_API_KEY":       "sk",
		"SENTINEL_API_KEYS":       "k1",
		"SENTINEL_MAX_CONCURRENT": "0",
	}, "SENTINEL_MAX_CONCURRENT")
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

func TestLoadConfig_RejectsBadCAFile(t *testing.T) {
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
			wantConfigErr(t, map[string]string{
				"ANTHROPIC_API_KEY":         "sk",
				"SENTINEL_API_KEYS":         "k1",
				"SENTINEL_UPSTREAM_CA_FILE": tt.path,
			}, "SENTINEL_UPSTREAM_CA_FILE")
		})
	}
}

func TestLoadConfig_CAFileKeepsHTTPSRule(t *testing.T) {
	wantConfigErr(t, map[string]string{
		"ANTHROPIC_API_KEY":         "sk",
		"SENTINEL_API_KEYS":         "k1",
		"SENTINEL_UPSTREAM_CA_FILE": writeFile(t, generate(t).CACert),
		"ANTHROPIC_BASE_URL":        "http://example.com",
	}, "ANTHROPIC_BASE_URL")
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

func TestLoadConfig_RateLimit(t *testing.T) {
	base := map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}
	with := func(extra map[string]string) map[string]string {
		vars := maps.Clone(base)
		maps.Copy(vars, extra)
		return vars
	}

	cfg, err := loadConfig(env(base))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.redis != nil || cfg.rateRPM != defaultRateRPM || cfg.rateFailClosed {
		t.Errorf("defaults: redis %v, rpm %d, failClosed %v; want nil, %d, false",
			cfg.redis, cfg.rateRPM, cfg.rateFailClosed, defaultRateRPM)
	}

	cfg, err = loadConfig(env(with(map[string]string{
		"SENTINEL_REDIS_URL":            "redis://:hunter2@redis:6379/0",
		"SENTINEL_RATE_LIMIT_RPM":       "600",
		"SENTINEL_RATE_LIMIT_FAIL_MODE": "closed",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.redis.Close()
	if cfg.redis == nil || cfg.redis.Options().Addr != "redis:6379" || cfg.rateRPM != 600 || !cfg.rateFailClosed {
		t.Errorf("set: rpm %d, failClosed %v, redis %v", cfg.rateRPM, cfg.rateFailClosed, cfg.redis)
	}

	for _, rpm := range []string{"0", "-5", "fast"} {
		wantConfigErr(t, with(map[string]string{"SENTINEL_RATE_LIMIT_RPM": rpm}), "SENTINEL_RATE_LIMIT_RPM")
	}
	for _, mode := range []string{"Closed", "deny", "1"} {
		wantConfigErr(t, with(map[string]string{"SENTINEL_RATE_LIMIT_FAIL_MODE": mode}), "SENTINEL_RATE_LIMIT_FAIL_MODE")
	}

	// A bad URL fails startup without echoing a password it may contain.
	_, err = loadConfig(env(with(map[string]string{"SENTINEL_REDIS_URL": "http://:hunter2@redis:6379"})))
	if err == nil || !strings.HasPrefix(err.Error(), "SENTINEL_REDIS_URL") || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("bad URL: err = %v, want a SENTINEL_REDIS_URL error without the password", err)
	}
}

func TestParseFailMode(t *testing.T) {
	for in, want := range map[string]bool{"": false, "open": false, " closed ": true} {
		if got, err := parseFailMode(in); err != nil || got != want {
			t.Errorf("parseFailMode(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
}

func TestLogRateLimit(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	cfg, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY":  "sk",
		"SENTINEL_API_KEYS":  "k1",
		"SENTINEL_REDIS_URL": "redis://:hunter2@127.0.0.1:1/0", // nothing listens
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.redis.Close()
	logRateLimit(logger, cfg)
	out := buf.String()
	for _, want := range []string{"rate limiting enabled", "rpm=60", "fail_mode=open", "redis not reachable"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("log contains the Redis password:\n%s", out)
	}
}
