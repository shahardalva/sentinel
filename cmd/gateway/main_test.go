package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
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
	"github.com/shahardalva/sentinel/internal/provider/upstream"
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
		len(cfg.upstreams) != 1 || cfg.upstreams[0].baseURL != "https://localhost:9090" {
		t.Errorf("loadConfig = %+v", cfg)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.addr != defaultAddr || cfg.maxConcurrent != proxy.DefaultMaxConcurrent ||
		cfg.caFile != "" || cfg.extraCAs != nil || cfg.ignored != nil {
		t.Errorf("loadConfig = %+v", cfg)
	}
	if got := upstreamNames(cfg); !slices.Equal(got, []string{"anthropic"}) {
		t.Errorf("upstreams = %q, want [anthropic]", got)
	}
	if u := cfg.upstreams[0]; u.baseURL != anthropic.DefaultBaseURL || u.provider.Name() != "anthropic" {
		t.Errorf("anthropic upstream = %+v", u)
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
		env       map[string]string // upstream variables
		wantWarns []string
	}{
		{"CA, custom upstream", ca, map[string]string{"ANTHROPIC_BASE_URL": "https://localhost:9090"}, nil},
		{"CA, default upstream", ca, nil, []string{"default Anthropic API"}},
		{"CA, explicit default upstream", ca, map[string]string{"ANTHROPIC_BASE_URL": "https://api.anthropic.com/"}, []string{"default Anthropic API"}},
		{"non-CA, custom upstream", leaf, map[string]string{"ANTHROPIC_BASE_URL": "https://localhost:9090"}, []string{"non-CA certificate"}},
		{
			"CA, both upstreams on their defaults", ca,
			map[string]string{"SENTINEL_UPSTREAMS": "anthropic,openai"},
			[]string{
				"default Anthropic API; anyone holding one of their keys could impersonate it and read ANTHROPIC_API_KEY",
				"default OpenAI API; anyone holding one of their keys could impersonate it and read OPENAI_API_KEY",
			},
		},
		{
			"CA, only openai on its default", ca,
			map[string]string{"SENTINEL_UPSTREAMS": "anthropic,openai", "ANTHROPIC_BASE_URL": "https://localhost:9090", "OPENAI_BASE_URL": "https://API.openai.com/v1/"},
			[]string{"default OpenAI API"},
		},
		{
			"CA, default hosts with a trailing dot", ca,
			map[string]string{"SENTINEL_UPSTREAMS": "anthropic,openai", "ANTHROPIC_BASE_URL": "https://api.anthropic.com.", "OPENAI_BASE_URL": "https://api.openai.com./v1"},
			[]string{"default Anthropic API", "default OpenAI API"},
		},
		{
			"CA, openai-compatible elsewhere", ca,
			map[string]string{"SENTINEL_UPSTREAMS": "openai", "OPENAI_BASE_URL": "https://localhost:9090/v1"}, nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{"ANTHROPIC_API_KEY": "sk", "OPENAI_API_KEY": "sk-o", "SENTINEL_API_KEYS": "k1"}
			maps.Copy(vars, tt.env)
			cfg, err := loadConfig(env(vars))
			if err != nil {
				t.Fatal(err)
			}
			var buf strings.Builder
			logCAs(slog.New(slog.NewTextHandler(&buf, nil)), "ca.pem", tt.certs, cfg.upstreams)
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
		"SENTINEL_MAX_CONCURRENT":       "32",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer cfg.redis.Close()
	if cfg.redis == nil || cfg.redis.Options().Addr != "redis:6379" || cfg.rateRPM != 600 || !cfg.rateFailClosed {
		t.Errorf("set: rpm %d, failClosed %v, redis %v", cfg.rateRPM, cfg.rateFailClosed, cfg.redis)
	}
	// Every request in flight can check its limit without waiting for a
	// Redis connection.
	if n := cfg.redis.Options().PoolSize; n != 32 {
		t.Errorf("Redis pool size = %d, want SENTINEL_MAX_CONCURRENT (32)", n)
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

func upstreamNames(cfg config) []string {
	var names []string
	for _, u := range cfg.upstreams {
		names = append(names, u.spec.name)
	}
	return names
}

func TestLoadConfig_Upstreams(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    []string // enabled upstreams, in order
		ignored []string // key variables set for an unlisted upstream
		wantErr string   // the variable the error must start with
	}{
		{name: "default list", env: map[string]string{"ANTHROPIC_API_KEY": "sk"}, want: []string{"anthropic"}},
		{
			name: "both, in listed order",
			env:  map[string]string{"SENTINEL_UPSTREAMS": " openai , anthropic ", "ANTHROPIC_API_KEY": "sk", "OPENAI_API_KEY": "sk-o"},
			want: []string{"openai", "anthropic"},
		},
		{
			name: "openai only, without ANTHROPIC_API_KEY",
			env:  map[string]string{"SENTINEL_UPSTREAMS": "openai", "OPENAI_API_KEY": "sk-o"},
			want: []string{"openai"},
		},
		{name: "unknown name", env: map[string]string{"SENTINEL_UPSTREAMS": "anthropic,gemini", "ANTHROPIC_API_KEY": "sk"}, wantErr: "SENTINEL_UPSTREAMS"},
		{name: "names are case-sensitive", env: map[string]string{"SENTINEL_UPSTREAMS": "OpenAI", "OPENAI_API_KEY": "sk-o"}, wantErr: "SENTINEL_UPSTREAMS"},
		{name: "duplicate", env: map[string]string{"SENTINEL_UPSTREAMS": "openai,openai", "OPENAI_API_KEY": "sk-o"}, wantErr: "SENTINEL_UPSTREAMS"},
		{name: "empty list", env: map[string]string{"SENTINEL_UPSTREAMS": " , ", "ANTHROPIC_API_KEY": "sk"}, wantErr: "SENTINEL_UPSTREAMS"},
		{name: "anthropic listed without its key", env: map[string]string{"SENTINEL_UPSTREAMS": "anthropic,openai", "OPENAI_API_KEY": "sk-o"}, wantErr: "ANTHROPIC_API_KEY"},
		{name: "openai listed without its key", env: map[string]string{"SENTINEL_UPSTREAMS": "anthropic,openai", "ANTHROPIC_API_KEY": "sk"}, wantErr: "OPENAI_API_KEY"},
		{
			name:    "unlisted key is ignored",
			env:     map[string]string{"ANTHROPIC_API_KEY": "sk", "OPENAI_API_KEY": "sk-o", "OPENAI_BASE_URL": "not validated, not used"},
			want:    []string{"anthropic"},
			ignored: []string{"OPENAI_API_KEY"},
		},
		{
			name:    "OPENAI_BASE_URL plain http to a non-loopback host",
			env:     map[string]string{"SENTINEL_UPSTREAMS": "openai", "OPENAI_API_KEY": "sk-o", "OPENAI_BASE_URL": "http://api.openai.com/v1"},
			wantErr: "OPENAI_BASE_URL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{"SENTINEL_API_KEYS": "k1"}
			maps.Copy(vars, tt.env)
			if tt.wantErr != "" {
				wantConfigErr(t, vars, tt.wantErr)
				return
			}
			cfg, err := loadConfig(env(vars))
			if err != nil {
				t.Fatalf("loadConfig: %v", err)
			}
			if got := upstreamNames(cfg); !slices.Equal(got, tt.want) {
				t.Errorf("upstreams = %q, want %q", got, tt.want)
			}
			var ignored []string
			for _, s := range cfg.ignored {
				ignored = append(ignored, s.keyVar)
			}
			if !slices.Equal(ignored, tt.ignored) {
				t.Errorf("ignored keys = %q, want %q", ignored, tt.ignored)
			}
		})
	}
}

func TestLoadConfig_UpstreamErrorText(t *testing.T) {
	tests := []struct {
		upstreams, want string
	}{
		{"anthropic,gemini", `SENTINEL_UPSTREAMS: unknown upstream "gemini"; known upstreams are anthropic, openai`},
		{"openai,openai", `SENTINEL_UPSTREAMS: upstream "openai" is listed more than once`},
		{",", `SENTINEL_UPSTREAMS: "," lists no upstreams; known upstreams are anthropic, openai`},
		{"openai", "OPENAI_API_KEY is not set; it is required while SENTINEL_UPSTREAMS lists openai"},
	}
	for _, tt := range tests {
		_, err := loadConfig(env(map[string]string{"SENTINEL_UPSTREAMS": tt.upstreams, "SENTINEL_API_KEYS": "k1"}))
		if err == nil || err.Error() != tt.want {
			t.Errorf("SENTINEL_UPSTREAMS=%q: err = %v, want %q", tt.upstreams, err, tt.want)
		}
	}
}

// TestLoadConfig_BaseURLSecretsNotEchoed checks that a rejected base URL's
// userinfo or query, which may hold a key, stays out of the startup error.
func TestLoadConfig_BaseURLSecretsNotEchoed(t *testing.T) {
	tests := []struct{ urlVar, url, want string }{
		{"OPENAI_BASE_URL", "https://user:secret@api.openai.com/v1", "OPENAI_BASE_URL: openai: base URL for https://api.openai.com must not contain credentials"},
		{"OPENAI_BASE_URL", "https://api.openai.com/v1?api-key=sk-secret-123", "OPENAI_BASE_URL: openai: base URL for https://api.openai.com must not contain a query"},
		{"OPENAI_BASE_URL", "https://user:secret@api.openai.com:port/v1", "OPENAI_BASE_URL: openai: base URL is not a valid URL"},
		{"ANTHROPIC_BASE_URL", "https://user:secret@api.anthropic.com", "ANTHROPIC_BASE_URL: anthropic: base URL for https://api.anthropic.com must not contain credentials"},
		{"ANTHROPIC_BASE_URL", "https://api.anthropic.com/?api-key=sk-secret-123", "ANTHROPIC_BASE_URL: anthropic: base URL for https://api.anthropic.com must not contain a query"},
	}
	for _, tt := range tests {
		_, err := loadConfig(env(map[string]string{
			"SENTINEL_UPSTREAMS": "anthropic,openai",
			"ANTHROPIC_API_KEY":  "sk",
			"OPENAI_API_KEY":     "sk-o",
			"SENTINEL_API_KEYS":  "k1",
			tt.urlVar:            tt.url,
		}))
		if err == nil || err.Error() != tt.want {
			t.Errorf("%s=%q: err = %v, want %q", tt.urlVar, tt.url, err, tt.want)
		}
		if err != nil && (strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "sk-secret-123")) {
			t.Errorf("%s=%q: error %q repeats a secret", tt.urlVar, tt.url, err)
		}
	}
}

func TestLoadConfig_RejectsMalformedUpstreamKeys(t *testing.T) {
	for _, keyVar := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY"} {
		for key, problem := range map[string]string{
			" ":             "has leading or trailing whitespace",
			"key-secret ":   "has leading or trailing whitespace",
			" key-secret":   "has leading or trailing whitespace",
			"key-secret\r":  "has leading or trailing whitespace",
			"key\x00secret": "contains a control character",
			"key\x7fsecret": "contains a control character",
		} {
			vars := map[string]string{
				"SENTINEL_UPSTREAMS": "anthropic,openai",
				"ANTHROPIC_API_KEY":  "sk",
				"OPENAI_API_KEY":     "sk-o",
				"SENTINEL_API_KEYS":  "k1",
			}
			vars[keyVar] = key
			_, err := loadConfig(env(vars))
			name := strings.ToLower(strings.TrimSuffix(keyVar, "_API_KEY"))
			want := keyVar + " " + problem + "; it is required while SENTINEL_UPSTREAMS lists " + name
			if err == nil || err.Error() != want {
				t.Errorf("%s=%q: err = %v, want %q", keyVar, key, err, want)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Errorf("%s=%q: error %q repeats the key", keyVar, key, err)
			}
		}
	}
	// A tab inside a key is a legal header character.
	if _, err := loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "a\tb", "SENTINEL_API_KEYS": "k1"})); err != nil {
		t.Errorf("key with an inner tab: %v", err)
	}
}

// TestProviderError checks the attribution of provider construction errors.
// Driving a non-URL failure through loadConfig would mean faking the system
// trust store, so this tests the attribution on its own.
func TestProviderError(t *testing.T) {
	spec := &upstreamSpecs[1] // openai
	urlErr := fmt.Errorf("openai: %w", upstream.ValidateBaseURL("http://api.openai.com/v1"))
	if got, want := providerError(spec, urlErr).Error(),
		"OPENAI_BASE_URL: openai: base URL for http://api.openai.com must use https (http is allowed only for loopback hosts)"; got != want {
		t.Errorf("base URL error = %q, want %q", got, want)
	}
	other := fmt.Errorf("openai: %w", errors.New("load system trust store: no roots"))
	if got, want := providerError(spec, other).Error(), "upstream openai: openai: load system trust store: no roots"; got != want {
		t.Errorf("other error = %q, want %q", got, want)
	}
}

func TestLogUpstreams(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"SENTINEL_UPSTREAMS": "openai",
		"OPENAI_API_KEY":     "sk-openai-secret",
		"OPENAI_BASE_URL":    "http://127.0.0.1:11434/v1",
		"ANTHROPIC_API_KEY":  "sk-ant-secret",
		"SENTINEL_API_KEYS":  "k1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	logUpstreams(slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	lines := slices.Collect(strings.Lines(buf.String()))
	if len(lines) != 2 {
		t.Fatalf("log = %q, want 2 lines", lines)
	}
	for i, want := range []string{
		`level=INFO msg="upstream enabled" upstream=openai base_url=http://127.0.0.1:11434/v1`,
		`level=WARN msg="ANTHROPIC_API_KEY is set but anthropic is not in SENTINEL_UPSTREAMS; ignoring"`,
	} {
		if !strings.Contains(lines[i], want) {
			t.Errorf("line %d = %q, want it to contain %q", i, lines[i], want)
		}
	}
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("log contains an upstream key:\n%s", buf.String())
	}

	// The default base URL is logged when the variable is unset.
	cfg, err = loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}))
	if err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	logUpstreams(slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	if want := "upstream=anthropic base_url=https://api.anthropic.com\n"; !strings.HasSuffix(buf.String(), want) {
		t.Errorf("log = %q, want it to end with %q", buf.String(), want)
	}
}

func TestNewMux_RoutesOnlyEnabledUpstreams(t *testing.T) {
	const (
		messages = "/v1/messages"
		chat     = "/v1/chat/completions"
	)
	tests := []struct {
		upstreams string
		enabled   []string
	}{
		{"anthropic", []string{messages}},
		{"openai", []string{chat}},
		{"anthropic,openai", []string{messages, chat}},
	}
	for _, tt := range tests {
		t.Run(tt.upstreams, func(t *testing.T) {
			cfg, err := loadConfig(env(map[string]string{
				"SENTINEL_UPSTREAMS": tt.upstreams,
				"ANTHROPIC_API_KEY":  "sk",
				"OPENAI_API_KEY":     "sk-o",
				"SENTINEL_API_KEYS":  "k1",
			}))
			if err != nil {
				t.Fatal(err)
			}
			mux := newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{})
			// Without a key an enabled route answers 401 from RequireKey and
			// never reaches the upstream; a disabled one is not routed.
			for _, path := range []string{messages, chat} {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
				want := http.StatusNotFound
				if slices.Contains(tt.enabled, path) {
					want = http.StatusUnauthorized
				}
				if rec.Code != want {
					t.Errorf("POST %s: status = %d, want %d", path, rec.Code, want)
				}
			}
		})
	}
}

// TestNewMux_SharedAdmission holds the only slot with a request on
// /v1/messages, so /v1/chat/completions must shed its request.
func TestNewMux_SharedAdmission(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	held := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer held.Close()
	defer close(release) // runs first, so Close doesn't wait on the held handler

	cfg, err := loadConfig(env(map[string]string{
		"SENTINEL_UPSTREAMS":      "anthropic,openai",
		"SENTINEL_MAX_CONCURRENT": "1",
		"ANTHROPIC_API_KEY":       "sk",
		"ANTHROPIC_BASE_URL":      held.URL,
		"OPENAI_API_KEY":          "sk-o",
		"OPENAI_BASE_URL":         "http://127.0.0.1:1/v1", // never reached
		"SENTINEL_API_KEYS":       "k1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}))
	defer srv.Close()

	// Cancelled before srv.Close, which would wait for it.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/messages", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Api-Key", "k1")
	go func() {
		if resp, err := srv.Client().Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached upstream")
	}

	chatReq, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	chatReq.Header.Set("Authorization", "Bearer k1")
	resp, err := srv.Client().Do(chatReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body %q", resp.StatusCode, body)
	}
	// In the chat route's own protocol.
	want := `{"error":{"message":"gateway is at capacity","type":"server_error","param":null,"code":"overloaded"}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}
