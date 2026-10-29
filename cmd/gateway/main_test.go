package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/devcert"
	"github.com/shahardalva/sentinel/internal/provider/anthropic"
	"github.com/shahardalva/sentinel/internal/provider/upstream"
	"github.com/shahardalva/sentinel/internal/proxy"
)

// env returns a getenv and an environ that read only from vars, so tests
// don't see the real environment. environ lists vars sorted by key.
func env(vars map[string]string) (func(string) string, func() []string) {
	getenv := func(key string) string { return vars[key] }
	environ := func() []string {
		var kv []string
		for _, k := range slices.Sorted(maps.Keys(vars)) {
			kv = append(kv, k+"="+vars[k])
		}
		return kv
	}
	return getenv, environ
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
		{
			"CA, custom upstreams on public hosts", ca,
			map[string]string{
				"SENTINEL_UPSTREAMS":                "oai2,claude,xai",
				"SENTINEL_UPSTREAM_OAI2_PROTOCOL":   "openai",
				"SENTINEL_UPSTREAM_OAI2_BASE_URL":   "https://API.OpenAI.com./v1",
				"SENTINEL_UPSTREAM_OAI2_API_KEY":    "sk",
				"SENTINEL_UPSTREAM_CLAUDE_PROTOCOL": "anthropic",
				"SENTINEL_UPSTREAM_CLAUDE_BASE_URL": "https://api.anthropic.com",
				"SENTINEL_UPSTREAM_CLAUDE_API_KEY":  "sk",
				"SENTINEL_UPSTREAM_XAI_PROTOCOL":    "openai",
				"SENTINEL_UPSTREAM_XAI_BASE_URL":    "https://api.x.ai/v1",
				"SENTINEL_UPSTREAM_XAI_API_KEY":     "sk",
			},
			[]string{
				"default OpenAI API; anyone holding one of their keys could impersonate it and read SENTINEL_UPSTREAM_OAI2_API_KEY",
				"default Anthropic API; anyone holding one of their keys could impersonate it and read SENTINEL_UPSTREAM_CLAUDE_API_KEY",
			},
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

func TestStartRateLimit(t *testing.T) {
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
	startRateLimit(context.Background(), logger, cfg)
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
		{name: "unknown name is a custom upstream", env: map[string]string{"SENTINEL_UPSTREAMS": "anthropic,gemini", "ANTHROPIC_API_KEY": "sk"}, wantErr: "SENTINEL_UPSTREAM_GEMINI_PROTOCOL"},
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
		{"anthropic,Gemini", `SENTINEL_UPSTREAMS: upstream name "Gemini" must be a lowercase letter followed by up to 31 lowercase letters or digits`},
		{"v1", `SENTINEL_UPSTREAMS: upstream name "v1" is reserved: its routes would start /v1/v1/`},
		{"gemini", "SENTINEL_UPSTREAM_GEMINI_PROTOCOL is not set; it is required while SENTINEL_UPSTREAMS lists gemini"},
		{"openai,openai", `SENTINEL_UPSTREAMS: upstream "openai" is listed more than once`},
		{",", `SENTINEL_UPSTREAMS: "," lists no upstreams`},
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
		"level=INFO msg=\"upstream enabled\" upstream=openai protocol=openai base_url=http://127.0.0.1:11434\n",
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
	if want := "upstream=anthropic protocol=anthropic base_url=https://api.anthropic.com\n"; !strings.HasSuffix(buf.String(), want) {
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
			mux := newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}, staticKeys(cfg))
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
	srv := httptest.NewServer(newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}, staticKeys(cfg)))
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

// customVars returns the variables for a custom upstream named name.
func customVars(name, protocol, baseURL, key string) map[string]string {
	prefix := "SENTINEL_UPSTREAM_" + strings.ToUpper(name) + "_"
	return map[string]string{prefix + "PROTOCOL": protocol, prefix + "BASE_URL": baseURL, prefix + "API_KEY": key}
}

func TestLoadConfig_CustomUpstreams(t *testing.T) {
	vars := map[string]string{"SENTINEL_UPSTREAMS": "xai, anthropic, local", "ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}
	maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-key"))
	maps.Copy(vars, customVars("local", " anthropic ", "http://127.0.0.1:9090", "local-key"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got := upstreamNames(cfg); !slices.Equal(got, []string{"xai", "anthropic", "local"}) {
		t.Fatalf("upstreams = %q", got)
	}
	tests := []struct {
		protocol, baseURL, keyVar string
		routes                    []string
	}{
		{"openai", "https://api.x.ai/v1", "SENTINEL_UPSTREAM_XAI_API_KEY", []string{"POST /xai/v1/chat/completions"}},
		{"anthropic", anthropic.DefaultBaseURL, "ANTHROPIC_API_KEY", []string{"POST /v1/messages", "POST /anthropic/v1/messages"}},
		{"anthropic", "http://127.0.0.1:9090", "SENTINEL_UPSTREAM_LOCAL_API_KEY", []string{"POST /local/v1/messages"}},
	}
	for i, want := range tests {
		u := cfg.upstreams[i]
		if u.spec.protocol.name != want.protocol || u.provider.Name() != want.protocol || u.baseURL != want.baseURL ||
			u.spec.keyVar != want.keyVar || !slices.Equal(u.spec.routes(), want.routes) {
			t.Errorf("upstream %s: protocol %s, provider %s, base URL %s, key %s, routes %q; want %+v",
				u.spec.name, u.spec.protocol.name, u.provider.Name(), u.baseURL, u.spec.keyVar, u.spec.routes(), want)
		}
	}
	if cfg.ignored != nil || cfg.ignoredCustom != nil {
		t.Errorf("ignored = %v, %q; want none", cfg.ignored, cfg.ignoredCustom)
	}
}

func TestLoadConfig_UpstreamNames(t *testing.T) {
	long := strings.Repeat("a", 32)
	for _, name := range []string{"a", "a1", long} {
		vars := map[string]string{"SENTINEL_UPSTREAMS": name, "SENTINEL_API_KEYS": "k1"}
		maps.Copy(vars, customVars(name, "openai", "https://example.com/v1", "sk"))
		if _, err := loadConfig(env(vars)); err != nil {
			t.Errorf("name %q: %v", name, err)
		}
	}
	for _, name := range []string{"Xai", "XAI", "x-ai", "x_ai", "1ai", long + "a", "v1", "x.ai", "x/ai"} {
		vars := map[string]string{"SENTINEL_UPSTREAMS": "anthropic," + name, "ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}
		maps.Copy(vars, customVars(name, "openai", "https://example.com/v1", "sk"))
		t.Run(name, func(t *testing.T) { wantConfigErr(t, vars, "SENTINEL_UPSTREAMS:") })
	}
}

func TestLoadConfig_CustomUpstreamRequiresVars(t *testing.T) {
	tests := []struct {
		name    string
		unset   string            // field left unset
		set     map[string]string // overrides
		wantErr string
	}{
		{name: "no protocol", unset: "PROTOCOL", wantErr: "SENTINEL_UPSTREAM_XAI_PROTOCOL is not set; it is required while SENTINEL_UPSTREAMS lists xai"},
		{name: "no base URL", unset: "BASE_URL", wantErr: "SENTINEL_UPSTREAM_XAI_BASE_URL is not set; it is required while SENTINEL_UPSTREAMS lists xai"},
		{name: "no key", unset: "API_KEY", wantErr: "SENTINEL_UPSTREAM_XAI_API_KEY is not set; it is required while SENTINEL_UPSTREAMS lists xai"},
		{name: "unknown protocol", set: map[string]string{"SENTINEL_UPSTREAM_XAI_PROTOCOL": "grpc"}, wantErr: `SENTINEL_UPSTREAM_XAI_PROTOCOL must be "anthropic" or "openai"`},
		{name: "protocols are case-sensitive", set: map[string]string{"SENTINEL_UPSTREAM_XAI_PROTOCOL": "OpenAI"}, wantErr: `SENTINEL_UPSTREAM_XAI_PROTOCOL must be "anthropic" or "openai"`},
		{
			name:    "plain-http base URL",
			set:     map[string]string{"SENTINEL_UPSTREAM_XAI_BASE_URL": "http://api.x.ai/v1"},
			wantErr: "SENTINEL_UPSTREAM_XAI_BASE_URL: openai: base URL for http://api.x.ai must use https (http is allowed only for loopback hosts)",
		},
		{
			name:    "key with trailing whitespace",
			set:     map[string]string{"SENTINEL_UPSTREAM_XAI_API_KEY": "xai-key "},
			wantErr: "SENTINEL_UPSTREAM_XAI_API_KEY has leading or trailing whitespace; it is required while SENTINEL_UPSTREAMS lists xai",
		},
		{
			name:    "key with a control character",
			set:     map[string]string{"SENTINEL_UPSTREAM_XAI_API_KEY": "xai\x00key"},
			wantErr: "SENTINEL_UPSTREAM_XAI_API_KEY contains a control character; it is required while SENTINEL_UPSTREAMS lists xai",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{"SENTINEL_UPSTREAMS": "xai", "SENTINEL_API_KEYS": "k1"}
			maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-key"))
			delete(vars, "SENTINEL_UPSTREAM_XAI_"+tt.unset)
			maps.Copy(vars, tt.set)
			_, err := loadConfig(env(vars))
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadConfig_RejectsBuiltinUpstreamVars(t *testing.T) {
	for _, v := range []string{
		"SENTINEL_UPSTREAM_OPENAI_BASE_URL",
		"SENTINEL_UPSTREAM_OPENAI_API_KEY",
		"SENTINEL_UPSTREAM_OPENAI_PROTOCOL",
		"SENTINEL_UPSTREAM_OPENAI_ANYTHING",
		"SENTINEL_UPSTREAM_ANTHROPIC_API_KEY",
	} {
		// Rejected whether or not the built-in is listed.
		for _, list := range []string{"anthropic", "anthropic,openai"} {
			_, err := loadConfig(env(map[string]string{
				"SENTINEL_UPSTREAMS": list,
				"ANTHROPIC_API_KEY":  "sk",
				"OPENAI_API_KEY":     "sk-o",
				"SENTINEL_API_KEYS":  "k1",
				v:                    "sk-secret-value",
			}))
			if err == nil || !strings.HasPrefix(err.Error(), v+": ") || strings.Contains(err.Error(), "secret") {
				t.Errorf("%s with SENTINEL_UPSTREAMS=%s: err = %v, want an error naming %s without its value", v, list, err, v)
			}
		}
	}
	_, err := loadConfig(env(map[string]string{
		"OPENAI_API_KEY":                    "sk-o",
		"SENTINEL_UPSTREAMS":                "openai",
		"SENTINEL_API_KEYS":                 "k1",
		"SENTINEL_UPSTREAM_OPENAI_BASE_URL": "https://api.x.ai/v1",
	}))
	want := "SENTINEL_UPSTREAM_OPENAI_BASE_URL: openai is a built-in upstream with a fixed protocol; set OPENAI_API_KEY and OPENAI_BASE_URL instead"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
	// Set but empty counts as unset, as it does for every other variable.
	if _, err := loadConfig(env(map[string]string{
		"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1", "SENTINEL_UPSTREAM_OPENAI_API_KEY": "",
	})); err != nil {
		t.Errorf("empty SENTINEL_UPSTREAM_OPENAI_API_KEY: %v", err)
	}
}

func TestLoadConfig_UnlistedCustomUpstreams(t *testing.T) {
	vars := map[string]string{
		"SENTINEL_UPSTREAMS":        "anthropic,xai",
		"ANTHROPIC_API_KEY":         "sk",
		"SENTINEL_API_KEYS":         "k1",
		"SENTINEL_UPSTREAM_CA_FILE": writeFile(t, generate(t).CACert), // not an upstream's
		// Not validated and not used: a plain-http public URL would stop
		// startup if groq were listed.
		"SENTINEL_UPSTREAM_GROQ_API_KEY":    "gsk-secret-1",
		"SENTINEL_UPSTREAM_GROQ_BASE_URL":   "http://user:secret@api.groq.com/openai/v1",
		"SENTINEL_UPSTREAM_OLLAMA_PROTOCOL": "openai",
		"SENTINEL_UPSTREAM_EMPTY_API_KEY":   "",       // empty counts as unset
		"SENTINEL_UPSTREAM_OTHER_SETTING":   "secret", // not a field
		"sentinel_upstream_lower_api_key":   "secret", // names are compared ignoring case
		"SENTINEL_UPSTREAM_X-AI_API_KEY":    "secret", // not a valid name
	}
	maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-secret"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got := upstreamNames(cfg); !slices.Equal(got, []string{"anthropic", "xai"}) {
		t.Errorf("upstreams = %q", got)
	}
	if !slices.Equal(cfg.ignoredCustom, []string{"groq", "lower", "ollama"}) {
		t.Errorf("ignoredCustom = %q, want [groq lower ollama]", cfg.ignoredCustom)
	}

	var buf strings.Builder
	logUpstreams(slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	var warns []string
	for line := range strings.Lines(buf.String()) {
		if strings.Contains(line, "level=WARN") {
			warns = append(warns, line)
		}
	}
	wantWarns := []string{
		`msg="SENTINEL_UPSTREAM_GROQ_* is set but groq is not in SENTINEL_UPSTREAMS; ignoring"`,
		`msg="SENTINEL_UPSTREAM_LOWER_* is set but lower is not in SENTINEL_UPSTREAMS; ignoring"`,
		`msg="SENTINEL_UPSTREAM_OLLAMA_* is set but ollama is not in SENTINEL_UPSTREAMS; ignoring"`,
	}
	if len(warns) != len(wantWarns) {
		t.Fatalf("warnings = %q, want %d", warns, len(wantWarns))
	}
	for i, want := range wantWarns {
		if !strings.Contains(warns[i], want) {
			t.Errorf("warning %d = %q, want %q", i, warns[i], want)
		}
	}
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "groq.com") {
		t.Errorf("log repeats a value:\n%s", buf.String())
	}
}

// TestLoadConfig_CustomUpstreamSecretsNotEchoed checks that no error or log
// line repeats a custom upstream's key or its base URL's userinfo or query.
func TestLoadConfig_CustomUpstreamSecretsNotEchoed(t *testing.T) {
	tests := []struct{ field, value, want string }{
		{"BASE_URL", "https://user:secret@api.x.ai/v1", "SENTINEL_UPSTREAM_XAI_BASE_URL: openai: base URL for https://api.x.ai must not contain credentials"},
		{"BASE_URL", "https://api.x.ai/v1?key=sk-secret-123", "SENTINEL_UPSTREAM_XAI_BASE_URL: openai: base URL for https://api.x.ai must not contain a query"},
		{"BASE_URL", "https://user:secret@api.x.ai:port/v1", "SENTINEL_UPSTREAM_XAI_BASE_URL: openai: base URL is not a valid URL"},
		{"API_KEY", " xai-secret", "SENTINEL_UPSTREAM_XAI_API_KEY has leading or trailing whitespace; it is required while SENTINEL_UPSTREAMS lists xai"},
		{"API_KEY", "xai-secret\n", "SENTINEL_UPSTREAM_XAI_API_KEY has leading or trailing whitespace; it is required while SENTINEL_UPSTREAMS lists xai"},
		{"PROTOCOL", "secret", `SENTINEL_UPSTREAM_XAI_PROTOCOL must be "anthropic" or "openai"`},
	}
	for _, tt := range tests {
		vars := map[string]string{"SENTINEL_UPSTREAMS": "xai", "SENTINEL_API_KEYS": "k1"}
		maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-key"))
		vars["SENTINEL_UPSTREAM_XAI_"+tt.field] = tt.value
		_, err := loadConfig(env(vars))
		if err == nil || err.Error() != tt.want {
			t.Errorf("%s=%q: err = %v, want %q", tt.field, tt.value, err, tt.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%s=%q: error %q repeats a secret", tt.field, tt.value, err)
		}
	}

	// A valid custom upstream logs only its base URL's scheme, host and
	// port, never its key or its path, which may hold a token. The CA
	// warning does the same.
	vars := map[string]string{"SENTINEL_UPSTREAMS": "gw", "SENTINEL_API_KEYS": "k1"}
	maps.Copy(vars, customVars("gw", "openai", "https://API.openai.com:8443/k/sk-path-secret/v1", "gw-secret"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	logUpstreams(logger, cfg)
	logCAs(logger, "ca.pem", nil, cfg.upstreams)
	for _, want := range []string{
		"msg=\"upstream enabled\" upstream=gw protocol=openai base_url=https://API.openai.com:8443\n",
		"upstream=gw base_url=https://API.openai.com:8443\n",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log = %q, want it to contain %q", buf.String(), want)
		}
	}
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "/v1") {
		t.Errorf("log contains the key or the path:\n%s", buf.String())
	}
}

// recordingUpstream is an OpenAI-compatible upstream that records the
// Authorization header of every request it gets.
type recordingUpstream struct {
	*httptest.Server
	mu    sync.Mutex
	auths []string
}

func newRecordingUpstream(t *testing.T) *recordingUpstream {
	t.Helper()
	u := &recordingUpstream{}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.auths = append(u.auths, r.Header.Get("Authorization"))
		u.mu.Unlock()
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"object":"chat.completion"}`))
	}))
	t.Cleanup(u.Close)
	return u
}

func (u *recordingUpstream) received() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.auths)
}

func TestNewMux_CustomUpstreams(t *testing.T) {
	alpha, beta := newRecordingUpstream(t), newRecordingUpstream(t)
	vars := map[string]string{"SENTINEL_UPSTREAMS": "alpha,beta", "SENTINEL_API_KEYS": "k1"}
	maps.Copy(vars, customVars("alpha", "openai", alpha.URL+"/v1", "alpha-upstream-key"))
	maps.Copy(vars, customVars("beta", "openai", beta.URL+"/v1", "beta-upstream-key"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	srv := httptest.NewServer(closeUnmatched(newMux(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), proxy.Options{}, staticKeys(cfg))))
	defer srv.Close()

	send := func(path string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer k1")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp
	}

	for range 2 {
		for _, path := range []string{"/alpha/v1/chat/completions", "/beta/v1/chat/completions"} {
			if resp := send(path); resp.StatusCode != http.StatusOK {
				t.Errorf("POST %s: status %d, want 200", path, resp.StatusCode)
			}
		}
	}
	// Each prefix reaches only its own server, with only its own key.
	for _, tt := range []struct {
		u    *recordingUpstream
		want string
	}{{alpha, "Bearer alpha-upstream-key"}, {beta, "Bearer beta-upstream-key"}} {
		got := tt.u.received()
		if len(got) != 2 || slices.ContainsFunc(got, func(a string) bool { return a != tt.want }) {
			t.Errorf("upstream got Authorization %q, want %q twice", got, tt.want)
		}
	}

	// No unprefixed route without a built-in upstream, no route for an
	// unknown prefix or for another protocol's path, and each 404 closes
	// the connection.
	for _, path := range []string{
		"/v1/chat/completions", "/v1/messages", "/gamma/v1/chat/completions",
		"/alpha/v1/messages", "/alpha/chat/completions", "/v1/alpha/v1/chat/completions",
	} {
		resp := send(path)
		if resp.StatusCode != http.StatusNotFound || !resp.Close {
			t.Errorf("POST %s: status %d, close %v; want 404 with Connection: close", path, resp.StatusCode, resp.Close)
		}
	}
	if n := len(alpha.received()) + len(beta.received()); n != 4 {
		t.Errorf("upstreams got %d requests, want 4", n)
	}

	// The log line names the upstream and the protocol's provider.
	var got []string
	for line := range strings.Lines(logs.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse log line %q: %v", line, err)
		}
		if m["msg"] == "request" {
			got = append(got, fmt.Sprintf("%v/%v", m["upstream"], m["provider"]))
		}
	}
	if want := []string{"alpha/openai", "beta/openai", "alpha/openai", "beta/openai"}; !slices.Equal(got, want) {
		t.Errorf("logged upstream/provider = %q, want %q", got, want)
	}
}

// TestNewMux_UnprefixedRoutesOnlyForBuiltins checks which routes exist with
// built-in and custom upstreams side by side.
func TestNewMux_UnprefixedRoutesOnlyForBuiltins(t *testing.T) {
	vars := map[string]string{
		"SENTINEL_UPSTREAMS": "anthropic,openai,xai,claude",
		"ANTHROPIC_API_KEY":  "sk",
		"OPENAI_API_KEY":     "sk-o",
		"SENTINEL_API_KEYS":  "k1",
	}
	maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-key"))
	maps.Copy(vars, customVars("claude", "anthropic", "https://example.com", "c-key"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	mux := newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}, staticKeys(cfg))
	// Without a key a route answers 401 from RequireKey and never reaches
	// the upstream.
	routed := []string{
		"/v1/messages", "/v1/chat/completions",
		"/anthropic/v1/messages", "/openai/v1/chat/completions",
		"/xai/v1/chat/completions", "/claude/v1/messages",
	}
	unrouted := []string{"/xai/v1/messages", "/claude/v1/chat/completions", "/anthropic/v1/chat/completions", "/groq/v1/chat/completions"}
	for _, path := range slices.Concat(routed, unrouted) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		want := http.StatusNotFound
		if slices.Contains(routed, path) {
			want = http.StatusUnauthorized
		}
		if rec.Code != want {
			t.Errorf("POST %s: status = %d, want %d", path, rec.Code, want)
		}
	}
}

// TestNewMux_SharedAdmissionAcrossPrefixes holds the only slot with a
// request on /alpha, so /beta and the built-in routes must shed theirs.
func TestNewMux_SharedAdmissionAcrossPrefixes(t *testing.T) {
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

	vars := map[string]string{
		"SENTINEL_UPSTREAMS":      "openai,alpha,beta",
		"SENTINEL_MAX_CONCURRENT": "1",
		"OPENAI_API_KEY":          "sk-o",
		"OPENAI_BASE_URL":         "http://127.0.0.1:1/v1", // never reached
		"SENTINEL_API_KEYS":       "k1",
	}
	maps.Copy(vars, customVars("alpha", "openai", held.URL+"/v1", "a-key"))
	maps.Copy(vars, customVars("beta", "openai", "http://127.0.0.1:1/v1", "b-key")) // never reached
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}, staticKeys(cfg)))
	defer srv.Close()

	// Cancelled before srv.Close, which would wait for it.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/alpha/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer k1")
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

	for _, path := range []string{"/beta/v1/chat/completions", "/v1/chat/completions", "/openai/v1/chat/completions"} {
		r, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer k1")
		resp, err := srv.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), `"code":"overloaded"`) {
			t.Errorf("POST %s: status %d, body %q; want 503 overloaded", path, resp.StatusCode, body)
		}
	}
}

// TestNewMux_RefusesUpstreamRedirects has an upstream of each protocol
// answer 307 to a second server. The gateway must not follow it: the second
// server never sees a request, so neither key reaches it, and the client
// gets a 502 whose log line names the cause.
func TestNewMux_RefusesUpstreamRedirects(t *testing.T) {
	var hits sync.Map // header name -> value, as the second server saw them
	var other atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		other.Add(1)
		hits.Store("X-Api-Key", r.Header.Get("X-Api-Key"))
		hits.Store("Authorization", r.Header.Get("Authorization"))
	}))
	defer elsewhere.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	vars := map[string]string{"SENTINEL_UPSTREAMS": "ant,oai", "SENTINEL_API_KEYS": "k1"}
	maps.Copy(vars, customVars("ant", "anthropic", redirector.URL, "ant-upstream-secret"))
	maps.Copy(vars, customVars("oai", "openai", redirector.URL+"/v1", "oai-upstream-secret"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	srv := httptest.NewServer(newMux(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), proxy.Options{}, staticKeys(cfg)))
	defer srv.Close()

	for _, path := range []string{"/ant/v1/messages", "/oai/v1/chat/completions"} {
		logs.Reset()
		req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer k1")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Errorf("POST %s: status %d, want 502", path, resp.StatusCode)
		}
		if !strings.Contains(logs.String(), "upstream redirect refused") {
			t.Errorf("POST %s: log %q does not name the refused redirect", path, logs.String())
		}
	}
	if n := other.Load(); n != 0 {
		var seen []string
		hits.Range(func(k, v any) bool { seen = append(seen, fmt.Sprintf("%v=%v", k, v)); return true })
		t.Errorf("the redirect target got %d requests, last with %q", n, seen)
	}
}

// TestGateway_UncleanPathsDoNotHang sends each path over a raw connection
// with headers declaring a body it never finishes. Every one must get a
// prompt answer and a closed connection: a redirect to the cleaned path
// would leave net/http draining the body with no deadline.
func TestGateway_UncleanPathsDoNotHang(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"ANTHROPIC_API_KEY": "sk", "SENTINEL_API_KEYS": "k1"}))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(closeUnmatched(newMux(cfg, slog.New(slog.DiscardHandler), proxy.Options{}, staticKeys(cfg))))
	defer srv.Close()

	tests := []struct {
		path string
		want int
	}{
		{"/nope", http.StatusNotFound},
		{"//anthropic/v1/messages", http.StatusBadRequest},
		{"/anthropic/./v1/messages", http.StatusBadRequest},
		{"/anthropic//v1/messages", http.StatusBadRequest},
		{"/x/../anthropic/v1/messages", http.StatusBadRequest},
		{"/anthropic/v1/messages/", http.StatusBadRequest},
		{"//v1/messages", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			conn, err := net.Dial("tcp", srv.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(proxy.RejectDrainTimeout + 2*time.Second))
			fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: gw\r\nX-Api-Key: k1\r\nContent-Length: 100000\r\n\r\n{\"m", tt.path)
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Fatalf("no response while the body was incomplete: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tt.want || resp.Header.Get("Location") != "" {
				t.Errorf("status %d, Location %q, body %q; want %d and no redirect", resp.StatusCode, resp.Header.Get("Location"), body, tt.want)
			}
			if tt.want == http.StatusBadRequest && !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
				t.Errorf("Content-Type = %q, want text/plain", resp.Header.Get("Content-Type"))
			}
			// EOF or a reset both mean the server let go; only a timeout
			// means it is still holding the connection.
			if _, err := br.ReadByte(); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("connection not closed after the response: %v", err)
			}
		})
	}
}

// TestLoadConfig_BuiltinUpstreamVarsIgnoreCase checks the conflict check
// on names in any case: Windows getenv finds them that way, so they must
// not slip past it.
func TestLoadConfig_BuiltinUpstreamVarsIgnoreCase(t *testing.T) {
	for _, v := range []string{"sentinel_upstream_openai_base_url", "Sentinel_Upstream_Anthropic_Api_Key"} {
		_, err := loadConfig(env(map[string]string{
			"ANTHROPIC_API_KEY": "sk",
			"SENTINEL_API_KEYS": "k1",
			v:                   "sk-secret-value",
		}))
		if err == nil || !strings.HasPrefix(err.Error(), v+": ") || strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: err = %v, want an error naming %s without its value", v, err, v)
		}
	}
}

func TestLoadConfig_UnknownCustomUpstreamVars(t *testing.T) {
	vars := map[string]string{
		"SENTINEL_UPSTREAMS":              "xai,groq",
		"SENTINEL_API_KEYS":               "k1",
		"SENTINEL_UPSTREAM_XAI_APIKEY":    "xai-secret-typo",
		"SENTINEL_UPSTREAM_XAI_BASEURL":   "https://secret.example/v1",
		"SENTINEL_UPSTREAM_GROQ_TIMEOUT":  "30s",
		"SENTINEL_UPSTREAM_OTHER_APIKEY":  "secret", // unlisted, and not a field
		"SENTINEL_UPSTREAM_GROQ_PROTOCOL": "openai",
	}
	maps.Copy(vars, customVars("xai", "openai", "https://api.x.ai/v1", "xai-key"))
	maps.Copy(vars, customVars("groq", "openai", "https://api.groq.com/openai/v1", "gsk"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	var buf strings.Builder
	logUpstreams(slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	var warns []string
	for line := range strings.Lines(buf.String()) {
		if strings.Contains(line, "level=WARN") {
			warns = append(warns, line)
		}
	}
	// One warning per upstream, in SENTINEL_UPSTREAMS order.
	wantWarns := []string{
		`msg="SENTINEL_UPSTREAM_XAI_APIKEY, SENTINEL_UPSTREAM_XAI_BASEURL set for upstream xai, which reads only SENTINEL_UPSTREAM_XAI_PROTOCOL, _BASE_URL and _API_KEY; ignoring"`,
		`msg="SENTINEL_UPSTREAM_GROQ_TIMEOUT set for upstream groq, which reads only SENTINEL_UPSTREAM_GROQ_PROTOCOL, _BASE_URL and _API_KEY; ignoring"`,
	}
	if len(warns) != len(wantWarns) {
		t.Fatalf("warnings = %q, want %d", warns, len(wantWarns))
	}
	for i, want := range wantWarns {
		if !strings.Contains(warns[i], want) {
			t.Errorf("warning %d = %q, want %q", i, warns[i], want)
		}
	}
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "30s") {
		t.Errorf("log repeats a value:\n%s", buf.String())
	}
}

func TestLoadConfig_ReservedNames(t *testing.T) {
	for name, want := range map[string]string{
		"v1": `SENTINEL_UPSTREAMS: upstream name "v1" is reserved: its routes would start /v1/v1/`,
		"ca": `SENTINEL_UPSTREAMS: upstream name "ca" is reserved: its variables would share a prefix with SENTINEL_UPSTREAM_CA_FILE`,
	} {
		vars := map[string]string{"SENTINEL_UPSTREAMS": name, "SENTINEL_API_KEYS": "k1"}
		maps.Copy(vars, customVars(name, "openai", "https://example.com/v1", "sk"))
		if _, err := loadConfig(env(vars)); err == nil || err.Error() != want {
			t.Errorf("%s: err = %v, want %q", name, err, want)
		}
	}
}

// TestListen checks that "listening" is logged only once the port is bound.
func TestListen(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	ln, err := listen(logger, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if want := "msg=listening addr=" + ln.Addr().String() + "\n"; !strings.HasSuffix(buf.String(), want) {
		t.Errorf("log = %q, want it to end with %q", buf.String(), want)
	}

	buf.Reset()
	if ln2, err := listen(logger, ln.Addr().String()); err == nil {
		ln2.Close()
		t.Fatal("second listen on a bound port succeeded")
	}
	if buf.Len() != 0 {
		t.Errorf("a failed bind logged %q", buf.String())
	}
}

// TestNewMux_ForwardErrorsOmitURLs checks that a failed upstream call logs
// its cause without the request URL. A base URL's path may hold a token,
// and a refused redirect's target is whatever the upstream sent.
func TestNewMux_ForwardErrorsOmitURLs(t *testing.T) {
	elsewhere := httptest.NewServer(http.NotFoundHandler()) // never reached
	defer elsewhere.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Redirect(w, r, elsewhere.URL+"/tok/sk-target-secret", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	vars := map[string]string{"SENTINEL_UPSTREAMS": "deada,deado,redira,rediro", "SENTINEL_API_KEYS": "k1"}
	// Nothing listens on port 1.
	maps.Copy(vars, customVars("deada", "anthropic", "http://127.0.0.1:1/k/sk-path-secret", "sk"))
	maps.Copy(vars, customVars("deado", "openai", "http://127.0.0.1:1/k/sk-path-secret/v1", "sk"))
	maps.Copy(vars, customVars("redira", "anthropic", redirector.URL+"/k/sk-path-secret", "sk"))
	maps.Copy(vars, customVars("rediro", "openai", redirector.URL+"/k/sk-path-secret/v1", "sk"))
	cfg, err := loadConfig(env(vars))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	srv := httptest.NewServer(newMux(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), proxy.Options{}, staticKeys(cfg)))
	defer srv.Close()

	tests := []struct{ path, wantErr string }{
		{"/deada/v1/messages", "anthropic: forward: Post: dial tcp 127.0.0.1:1: "},
		{"/deado/v1/chat/completions", "openai: forward: Post: dial tcp 127.0.0.1:1: "},
		{"/redira/v1/messages", "anthropic: forward: Post: upstream redirect refused"},
		{"/rediro/v1/chat/completions", "openai: forward: Post: upstream redirect refused"},
	}
	for _, tt := range tests {
		logs.Reset()
		req, err := http.NewRequest(http.MethodPost, srv.URL+tt.path, strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer k1")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Errorf("POST %s: status %d, want 502", tt.path, resp.StatusCode)
		}
		var line map[string]any
		if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
			t.Fatalf("POST %s: parse log %q: %v", tt.path, logs.String(), err)
		}
		if msg, _ := line["error"].(string); !strings.HasPrefix(msg, tt.wantErr) {
			t.Errorf("POST %s: logged error %q, want it to start %q", tt.path, msg, tt.wantErr)
		}
		if strings.Contains(logs.String(), "secret") || strings.Contains(logs.String(), "/k/") {
			t.Errorf("POST %s: log repeats a URL:\n%s", tt.path, logs.String())
		}
	}
}
