package upstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func mustClient(t *testing.T, cfg ClientConfig) *http.Client {
	t.Helper()
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestNewClient_RefusesRedirects(t *testing.T) {
	var followed bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true }))
	defer target.Close()
	for _, code := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		src := httptest.NewServer(http.RedirectHandler(target.URL, code))
		req, err := http.NewRequest(http.MethodPost, src.URL, strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Api-Key", "secret")
		resp, err := mustClient(t, ClientConfig{MaxIdleConns: 1}).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		if !errors.Is(err, ErrRedirect) {
			t.Errorf("%d: Do = %v, want ErrRedirect", code, err)
		}
		src.Close()
	}
	if followed {
		t.Error("a redirect was followed")
	}
}

func TestStripURL(t *testing.T) {
	for _, cause := range []error{ErrRedirect, context.Canceled} {
		err := StripURL(&url.Error{Op: "Post", URL: "https://gw.example/k/sk-secret/v1", Err: cause})
		if got, want := err.Error(), "Post: "+cause.Error(); got != want {
			t.Errorf("StripURL = %q, want %q", got, want)
		}
		if !errors.Is(err, cause) {
			t.Errorf("errors.Is(%v, %v) = false; the cause must stay wrapped", err, cause)
		}
	}
	other := errors.New("build request")
	if got := StripURL(other); got != other {
		t.Errorf("StripURL(non-url error) = %v, want it unchanged", got)
	}
}

func TestValidateBaseURL(t *testing.T) {
	valid := []string{
		"https://api.anthropic.com",
		"https://proxy.internal:8443/anthropic/",
		"http://127.0.0.1:9000",
		"http://[::1]:9000",
		"http://localhost:9000",
	}
	for _, u := range valid {
		if err := ValidateBaseURL(u); err != nil {
			t.Errorf("ValidateBaseURL(%q) = %v, want ok", u, err)
		}
	}
	invalid := []string{
		"",                         // no host
		"http://api.anthropic.com", // key in plaintext over the network
		"http://10.0.0.5:8080",
		"api.anthropic.com", // no scheme
		"https://",
		"ftp://api.anthropic.com",
		"https://user:pass@api.anthropic.com",
		"https://api.anthropic.com?x=1",
		"https://api.anthropic.com/#frag",
		"https://api.anthropic.com/\x7f",
	}
	for _, u := range invalid {
		if err := ValidateBaseURL(u); err == nil {
			t.Errorf("ValidateBaseURL(%q) = ok, want error", u)
		}
	}
}

// TestValidateBaseURL_NoSecretsInErrors checks that errors name at most the
// scheme and hostname: a base URL may carry a key in its userinfo, path or
// query, and the error reaches the startup log.
func TestValidateBaseURL_NoSecretsInErrors(t *testing.T) {
	tests := []struct{ url, want string }{
		{"https://user:secret@api.openai.com/v1", "base URL for https://api.openai.com must not contain credentials"},
		{"https://sk-secret@api.openai.com/v1", "base URL for https://api.openai.com must not contain credentials"},
		{"https://api.openai.com/v1?api-key=sk-secret", "base URL for https://api.openai.com must not contain a query"},
		{"https://api.openai.com/v1#sk-secret", "base URL for https://api.openai.com must not contain a fragment"},
		{"http://api.openai.com/key/sk-secret", "base URL for http://api.openai.com must use https (http is allowed only for loopback hosts)"},
		{"/v1/sk-secret", "base URL must be scheme://host[/path]"},
		// url.Parse fails; its *url.Error would repeat the whole URL.
		{"https://user:secret@api.openai.com/\x7f", "base URL is not a valid URL"},
		{"https://user:secret@api.openai.com:port/v1", "base URL is not a valid URL"},
		{"https://api.openai.com/v1?api-key=sk-secret%zz", "base URL for https://api.openai.com must not contain a query"},
		{"https://us%zzer:secret@api.openai.com/v1", "base URL is not a valid URL"},
	}
	for _, tt := range tests {
		err := ValidateBaseURL(tt.url)
		if err == nil || err.Error() != tt.want {
			t.Errorf("ValidateBaseURL(%q) = %v, want %q", tt.url, err, tt.want)
			continue
		}
		if strings.Contains(err.Error(), "secret") {
			t.Errorf("ValidateBaseURL(%q) error %q repeats a secret", tt.url, err)
		}
		if _, ok := errors.AsType[*BaseURLError](err); !ok {
			t.Errorf("ValidateBaseURL(%q) error is %T, want *BaseURLError", tt.url, err)
		}
		if errors.Unwrap(err) != nil {
			t.Errorf("ValidateBaseURL(%q) error wraps %v", tt.url, errors.Unwrap(err))
		}
	}
}

func TestIsLoopback(t *testing.T) {
	for _, h := range []string{"localhost", "127.0.0.1", "127.8.9.10", "::1"} {
		if !IsLoopback(h) {
			t.Errorf("IsLoopback(%q) = false, want true", h)
		}
	}
	for _, h := range []string{"", "localhost.example.com", "10.0.0.5", "0.0.0.0", "::", "api.anthropic.com"} {
		if IsLoopback(h) {
			t.Errorf("IsLoopback(%q) = true, want false", h)
		}
	}
}

func TestNewClient_ConnectionPool(t *testing.T) {
	tr, ok := mustClient(t, ClientConfig{MaxIdleConns: 300}).Transport.(*http.Transport)
	if !ok {
		t.Fatal("client has no *http.Transport")
	}
	if tr.MaxIdleConnsPerHost != 300 || tr.MaxIdleConns != 300 {
		t.Errorf("idle conns = %d per host, %d total; want 300", tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
	}
	if tr == http.DefaultTransport {
		t.Error("shares http.DefaultTransport; must be a clone")
	}
}

// roundTripperFunc is a RoundTripper that isn't an *http.Transport.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Not parallel: it swaps the process-wide http.DefaultTransport.
func TestNewClient_ReplacedDefaultTransport(t *testing.T) {
	old := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = old })
	http.DefaultTransport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("replaced transport used")
	})

	c, err := NewClient(ClientConfig{MaxIdleConns: 300})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport = %T, want a fresh *http.Transport", c.Transport)
	}
	// Go's documented DefaultTransport settings, except the pool, which
	// NewClient sets, and TLS, which it adds.
	if tr.Proxy == nil || tr.DialContext == nil || !tr.ForceAttemptHTTP2 ||
		tr.IdleConnTimeout != 90*time.Second || tr.TLSHandshakeTimeout != 10*time.Second ||
		tr.ExpectContinueTimeout != time.Second {
		t.Errorf("transport lacks DefaultTransport's defaults: %+v", tr)
	}
	if tr.MaxIdleConns != 300 || tr.MaxIdleConnsPerHost != 300 {
		t.Errorf("idle conns = %d per host, %d total; want 300", tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Errorf("TLSClientConfig = %+v, want MinVersion >= TLS 1.2", tr.TLSClientConfig)
	}

	// And it works, without going through the replaced transport.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
}

func TestNewClient_RequiresPoolSize(t *testing.T) {
	for _, n := range []int{0, -1} {
		if _, err := NewClient(ClientConfig{MaxIdleConns: n}); err == nil {
			t.Errorf("NewClient(MaxIdleConns: %d) = ok, want error", n)
		}
	}
}

func TestNewClient_TLSConfig(t *testing.T) {
	tr := mustClient(t, ClientConfig{MaxIdleConns: 1}).Transport.(*http.Transport)
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Errorf("TLSClientConfig = %+v, want MinVersion >= TLS 1.2", tr.TLSClientConfig)
	}
	if tr.TLSClientConfig.RootCAs != nil {
		t.Error("RootCAs set without ExtraRootCAs; want the system default")
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 lost; a custom TLS config would disable HTTP/2")
	}
}

func TestNewClient_ExtraRootCAs(t *testing.T) {
	// httptest's certificate is self-signed and marked as a CA, so it can
	// stand in for a private CA.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	get := func(cfg ClientConfig) error {
		cfg.MaxIdleConns = 1
		resp, err := mustClient(t, cfg).Get(srv.URL)
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}

	var verr *tls.CertificateVerificationError
	if err := get(ClientConfig{}); !errors.As(err, &verr) {
		t.Errorf("system roots only: Get = %v, want certificate verification error", err)
	}
	if err := get(ClientConfig{ExtraRootCAs: []*x509.Certificate{srv.Certificate()}}); err != nil {
		t.Errorf("server's CA trusted: Get = %v, want ok", err)
	}
}

func TestHasControlChar(t *testing.T) {
	for _, v := range []string{"a\r\nX-Injected: 1", "a\nb", "a\rb", "a\x00b", "a\x7fb"} {
		if !HasControlChar(v) {
			t.Errorf("HasControlChar(%q) = false, want true", v)
		}
	}
	// A tab is a legal header character.
	for _, v := range []string{"", "feature-a", "a,\tb", "ünïcode"} {
		if HasControlChar(v) {
			t.Errorf("HasControlChar(%q) = true, want false", v)
		}
	}
}
