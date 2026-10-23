// Package anthropic implements proxy.Provider for the Anthropic Messages API.
package anthropic

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
)

const (
	DefaultBaseURL = "https://api.anthropic.com"
	DefaultVersion = "2023-06-01"
)

type Config struct {
	APIKey  string
	BaseURL string       // defaults to DefaultBaseURL
	Client  *http.Client // defaults to a client without its own timeout; callers bound requests via ctx
	// MaxIdleConns sizes the default client's idle pool; set it to the
	// gateway's concurrency cap. Defaults to proxy.DefaultMaxConcurrent.
	MaxIdleConns int
	// RootCAs, if set, replaces the default client's trust store for the
	// upstream TLS connection. Ignored when Client is set.
	RootCAs *x509.CertPool
}

type Provider struct {
	apiKey   string
	endpoint string
	client   *http.Client
}

// New returns a Provider for cfg. It fails if BaseURL isn't an https URL;
// plain http is allowed only to a loopback host, for local test servers,
// since the upstream key would otherwise cross the network in the clear.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if err := validateBaseURL(cfg.BaseURL); err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		// All traffic goes to one host. The default keeps only 2 idle
		// connections per host, so under load most requests would pay for a
		// fresh TLS handshake.
		if cfg.MaxIdleConns <= 0 {
			cfg.MaxIdleConns = proxy.DefaultMaxConcurrent
		}
		t.MaxIdleConns = cfg.MaxIdleConns
		t.MaxIdleConnsPerHost = cfg.MaxIdleConns
		if cfg.RootCAs != nil {
			t.TLSClientConfig = &tls.Config{RootCAs: cfg.RootCAs, MinVersion: tls.VersionTLS12}
		}
		cfg.Client = &http.Client{Transport: t}
	}
	return &Provider{
		apiKey:   cfg.APIKey,
		endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/v1/messages",
		client:   cfg.Client,
	}, nil
}

func validateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("anthropic: base URL: %w", err)
	}
	if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("anthropic: base URL %q must be scheme://host[/path] with no credentials, query, or fragment", raw)
	}
	switch {
	case u.Scheme == "https":
		return nil
	case u.Scheme == "http" && isLoopback(u.Hostname()):
		return nil
	}
	return fmt.Errorf("anthropic: base URL %q must use https (http is allowed only for loopback hosts)", raw)
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (p *Provider) Name() string { return "anthropic" }

// Forward sends the body to the Messages API. Only anthropic-version and
// anthropic-beta are taken from the client; credentials always come from the
// gateway's config. Only allowlisted response headers are returned.
func (p *Provider) Forward(ctx context.Context, req *proxy.Request) (*proxy.Response, error) {
	version := req.Header.Get("Anthropic-Version")
	if version == "" {
		version = DefaultVersion
	}
	if !validVersion(version) {
		return nil, &proxy.RequestError{Message: "anthropic-version must be a date in YYYY-MM-DD form"}
	}

	up, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(req.Body))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	up.Header.Set("Content-Type", "application/json")
	up.Header.Set("X-Api-Key", p.apiKey)
	up.Header.Set("Anthropic-Version", version)
	if beta := req.Header.Values("Anthropic-Beta"); len(beta) > 0 {
		// Control characters (CR and LF above all) would make the HTTP
		// client refuse the request, which would surface as a 502.
		if slices.ContainsFunc(beta, hasControlChar) {
			return nil, &proxy.RequestError{Message: "anthropic-beta contains invalid characters"}
		}
		up.Header["Anthropic-Beta"] = slices.Clone(beta)
	}
	resp, err := p.client.Do(up)
	if err != nil {
		return nil, fmt.Errorf("anthropic: forward: %w", err)
	}

	header := make(http.Header)
	for k, vv := range resp.Header {
		if allowResponseHeader(k) {
			header[k] = vv
		}
	}
	return &proxy.Response{
		Status:    resp.StatusCode,
		Header:    header,
		Body:      resp.Body,
		RequestID: resp.Header.Get("Request-Id"),
	}, nil
}

// validVersion reports whether v is an API version date like 2023-06-01.
func validVersion(v string) bool {
	_, err := time.Parse(time.DateOnly, v)
	return err == nil
}

// hasControlChar reports whether s contains an ASCII control character other
// than horizontal tab, which header values may not.
func hasControlChar(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool {
		return (r < ' ' && r != '\t') || r == 0x7f
	})
}

// allowResponseHeader reports whether an upstream response header may reach
// the client. Everything else (organization ID, cookies, CDN headers) is
// dropped.
func allowResponseHeader(name string) bool {
	name = http.CanonicalHeaderKey(name)
	switch name {
	case "Content-Type", "Retry-After", "Request-Id", "X-Should-Retry":
		return true
	}
	return strings.HasPrefix(name, "Anthropic-Ratelimit-")
}
