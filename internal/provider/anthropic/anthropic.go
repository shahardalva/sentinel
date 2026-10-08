// Package anthropic implements proxy.Provider for the Anthropic Messages API.
package anthropic

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
)

const (
	DefaultBaseURL = "https://api.anthropic.com"
	DefaultVersion = "2023-06-01"

	maxIdleConns = 128 // matches proxy.DefaultMaxConcurrent
)

type Config struct {
	APIKey  string
	BaseURL string       // defaults to DefaultBaseURL
	Client  *http.Client // defaults to a client without its own timeout; callers bound requests via ctx
}

type Provider struct {
	apiKey   string
	endpoint string
	client   *http.Client
}

func New(cfg Config) *Provider {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.Client == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		// All traffic goes to one host. The default keeps only 2 idle
		// connections per host, so under load most requests would pay for a
		// fresh TLS handshake.
		t.MaxIdleConns = maxIdleConns
		t.MaxIdleConnsPerHost = maxIdleConns
		cfg.Client = &http.Client{Transport: t}
	}
	return &Provider{
		apiKey:   cfg.APIKey,
		endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/v1/messages",
		client:   cfg.Client,
	}
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
