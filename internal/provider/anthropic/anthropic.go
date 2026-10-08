// Package anthropic implements proxy.Provider for the Anthropic Messages API.
package anthropic

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"strings"

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
		cfg.Client = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
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
// gateway's config.
func (p *Provider) Forward(ctx context.Context, req *proxy.Request) (*http.Response, error) {
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	version := req.Header.Get("Anthropic-Version")
	if version == "" {
		version = DefaultVersion
	}
	up.Header.Set("Content-Type", "application/json")
	up.Header.Set("X-Api-Key", p.apiKey)
	up.Header.Set("Anthropic-Version", version)
	if beta := req.Header.Values("Anthropic-Beta"); len(beta) > 0 {
		up.Header["Anthropic-Beta"] = slices.Clone(beta)
	}
	return p.client.Do(up)
}

// AllowResponseHeader reports whether an upstream response header may reach
// the client. Everything else (organization ID, cookies, CDN headers) is
// dropped.
func (p *Provider) AllowResponseHeader(name string) bool {
	name = http.CanonicalHeaderKey(name)
	switch name {
	case "Content-Type", "Retry-After", "Request-Id", "X-Should-Retry":
		return true
	}
	return strings.HasPrefix(name, "Anthropic-Ratelimit-")
}
