// Package openai implements proxy.Provider for the OpenAI Chat Completions
// API and the many servers that speak it (Ollama, vLLM and others).
package openai

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/shahardalva/sentinel/internal/provider/upstream"
	"github.com/shahardalva/sentinel/internal/proxy"
)

// DefaultBaseURL includes /v1, as the OpenAI SDKs' own OPENAI_BASE_URL does,
// so a base URL copied from an SDK setup works unchanged.
const DefaultBaseURL = "https://api.openai.com/v1"

type Config struct {
	APIKey  string
	BaseURL string       // defaults to DefaultBaseURL
	Client  *http.Client // defaults to a client without its own timeout; callers bound requests via ctx
	// MaxIdleConns sizes the default client's idle pool; set it to the
	// gateway's concurrency cap. Defaults to proxy.DefaultMaxConcurrent.
	MaxIdleConns int
	// ExtraRootCAs are trusted for the upstream TLS connection in addition
	// to the system trust store, never instead of it. Ignored when Client
	// is set.
	ExtraRootCAs []*x509.Certificate
}

type Provider struct {
	apiKey   string
	endpoint string
	client   *http.Client
}

// New returns a Provider for cfg. It fails if BaseURL isn't an https URL;
// plain http is allowed only to a loopback host, for local test servers and
// local model servers, since the upstream key would otherwise cross the
// network in the clear.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if err := upstream.ValidateBaseURL(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	if cfg.Client == nil {
		if cfg.MaxIdleConns <= 0 {
			cfg.MaxIdleConns = proxy.DefaultMaxConcurrent
		}
		client, err := upstream.NewClient(upstream.ClientConfig{
			MaxIdleConns: cfg.MaxIdleConns,
			ExtraRootCAs: cfg.ExtraRootCAs,
		})
		if err != nil {
			return nil, fmt.Errorf("openai: %w", err)
		}
		cfg.Client = client
	}
	return &Provider{
		apiKey:   cfg.APIKey,
		endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions",
		client:   cfg.Client,
	}, nil
}

func (p *Provider) Name() string { return "openai" }

// FormatError renders the gateway's own errors as the OpenAI API does; see
// the package-level FormatError.
func (p *Provider) FormatError(status int, kind proxy.ErrorKind, msg string) (string, []byte) {
	return FormatError(status, kind, msg)
}

// errorShape is OpenAI's error type and code for a kind; an empty code is
// sent as null.
type errorShape struct {
	typ, code string
}

// errorShapes maps each kind to OpenAI's error type and code. OpenAI SDKs
// classify errors by HTTP status (RateLimitError for a 429, and so on), not
// by these strings, so the strings are informative. They follow OpenAI's
// documented shapes, but not every value has been checked against OpenAI's
// live responses.
var errorShapes = map[proxy.ErrorKind]errorShape{
	proxy.ErrInvalidRequest:  {"invalid_request_error", ""},
	proxy.ErrAuthentication:  {"invalid_request_error", "invalid_api_key"},
	proxy.ErrRateLimit:       {"requests", "rate_limit_exceeded"},
	proxy.ErrRequestTooLarge: {"invalid_request_error", "request_too_large"},
	proxy.ErrTimeout:         {"timeout_error", ""},
	proxy.ErrOverloaded:      {"server_error", "overloaded"},
	proxy.ErrAPI:             {"server_error", ""},
}

// errorBody fixes the field order of OpenAI's error object, which a map
// would sort.
type errorBody struct {
	Error struct {
		Message string  `json:"message"`
		Type    string  `json:"type"`
		Param   *string `json:"param"`
		Code    *string `json:"code"`
	} `json:"error"`
}

// FormatError renders an error in the OpenAI API's error shape:
// {"error":{"message":...,"type":...,"param":null,"code":...}}. An unknown
// kind is reported as server_error with a null code. It needs no Provider,
// so a route can format errors (in RequireKey, say) before one exists.
func FormatError(_ int, kind proxy.ErrorKind, msg string) (string, []byte) {
	shape, ok := errorShapes[kind]
	if !ok {
		shape = errorShapes[proxy.ErrAPI]
	}
	var e errorBody
	e.Error.Message = msg
	e.Error.Type = shape.typ
	if shape.code != "" {
		e.Error.Code = &shape.code
	}
	var b bytes.Buffer
	_ = json.NewEncoder(&b).Encode(e)
	return "application/json", b.Bytes()
}

// Forward sends the body to the Chat Completions endpoint. No client header
// is forwarded: credentials always come from the gateway's config, and
// OpenAI-Organization and OpenAI-Project would let a client pick whose
// account is billed. OpenAI-Beta and the SDKs' X-Stainless-* telemetry go
// the same way. Only allowlisted response headers are returned.
func (p *Provider) Forward(ctx context.Context, req *proxy.Request) (*proxy.Response, error) {
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(req.Body))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	up.Header.Set("Content-Type", "application/json")
	up.Header.Set("Authorization", "Bearer "+p.apiKey)
	resp, err := p.client.Do(up)
	if err != nil {
		return nil, fmt.Errorf("openai: forward: %w", err)
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
		RequestID: resp.Header.Get("X-Request-Id"),
	}, nil
}

// allowResponseHeader reports whether an upstream response header may reach
// the client. Everything else (organization and project, processing time,
// cookies, CDN headers, and the upstream's X-Request-Id, which would clash
// with the gateway's own) is dropped.
func allowResponseHeader(name string) bool {
	name = http.CanonicalHeaderKey(name)
	switch name {
	case "Content-Type", "Retry-After":
		return true
	}
	return strings.HasPrefix(name, "X-Ratelimit-")
}
