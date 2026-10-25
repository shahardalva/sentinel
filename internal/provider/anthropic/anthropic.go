// Package anthropic implements proxy.Provider for the Anthropic Messages API.
package anthropic

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/shahardalva/sentinel/internal/provider/upstream"
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
// plain http is allowed only to a loopback host, for local test servers,
// since the upstream key would otherwise cross the network in the clear.
func New(cfg Config) (*Provider, error) {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if err := upstream.ValidateBaseURL(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("anthropic: %w", err)
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
			return nil, fmt.Errorf("anthropic: %w", err)
		}
		cfg.Client = client
	}
	return &Provider{
		apiKey:   cfg.APIKey,
		endpoint: strings.TrimRight(cfg.BaseURL, "/") + "/v1/messages",
		client:   cfg.Client,
	}, nil
}

func (p *Provider) Name() string { return "anthropic" }

// FormatError renders the gateway's own errors as the Messages API does; see
// the package-level FormatError.
func (p *Provider) FormatError(status int, kind proxy.ErrorKind, msg string) (string, []byte) {
	return FormatError(status, kind, msg)
}

// errorTypes maps each kind to the Messages API's error type.
var errorTypes = map[proxy.ErrorKind]string{
	proxy.ErrInvalidRequest:  "invalid_request_error",
	proxy.ErrAuthentication:  "authentication_error",
	proxy.ErrRateLimit:       "rate_limit_error",
	proxy.ErrRequestTooLarge: "request_too_large",
	proxy.ErrTimeout:         "timeout_error",
	proxy.ErrOverloaded:      "overloaded_error",
	proxy.ErrAPI:             "api_error",
}

// FormatError renders an error in the Messages API's error shape:
// {"type":"error","error":{"type":...,"message":...}}. An unknown kind is
// reported as api_error. It needs no Provider, so a route can format
// errors (in RequireKey, say) before one exists.
func FormatError(_ int, kind proxy.ErrorKind, msg string) (string, []byte) {
	typ, ok := errorTypes[kind]
	if !ok {
		typ = "api_error"
	}
	var b bytes.Buffer
	_ = json.NewEncoder(&b).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": typ, "message": msg},
	})
	return "application/json", b.Bytes()
}

// Forward sends the body to the Messages API. Only anthropic-version and
// anthropic-beta are taken from the client; credentials always come from the
// gateway's config. Only allowlisted response headers are returned, and a
// non-2xx body is replaced by sanitizeError's; a 2xx body is untouched, so a
// stream stays incremental.
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
		if slices.ContainsFunc(beta, upstream.HasControlChar) {
			return nil, &proxy.RequestError{Message: "anthropic-beta contains invalid characters"}
		}
		up.Header["Anthropic-Beta"] = slices.Clone(beta)
	}
	resp, err := p.client.Do(up)
	if err != nil {
		return nil, fmt.Errorf("anthropic: forward: %w", upstream.StripURL(err))
	}

	header := make(http.Header)
	for k, vv := range resp.Header {
		if allowResponseHeader(k) {
			header[k] = vv
		}
	}
	body := resp.Body
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, err := sanitizeError(resp.StatusCode, resp.Body)
		if err != nil {
			return nil, fmt.Errorf("anthropic: read error body: %w", err)
		}
		header.Set("Content-Type", "application/json")
		body = io.NopCloser(bytes.NewReader(b))
	}
	return &proxy.Response{
		Status:    resp.StatusCode,
		Header:    header,
		Body:      body,
		RequestID: resp.Header.Get("Request-Id"),
	}, nil
}

// sanitizeError reads and closes a non-2xx body, and returns the body the
// client gets instead. A Messages API error is re-encoded with the same
// error type and its message's account identifiers redacted (see
// upstream.RedactAccountIDs); fields other than the error's type and
// message and the request_id are dropped. Anything else, or a body over
// upstream.MaxErrorBody, becomes the gateway's own api_error, "upstream
// error". The error is a failed read.
func sanitizeError(status int, body io.ReadCloser) ([]byte, error) {
	b, err := upstream.ReadErrorBody(body)
	if err != nil && !errors.Is(err, upstream.ErrErrorBodyTooLarge) {
		return nil, err
	}
	if err == nil {
		if out, ok := redactError(b); ok {
			return out, nil
		}
	}
	_, out := FormatError(status, proxy.ErrAPI, "upstream error")
	return out, nil
}

// upstreamError is the Messages API's error shape.
type upstreamError struct {
	Type  string `json:"type"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
	RequestID string `json:"request_id"`
}

// redactError re-encodes b, if it is a Messages API error, in FormatError's
// form with identifiers redacted.
func redactError(b []byte) ([]byte, bool) {
	var e upstreamError
	if json.Unmarshal(b, &e) != nil || e.Type != "error" || e.Error == nil || e.Error.Type == "" {
		return nil, false
	}
	out := map[string]any{
		"type": "error",
		"error": map[string]string{
			"type":    upstream.RedactAccountIDs(e.Error.Type),
			"message": upstream.RedactAccountIDs(e.Error.Message),
		},
	}
	if e.RequestID != "" {
		out["request_id"] = upstream.RedactAccountIDs(e.RequestID)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Messages like "max_tokens: 100000 > 64000" stay as the upstream wrote
	// them; the body is JSON, never HTML.
	enc.SetEscapeHTML(false)
	_ = enc.Encode(out)
	return buf.Bytes(), true
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
