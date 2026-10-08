// Package proxy implements the gateway's HTTP handlers that forward
// client requests to an upstream LLM provider.
package proxy

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
	"unicode/utf8"
)

// Provider forwards a request to an upstream LLM API. Implementations own
// authentication and decide which client headers are passed upstream and
// which upstream response headers are passed back.
type Provider interface {
	Name() string
	Forward(ctx context.Context, req *Request) (*http.Response, error)
	// AllowResponseHeader reports whether an upstream response header may be
	// sent to the client. It must reject hop-by-hop headers.
	AllowResponseHeader(name string) bool
}

// Request is a client request as handed to a Provider.
type Request struct {
	Body   []byte
	Header http.Header // client headers, untrusted
}

const (
	// A non-streaming request sends no headers until generation finishes, so
	// this matches the Anthropic SDKs' own 10-minute default.
	DefaultHeaderTimeout = 10 * time.Minute
	DefaultIdleTimeout   = 60 * time.Second
	DefaultBodyTimeout   = 2 * time.Minute
	DefaultMaxBodyBytes  = 32 << 20 // Anthropic's request size limit

	maxLoggedModel = 128 // bytes; the model field is client-controlled
)

var (
	errHeaderTimeout = errors.New("upstream response headers timed out")
	errIdleTimeout   = errors.New("upstream stream idle timeout")
)

// Options configures a Handler. Zero values select the defaults.
type Options struct {
	// HeaderTimeout bounds the wait for the upstream's response headers.
	HeaderTimeout time.Duration
	// IdleTimeout bounds each wait for the next chunk from upstream, and each
	// write of a chunk to the client. A stream may run indefinitely while
	// data keeps flowing.
	IdleTimeout time.Duration
	// BodyTimeout bounds reading the client's request body.
	BodyTimeout  time.Duration
	MaxBodyBytes int64
}

// Handler proxies a request to a Provider and returns its response as-is.
type Handler struct {
	provider      Provider
	logger        *slog.Logger
	headerTimeout time.Duration
	idleTimeout   time.Duration
	bodyTimeout   time.Duration
	maxBody       int64
}

func NewHandler(p Provider, logger *slog.Logger, opts Options) *Handler {
	if opts.HeaderTimeout <= 0 {
		opts.HeaderTimeout = DefaultHeaderTimeout
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	}
	if opts.BodyTimeout <= 0 {
		opts.BodyTimeout = DefaultBodyTimeout
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = DefaultMaxBodyBytes
	}
	return &Handler{
		provider:      p,
		logger:        logger,
		headerTimeout: opts.HeaderTimeout,
		idleTimeout:   opts.IdleTimeout,
		bodyTimeout:   opts.BodyTimeout,
		maxBody:       opts.MaxBodyBytes,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := rand.Text()
	w.Header().Set("X-Request-Id", reqID)

	var (
		model, upstreamID string
		status            int
		upstreamDur       time.Duration
		failure           error
	)
	defer func() {
		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		}
		attrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("provider", h.provider.Name()),
			slog.String("model", model),
			slog.Int("status", status),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
			slog.Int64("upstream_ms", upstreamDur.Milliseconds()),
		}
		if upstreamID != "" {
			attrs = append(attrs, slog.String("upstream_request_id", upstreamID))
		}
		if failure != nil {
			attrs = append(attrs, slog.String("error", failure.Error()))
		}
		h.logger.LogAttrs(r.Context(), level, "request", attrs...)
	}()

	// The read deadline is per request, not server-wide, and is cleared once
	// the body is in: a read deadline expiring later, during a long stream,
	// would be treated as the client going away.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(h.bodyTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBody))
	_ = rc.SetReadDeadline(time.Time{})
	if err != nil {
		failure = err
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			status = http.StatusRequestEntityTooLarge
			writeError(w, status, "request_too_large", "request body too large")
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			status = http.StatusRequestTimeout
			writeError(w, status, "timeout_error", "request body not received in time")
			return
		}
		status = http.StatusBadRequest
		writeError(w, status, "invalid_request_error", "could not read request body")
		return
	}
	model = truncate(extractModel(body), maxLoggedModel)

	// One context for the whole upstream exchange. Timers cancel it with a
	// cause, so a timeout is distinguishable from the client going away.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)

	upStart := time.Now()
	headerTimer := time.AfterFunc(h.headerTimeout, func() { cancel(errHeaderTimeout) })
	resp, err := h.provider.Forward(ctx, &Request{Body: body, Header: r.Header})
	headerTimer.Stop()
	upstreamDur = time.Since(upStart)
	if err != nil {
		failure = err
		switch {
		case r.Context().Err() != nil:
			status = 499 // client closed request; nobody to answer
		case errors.Is(context.Cause(ctx), errHeaderTimeout):
			failure = errHeaderTimeout
			status = http.StatusGatewayTimeout
			writeError(w, status, "timeout_error", "upstream timed out")
		default:
			status = http.StatusBadGateway
			writeError(w, status, "api_error", "upstream unavailable")
		}
		return
	}
	defer resp.Body.Close()

	upstreamID = resp.Header.Get("Request-Id")
	for k, vv := range resp.Header {
		if h.provider.AllowResponseHeader(k) {
			w.Header()[k] = append(w.Header()[k], vv...)
		}
	}
	w.Header().Set("X-Request-Id", reqID)
	status = resp.StatusCode
	w.WriteHeader(status)
	idleTimer := time.AfterFunc(h.idleTimeout, func() { cancel(errIdleTimeout) })
	idleTimer.Stop()
	src := &idleReader{r: resp.Body, timer: idleTimer, d: h.idleTimeout}
	if err := copyFlush(w, src, h.idleTimeout); err != nil {
		// Status is already sent; all we can do is record it.
		failure = err
		if errors.Is(context.Cause(ctx), errIdleTimeout) {
			failure = errIdleTimeout
		}
	}
	upstreamDur = time.Since(upStart)
}

// idleReader arms timer for the duration of each Read, so it fires only when
// the upstream itself goes quiet, not while a chunk is being written out.
type idleReader struct {
	r     io.Reader
	timer *time.Timer
	d     time.Duration
}

func (ir *idleReader) Read(p []byte) (int, error) {
	ir.timer.Reset(ir.d)
	n, err := ir.r.Read(p)
	ir.timer.Stop()
	return n, err
}

// copyFlush copies src to w, flushing after every write so streamed (SSE)
// events reach the client as they arrive instead of waiting in a buffer.
// Each write must complete within writeTimeout, so a client that stops
// reading can't hold the stream open.
func copyFlush(w http.ResponseWriter, src io.Reader, writeTimeout time.Duration) error {
	rc := http.NewResponseController(w)
	// Clear the deadline so it can't affect the next request on this connection.
	defer rc.SetWriteDeadline(time.Time{})
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if derr := rc.SetWriteDeadline(time.Now().Add(writeTimeout)); derr != nil && !errors.Is(derr, http.ErrNotSupported) {
				return derr
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if ferr := rc.Flush(); ferr != nil && !errors.Is(ferr, http.ErrNotSupported) {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// extractModel returns the "model" field of a JSON body, or "" if absent.
func extractModel(body []byte) string {
	var v struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &v)
	return v.Model
}

// truncate shortens s to at most n bytes without splitting a UTF-8 sequence.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// writeError writes an error in Anthropic's error response shape.
func writeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": typ, "message": msg},
	})
}
