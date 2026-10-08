// Package proxy implements the gateway's HTTP handlers that forward
// client requests to an upstream LLM provider.
package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// Provider forwards a request to an upstream LLM API. Implementations own
// authentication and decide which client headers are passed upstream and
// which upstream response headers are passed back.
type Provider interface {
	Name() string
	// Forward sends req upstream. It returns a *RequestError to reject the
	// client's request without contacting upstream.
	Forward(ctx context.Context, req *Request) (*Response, error)
}

// Request is a client request as handed to a Provider.
type Request struct {
	Body   []byte
	Header http.Header // client headers, untrusted
}

// Response is an upstream response as handed back by a Provider.
type Response struct {
	Status int
	// Header holds only headers safe to send to the client; the Provider has
	// already dropped hop-by-hop and internal ones.
	Header http.Header
	Body   io.ReadCloser
	// RequestID is the upstream's ID for this request, for the log. May be empty.
	RequestID string
}

// RequestError rejects a client request as invalid. The handler answers it
// with a 400 carrying Message, so Message must be safe to show the client.
type RequestError struct {
	Message string
}

func (e *RequestError) Error() string { return e.Message }

const (
	// A non-streaming request sends no headers until generation finishes, so
	// this matches the Anthropic SDKs' own 10-minute default.
	DefaultHeaderTimeout = 10 * time.Minute
	DefaultIdleTimeout   = 60 * time.Second
	DefaultBodyTimeout   = 2 * time.Minute
	DefaultMaxBodyBytes  = 32 << 20 // Anthropic's request size limit
	// Bounds requests in flight upstream, and so their buffered bodies to
	// MaxConcurrent × MaxBodyBytes (4 GiB with the defaults). Bodies still
	// being uploaded don't hold a slot; their buffers grow only as bytes
	// arrive. Lower either for a smaller deployment.
	DefaultMaxConcurrent = 128

	// RejectDrainTimeout bounds how long a connection is kept, after an early
	// rejection, for the rest of a body the gateway won't use. Long enough
	// for a client already sending it to finish, so closing doesn't reset
	// the connection before the client reads the response.
	RejectDrainTimeout = time.Second

	maxLoggedModel = 128 // bytes; the model field is client-controlled

	// initialBodyBuf caps the up-front allocation for a request body. The
	// declared Content-Length is client-controlled, so beyond this the buffer
	// grows only as bytes actually arrive.
	initialBodyBuf = 64 << 10
)

var (
	errHeaderTimeout   = errors.New("upstream response headers timed out")
	errIdleTimeout     = errors.New("upstream stream idle timeout")
	errOverloaded      = errors.New("gateway at capacity")
	errUpstreamAuth    = errors.New("upstream rejected the gateway's credentials")
	errContentEncoding = errors.New("unsupported content-encoding")
)

// clientWriteError marks a stream failure on the client's side of the proxy:
// a failed write or flush to the client, including the write timeout.
type clientWriteError struct{ err error }

func (e *clientWriteError) Error() string { return "write to client: " + e.err.Error() }
func (e *clientWriteError) Unwrap() error { return e.err }

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
	// MaxConcurrent caps requests in flight upstream; requests beyond it get
	// a 503. A request takes its slot only once its body is fully read.
	MaxConcurrent int
}

// Handler proxies a request to a Provider and returns its response as-is.
type Handler struct {
	provider      Provider
	logger        *slog.Logger
	headerTimeout time.Duration
	idleTimeout   time.Duration
	bodyTimeout   time.Duration
	maxBody       int64
	slots         chan struct{} // counting semaphore of MaxConcurrent
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
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	return &Handler{
		provider:      p,
		logger:        logger,
		headerTimeout: opts.HeaderTimeout,
		idleTimeout:   opts.IdleTimeout,
		bodyTimeout:   opts.BodyTimeout,
		maxBody:       opts.MaxBodyBytes,
		slots:         make(chan struct{}, opts.MaxConcurrent),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := rand.Text()
	w.Header().Set("X-Request-Id", reqID)

	var (
		model, upstreamID           string
		status                      int
		phase                       = phaseReadBody // the stage reached, or where it failed
		readDur, ttfbDur, streamDur time.Duration
		failure                     error
	)
	defer func() {
		attrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("provider", h.provider.Name()),
			slog.String("model", model),
			slog.Int("status", status),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
			slog.Int64("read_body_ms", readDur.Milliseconds()),
		}
		if phase == phaseUpstream || phase == phaseStream {
			attrs = append(attrs, slog.Int64("upstream_ttfb_ms", ttfbDur.Milliseconds()))
		}
		if phase == phaseStream {
			attrs = append(attrs, slog.Int64("stream_ms", streamDur.Milliseconds()))
		}
		if upstreamID != "" {
			attrs = append(attrs, slog.String("upstream_request_id", upstreamID))
		}
		if failure != nil {
			attrs = append(attrs, slog.String("error", failure.Error()), slog.String("phase", phase))
		}
		_, clientFailed := errors.AsType[*clientWriteError](failure)
		clientGone := clientFailed || r.Context().Err() != nil
		h.logger.LogAttrs(r.Context(), logLevel(status, phase, failure, clientGone), "request", attrs...)
	}()

	// The body is forwarded and inspected as-is, so an encoded one would be
	// unreadable upstream and opaque to any check on its contents.
	if ce := r.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		failure = errContentEncoding
		status = http.StatusUnsupportedMediaType
		rejectEarly(w, status, "invalid_request_error", "content-encoding is not supported; send an uncompressed body")
		return
	}

	// A declared length over the limit is rejected before reading anything.
	if r.ContentLength > h.maxBody {
		failure = &http.MaxBytesError{Limit: h.maxBody}
		status = http.StatusRequestEntityTooLarge
		rejectEarly(w, status, "request_too_large", "request body too large")
		return
	}

	// The read deadline is per request, not server-wide, and is cleared once
	// the body is in: a read deadline expiring later, during a long stream,
	// would be treated as the client going away.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(h.bodyTimeout))
	body, err := readBody(http.MaxBytesReader(w, r.Body, h.maxBody), r.ContentLength)
	_ = rc.SetReadDeadline(time.Time{})
	readDur = time.Since(start)
	if err != nil {
		failure = err
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			status = http.StatusRequestEntityTooLarge
			rejectEarly(w, status, "request_too_large", "request body too large")
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			status = http.StatusRequestTimeout
			rejectEarly(w, status, "timeout_error", "request body not received in time")
			return
		}
		status = http.StatusBadRequest
		rejectEarly(w, status, "invalid_request_error", "could not read request body")
		return
	}
	model = truncate(extractModel(body), maxLoggedModel)

	// The slot is taken only once the body is in, so a slow upload can't hold
	// one. Shed load rather than queue: a queued request still holds its
	// connection, and waiting only moves the memory pressure elsewhere.
	phase = phaseAdmit
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		failure = errOverloaded
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
		rejectEarly(w, status, "overloaded_error", "gateway is at capacity")
		return
	}

	// One context for the whole upstream exchange. Timers cancel it with a
	// cause, so a timeout is distinguishable from the client going away.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)

	phase = phaseUpstream
	upStart := time.Now()
	headerTimer := time.AfterFunc(h.headerTimeout, func() { cancel(errHeaderTimeout) })
	resp, err := h.provider.Forward(ctx, &Request{Body: body, Header: r.Header})
	headerTimer.Stop()
	ttfbDur = time.Since(upStart)
	if err != nil {
		failure = err
		reqErr, invalid := errors.AsType[*RequestError](err)
		switch {
		case invalid:
			status = http.StatusBadRequest
			writeError(w, status, "invalid_request_error", reqErr.Message)
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

	upstreamID = resp.RequestID
	// Clients never authenticate to the upstream; the gateway does. An
	// upstream 401/403 means the gateway's own credentials were rejected, so
	// passing it on would look like a problem with the client's key.
	if resp.Status == http.StatusUnauthorized || resp.Status == http.StatusForbidden {
		failure = fmt.Errorf("%w (status %d)", errUpstreamAuth, resp.Status)
		status = http.StatusBadGateway
		writeError(w, status, "api_error", "upstream authentication failed")
		return
	}
	for k, vv := range resp.Header {
		w.Header()[k] = append(w.Header()[k], vv...)
	}
	w.Header().Set("X-Request-Id", reqID) // always the gateway's own
	status = resp.Status
	w.WriteHeader(status)

	phase = phaseStream
	streamStart := time.Now()
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
	streamDur = time.Since(streamStart)
}

// Request phases, as logged. A failure's phase says where it happened.
const (
	phaseReadBody = "read_body"
	phaseAdmit    = "admit" // waiting for a concurrency slot
	phaseUpstream = "upstream"
	phaseStream   = "stream"
)

// logLevel picks the level for a request's log line. Server-side and upstream
// failures are errors, including a stream that breaks after a 2xx was sent.
// Failures caused by the client (bad body, disconnect) and load shedding are
// warnings.
func logLevel(status int, phase string, failure error, clientGone bool) slog.Level {
	switch {
	case errors.Is(failure, errOverloaded):
		return slog.LevelWarn
	case status >= 500:
		return slog.LevelError
	case failure == nil:
		return slog.LevelInfo
	case phase == phaseStream && !clientGone:
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
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
// reading can't hold the stream open. Failures on the client's side are
// returned as *clientWriteError; a read error from src is returned as is.
func copyFlush(w http.ResponseWriter, src io.Reader, writeTimeout time.Duration) error {
	rc := http.NewResponseController(w)
	// Clear the deadline so it can't affect the next request on this connection.
	defer rc.SetWriteDeadline(time.Time{})
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if derr := rc.SetWriteDeadline(time.Now().Add(writeTimeout)); derr != nil && !errors.Is(derr, http.ErrNotSupported) {
				return &clientWriteError{derr}
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return &clientWriteError{werr}
			}
			if ferr := rc.Flush(); ferr != nil && !errors.Is(ferr, http.ErrNotSupported) {
				return &clientWriteError{ferr}
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

// readBody reads r to the end. When the length is declared, the buffer is
// allocated up front at that size, capped at initialBodyBuf, so most bodies
// are read without repeated doubling and a large declared length reserves no
// more than that until the bytes arrive.
func readBody(r io.Reader, contentLength int64) ([]byte, error) {
	if contentLength <= 0 {
		return io.ReadAll(r)
	}
	// MinRead of headroom lets ReadFrom see EOF without growing the buffer.
	buf := bytes.NewBuffer(make([]byte, 0, min(contentLength, initialBodyBuf)+bytes.MinRead))
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
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

// rejectEarly answers a request whose body may still be unread. net/http
// drains up to 256 KiB of an unread body, with no deadline, before sending
// the response and again when the handler returns, so a client that stalls
// mid-body would hold the connection open indefinitely. Connection: close
// skips the first drain; the read deadline bounds the second.
func rejectEarly(w http.ResponseWriter, status int, typ, msg string) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(RejectDrainTimeout))
	w.Header().Set("Connection", "close")
	writeError(w, status, typ, msg)
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
