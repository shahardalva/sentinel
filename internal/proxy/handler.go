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
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Provider forwards a request to an upstream LLM API. Implementations own
// authentication and decide which client headers are passed upstream and
// which upstream response headers are passed back. A Provider also renders
// the gateway's own errors in its protocol's shape.
type Provider interface {
	ErrorFormatter
	Name() string
	// Forward sends req upstream. It returns a *RequestError to reject the
	// client's request without contacting upstream. A nil error means a
	// non-nil *Response with a non-nil Body and a 2xx, 4xx or 5xx Status,
	// which the caller closes; a Handler answers a Provider that breaks this
	// with a 502.
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
	// already dropped hop-by-hop and internal ones. The Handler strips
	// hop-by-hop headers again as a backstop.
	Header http.Header
	Body   io.ReadCloser
	// RequestID is the upstream's ID for this request, for the log. May be empty.
	RequestID string
}

// RateLimiter decides whether a client may make another request. client is
// the identity RequireKey derived from the client's key.
type RateLimiter interface {
	// Allow takes one request from client's allowance. An error means the
	// limiter could not decide; the Handler then applies its fail mode.
	Allow(ctx context.Context, client string) (RateLimit, error)
}

// RateLimit is a RateLimiter's decision.
type RateLimit struct {
	Allowed bool
	// Remaining is how many more requests the client may make right away.
	Remaining int64
	// RetryAfter is how long until the next request would be allowed, when
	// this one is not.
	RetryAfter time.Duration
}

// RequestError rejects a client request as invalid. The handler answers it
// with Status carrying Message, so Message must be safe to show the client.
type RequestError struct {
	Message string
	// Status is the 4xx status to answer with. Zero means 400; see status
	// for the ones that also become 400.
	Status int
}

func (e *RequestError) Error() string { return e.Message }

// status returns the status to answer e with. Anything outside 400-499
// becomes 400: a RequestError is the client's fault, and a provider must not
// turn it into a 5xx (logged as a gateway failure, and retried by SDKs) or a
// success. Some 4xx mislead and become 400 too.
func (e *RequestError) status() int {
	switch e.Status {
	case http.StatusUnauthorized:
		// Says the client's gateway key is wrong; RequireKey owns that.
		return http.StatusBadRequest
	case http.StatusProxyAuthRequired:
		// Asks the client to authenticate to a proxy it never configured.
		return http.StatusBadRequest
	case http.StatusTooManyRequests:
		// SDKs retry it, so a rejected request would be sent again and
		// again; and it reads as the rate limiter's answer.
		return http.StatusBadRequest
	case 499:
		// The gateway's own log code for a client that went away.
		return http.StatusBadRequest
	}
	if e.Status < 400 || e.Status > 499 {
		return http.StatusBadRequest
	}
	return e.Status
}

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
	errRateLimited     = errors.New("client over its rate limit")
	errBadResponse     = errors.New("provider broke its contract")
)

// limiterWarnInterval spaces out the warnings logged while the rate limiter
// is unavailable in fail-open mode, so an outage doesn't flood the log.
const limiterWarnInterval = 10 * time.Second

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
	// Ignored when Admission is set.
	MaxConcurrent int
	// Admission, if set, is the cap shared with other Handlers; it must come
	// from NewAdmission. If nil, the Handler gets its own, of MaxConcurrent
	// slots.
	Admission *Admission
	// RateLimiter, if set, is asked before the body is read whether the
	// authenticated client may make this request. Requests that did not pass
	// through RequireKey have no client identity and are not limited.
	RateLimiter RateLimiter
	// RateLimitFailClosed answers 503 when RateLimiter returns an error. By
	// default (fail open) the request is allowed and a warning logged.
	RateLimitFailClosed bool
	// Upstream names the upstream the Handler forwards to, for the request
	// log's "upstream" field; "provider" is the protocol's Name(). Empty
	// means the provider's Name().
	Upstream string
	// Recorder, if set, gets a RequestInfo for every request. Nil records
	// nothing and costs nothing.
	Recorder Recorder
}

// Handler proxies a request to a Provider and returns its response as-is.
type Handler struct {
	provider      Provider
	upstream      string
	logger        *slog.Logger
	headerTimeout time.Duration
	idleTimeout   time.Duration
	bodyTimeout   time.Duration
	maxBody       int64
	admission     *Admission
	limiter       RateLimiter
	failClosed    bool
	recorder      Recorder
	limiterWarn   logThrottle // fail-open warnings
}

// NewHandler returns a Handler forwarding to p. It panics if opts.Admission
// was not built by NewAdmission, since a zero-value Admission would shed
// every request.
func NewHandler(p Provider, logger *slog.Logger, opts Options) *Handler {
	if opts.Admission != nil && opts.Admission.slots == nil {
		panic("proxy: Options.Admission must be created with NewAdmission")
	}
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
	if opts.Admission == nil {
		opts.Admission = NewAdmission(opts.MaxConcurrent)
	}
	if opts.Upstream == "" {
		opts.Upstream = p.Name()
	}
	return &Handler{
		provider:      p,
		upstream:      opts.Upstream,
		logger:        logger,
		headerTimeout: opts.HeaderTimeout,
		idleTimeout:   opts.IdleTimeout,
		bodyTimeout:   opts.BodyTimeout,
		maxBody:       opts.MaxBodyBytes,
		admission:     opts.Admission,
		limiter:       opts.RateLimiter,
		failClosed:    opts.RateLimitFailClosed,
		recorder:      opts.Recorder,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := rand.Text()
	w.Header().Set("X-Request-Id", reqID)

	var (
		model, upstreamID           string
		status                      int
		phase                       = PhaseReadBody // the stage reached, or where it failed
		limitDur                    time.Duration
		limit                       *RateLimit // nil unless the limiter decided
		readDur, ttfbDur, streamDur time.Duration
		failure                     error
		outcome                     RateLimitOutcome // for the Recorder
		ran                         Stages           // for the Recorder
	)
	defer func() {
		// A panic (a provider bug) unwinds through here before net/http
		// recovers it. Record it as the server failure it is, then let it
		// go on with its own value. The stack is taken now, while it still
		// shows where the panic happened; the re-panic's won't.
		recovered := recover()
		var stack []byte
		if recovered != nil {
			stack = debug.Stack()
			if status == 0 {
				status = http.StatusInternalServerError
			}
			failure = fmt.Errorf("panic: %v", recovered)
		}
		total := time.Since(start)
		attrs := []slog.Attr{
			slog.String("request_id", reqID),
			slog.String("upstream", h.upstream),
			slog.String("provider", h.provider.Name()),
			slog.String("model", model),
			slog.Int("status", status),
			slog.Int64("latency_ms", total.Milliseconds()),
		}
		if h.limiter != nil {
			// Microseconds: a healthy Redis answers well under a millisecond.
			attrs = append(attrs, slog.Int64("rate_limit_us", limitDur.Microseconds()))
		}
		if limit != nil {
			attrs = append(attrs, slog.Int64("rate_limit_remaining", limit.Remaining))
		}
		attrs = append(attrs, slog.Int64("read_body_ms", readDur.Milliseconds()))
		if phase == PhaseUpstream || phase == PhaseStream {
			attrs = append(attrs, slog.Int64("upstream_ttfb_ms", ttfbDur.Milliseconds()))
		}
		if phase == PhaseStream {
			attrs = append(attrs, slog.Int64("stream_ms", streamDur.Milliseconds()))
		}
		if upstreamID != "" {
			attrs = append(attrs, slog.String("upstream_request_id", upstreamID))
		}
		if failure != nil {
			attrs = append(attrs, slog.String("error", failure.Error()), slog.String("phase", phase))
		}
		if stack != nil {
			attrs = append(attrs, slog.String("stack", string(stack)))
		}
		_, clientFailed := errors.AsType[*clientWriteError](failure)
		clientGone := clientFailed || r.Context().Err() != nil
		h.logger.LogAttrs(r.Context(), logLevel(status, phase, failure, clientGone), "request", attrs...)
		if h.recorder != nil {
			info := RequestInfo{
				Upstream:          h.upstream,
				Protocol:          h.provider.Name(),
				Status:            status,
				Failed:            failure != nil,
				RateLimit:         outcome,
				Ran:               ran,
				RateLimitDuration: limitDur,
				ReadBody:          readDur,
				UpstreamTTFB:      ttfbDur,
				Stream:            streamDur,
				Total:             total,
			}
			if info.Failed {
				info.Phase = phase
			}
			h.recorder.RecordRequest(r.Context(), info)
		}
		if recovered != nil {
			panic(recovered)
		}
	}()

	// The body is forwarded and inspected as-is, so an encoded one would be
	// unreadable upstream and opaque to any check on its contents.
	if ce := r.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		failure = errContentEncoding
		status = http.StatusUnsupportedMediaType
		rejectEarly(w, h.provider, status, ErrInvalidRequest, "content-encoding is not supported; send an uncompressed body")
		return
	}

	// A declared length over the limit is rejected before reading anything.
	if r.ContentLength > h.maxBody {
		failure = &http.MaxBytesError{Limit: h.maxBody}
		status = http.StatusRequestEntityTooLarge
		rejectEarly(w, h.provider, status, ErrRequestTooLarge, "request body too large")
		return
	}

	// Before the body is read, so a client over its limit costs one Redis
	// call and no upload.
	if id := clientID(r.Context()); h.limiter != nil && id != "" {
		phase = PhaseRateLimit
		ran |= StageRateLimit
		limitStart := time.Now()
		lim, err := h.limiter.Allow(r.Context(), id)
		limitDur = time.Since(limitStart)
		switch {
		case err != nil && r.Context().Err() != nil:
			// The client went away; Redis may be fine, and nobody is left
			// to answer.
			failure = err
			status = 499 // client closed request
			return
		case err != nil && h.failClosed:
			outcome = RateLimitFailClosed
			failure = fmt.Errorf("rate limiter unavailable: %w", err)
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", "1")
			rejectEarly(w, h.provider, status, ErrAPI, "rate limiter unavailable")
			return
		case err != nil:
			outcome = RateLimitFailOpen
			h.warnLimiterDown(r.Context(), err)
		case !lim.Allowed:
			outcome = RateLimitLimited
			limit = &lim
			failure = errRateLimited
			status = http.StatusTooManyRequests
			w.Header().Set("Retry-After", retryAfterSeconds(lim.RetryAfter))
			rejectEarly(w, h.provider, status, ErrRateLimit, "rate limit exceeded")
			return
		default:
			outcome = RateLimitAllowed
			limit = &lim
		}
		phase = PhaseReadBody
	}

	// The read deadline is per request, not server-wide, and is cleared once
	// the body is in: a read deadline expiring later, during a long stream,
	// would be treated as the client going away.
	rc := http.NewResponseController(w)
	ran |= StageReadBody
	readStart := time.Now()
	_ = rc.SetReadDeadline(readStart.Add(h.bodyTimeout))
	body, err := readBody(http.MaxBytesReader(w, r.Body, h.maxBody), r.ContentLength)
	_ = rc.SetReadDeadline(time.Time{})
	readDur = time.Since(readStart)
	if err != nil {
		failure = err
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			status = http.StatusRequestEntityTooLarge
			rejectEarly(w, h.provider, status, ErrRequestTooLarge, "request body too large")
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			status = http.StatusRequestTimeout
			rejectEarly(w, h.provider, status, ErrTimeout, "request body not received in time")
			return
		}
		status = http.StatusBadRequest
		rejectEarly(w, h.provider, status, ErrInvalidRequest, "could not read request body")
		return
	}
	model = truncate(extractModel(body), maxLoggedModel)

	// The slot is taken only once the body is in, so a slow upload can't hold
	// one. Shed load rather than queue: a queued request still holds its
	// connection, and waiting only moves the memory pressure elsewhere.
	phase = PhaseAdmit
	if !h.admission.TryAcquire() {
		failure = errOverloaded
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
		rejectEarly(w, h.provider, status, ErrOverloaded, "gateway is at capacity")
		return
	}
	defer h.admission.Release()

	// One context for the whole upstream exchange. Timers cancel it with a
	// cause, so a timeout is distinguishable from the client going away.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)

	phase = PhaseUpstream
	ran |= StageUpstreamTTFB
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
			status = reqErr.status()
			writeError(w, h.provider, status, ErrInvalidRequest, reqErr.Message)
		case r.Context().Err() != nil:
			status = 499 // client closed request; nobody to answer
		case errors.Is(context.Cause(ctx), errHeaderTimeout):
			failure = errHeaderTimeout
			status = http.StatusGatewayTimeout
			writeError(w, h.provider, status, ErrTimeout, "upstream timed out")
		default:
			status = http.StatusBadGateway
			writeError(w, h.provider, status, ErrAPI, "upstream unavailable")
		}
		return
	}
	if err := checkResponse(resp); err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		failure = err
		status = http.StatusBadGateway
		writeError(w, h.provider, status, ErrAPI, "upstream unavailable")
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
		writeError(w, h.provider, status, ErrAPI, "upstream authentication failed")
		return
	}
	copyEndToEnd(w.Header(), resp.Header)
	w.Header().Set("X-Request-Id", reqID) // always the gateway's own
	status = resp.Status
	w.WriteHeader(status)

	phase = PhaseStream
	ran |= StageStream
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

// checkResponse reports whether a Provider kept its contract for a nil
// error: a non-nil Response with a non-nil Body and a 2xx, 4xx or 5xx
// status. WriteHeader panics below 100 and would send a 1xx as an
// informational response. A 3xx is never an answer to pass on: upstream
// clients refuse redirects, so one that gets here has no Location, or is a
// 300 or 304, which a client can't act on (a 304 can't even carry the body).
func checkResponse(resp *Response) error {
	switch {
	case resp == nil:
		return fmt.Errorf("%w: nil response", errBadResponse)
	case resp.Body == nil:
		return fmt.Errorf("%w: nil body", errBadResponse)
	case resp.Status < 200 || resp.Status > 599 || (resp.Status >= 300 && resp.Status < 400):
		return fmt.Errorf("%w: status %d", errBadResponse, resp.Status)
	}
	return nil
}

// hopByHop are Connection and the connection-specific headers RFC 9110
// section 7.6.1 names (Proxy-Connection, Keep-Alive, TE, Transfer-Encoding,
// Upgrade), plus Trailer, Proxy-Authenticate and Proxy-Authorization, which
// RFC 2616 listed as hop-by-hop and httputil.ReverseProxy still strips. They
// describe one connection, not the response, so they never pass a proxy.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Connection":    true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Te":                  true,
	"Trailer":             true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
}

// copyEndToEnd appends src's headers to dst, leaving out hop-by-hop headers
// and any header src's Connection value names. Providers allowlist their
// response headers already; this is the backstop for one that doesn't.
// Names are canonicalized, since a Provider may build its Header by hand,
// and written to dst canonical too: a provider's "x-request-id" must not
// end up beside the gateway's X-Request-Id.
func copyEndToEnd(dst, src http.Header) {
	named := map[string]bool{}
	for k, vv := range src {
		if http.CanonicalHeaderKey(k) != "Connection" {
			continue
		}
		for _, v := range vv {
			for f := range strings.SplitSeq(v, ",") {
				if f = strings.TrimSpace(f); f != "" {
					named[http.CanonicalHeaderKey(f)] = true
				}
			}
		}
	}
	for k, vv := range src {
		ck := http.CanonicalHeaderKey(k)
		if hopByHop[ck] || named[ck] {
			continue
		}
		dst[ck] = append(dst[ck], vv...)
	}
}

// warnLimiterDown logs that the rate limiter failed and the request was let
// through, at most once per limiterWarnInterval, with a count of the
// failures not logged since the last warning.
func (h *Handler) warnLimiterDown(ctx context.Context, err error) {
	ok, suppressed := h.limiterWarn.allow(time.Now(), limiterWarnInterval)
	if !ok {
		return
	}
	h.logger.LogAttrs(ctx, slog.LevelWarn, "rate limiter unavailable; allowing requests (fail open)",
		slog.String("error", err.Error()),
		slog.Int64("suppressed", suppressed),
	)
}

// retryAfterSeconds formats d for a Retry-After header: whole seconds,
// rounded up, at least 1.
func retryAfterSeconds(d time.Duration) string {
	s := int64((d + time.Second - 1) / time.Second)
	return strconv.FormatInt(max(s, 1), 10)
}

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
	case phase == PhaseStream && !clientGone:
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
