package proxy_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shahardalva/sentinel/internal/proxy"
)

// wireLimiter is a RateLimiter with a fixed answer.
type wireLimiter struct {
	lim proxy.RateLimit
	err error
}

func (l wireLimiter) Allow(context.Context, string) (proxy.RateLimit, error) { return l.lim, l.err }

// rawExchange sends req as-is on a fresh connection to srv and returns the
// response exactly as it came off the wire, with the Date and X-Request-Id
// values, which change on every request, replaced by placeholders.
func rawExchange(t *testing.T, srv *httptest.Server, req string) string {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	resp, err := http.ReadResponse(bufio.NewReader(io.TeeReader(conn, &raw)), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	s := regexp.MustCompile(`(?m)^Date: .*\r$`).ReplaceAllString(raw.String(), "Date: <date>\r")
	return regexp.MustCompile(`(?m)^X-Request-Id: [A-Z2-7]+\r$`).ReplaceAllString(s, "X-Request-Id: <id>\r")
}

// TestGateway_ErrorResponsesOnTheWire pins the exact bytes of every error
// response the gateway itself produces on the Anthropic route: status line,
// headers and body.
func TestGateway_ErrorResponsesOnTheWire(t *testing.T) {
	const (
		authed = "POST /v1/messages HTTP/1.1\r\nHost: gw\r\nX-Api-Key: " + gatewayKey + "\r\n"
		body   = "Content-Length: 2\r\n\r\n{}"
	)
	// wire builds an error response as net/http writes it: the handler's
	// headers sorted, then Date and Content-Length. close marks responses
	// from rejectEarly; retryAfter is empty if the header is absent;
	// RequireKey answers before X-Request-Id is set.
	wire := func(status string, close bool, retryAfter string, withID bool, typ, msg string) string {
		b := `{"error":{"message":"` + msg + `","type":"` + typ + `"},"type":"error"}` + "\n"
		s := "HTTP/1.1 " + status + "\r\n"
		if close {
			s += "Connection: close\r\n"
		}
		s += "Content-Type: application/json\r\n"
		if retryAfter != "" {
			s += "Retry-After: " + retryAfter + "\r\n"
		}
		if withID {
			s += "X-Request-Id: <id>\r\n"
		}
		return s + "Date: <date>\r\nContent-Length: " + strconv.Itoa(len(b)) + "\r\n\r\n" + b
	}
	closing := func(status, retryAfter, typ, msg string) string {
		return wire(status, true, retryAfter, true, typ, msg)
	}
	keepAlive := func(status, typ, msg string) string {
		return wire(status, false, "", true, typ, msg)
	}

	// held blocks every upstream request until the test ends.
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	held := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer held.Close()
	defer close(release) // runs first, so Close doesn't wait on held handlers

	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer rejecting.Close()

	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close()

	tests := []struct {
		name     string
		upstream string
		opts     proxy.Options
		// hold, if set, sends one request first and waits until it is
		// held upstream.
		hold bool
		req  string
		want string
	}{
		{
			name:     "401 missing key",
			upstream: unusedUpstream,
			req:      "POST /v1/messages HTTP/1.1\r\nHost: gw\r\n" + body,
			want:     wire("401 Unauthorized", true, "", false, "authentication_error", "invalid x-api-key"),
		},
		{
			name:     "400 invalid anthropic-version",
			upstream: unusedUpstream,
			req:      authed + "Anthropic-Version: latest\r\n" + body,
			want:     keepAlive("400 Bad Request", "invalid_request_error", "anthropic-version must be a date in YYYY-MM-DD form"),
		},
		// No case for an anthropic-beta value with a control character:
		// net/http's server answers its own 400 before the gateway sees the
		// request, so a case here would pin net/http's response, not ours.
		// TestGateway_InvalidAnthropicBetaBytes pins the gateway's bytes
		// through a ResponseRecorder instead.
		{
			name:     "400 unreadable body",
			upstream: unusedUpstream,
			req:      authed + "Transfer-Encoding: chunked\r\n\r\nzz\r\n",
			want:     closing("400 Bad Request", "", "invalid_request_error", "could not read request body"),
		},
		{
			name:     "408 body not received in time",
			upstream: unusedUpstream,
			opts:     proxy.Options{BodyTimeout: 50 * time.Millisecond},
			req:      authed + "Content-Length: 100\r\n\r\n{\"model\":",
			want:     closing("408 Request Timeout", "", "timeout_error", "request body not received in time"),
		},
		{
			name:     "413 declared length",
			upstream: unusedUpstream,
			opts:     proxy.Options{MaxBodyBytes: 1},
			req:      authed + body,
			want:     closing("413 Request Entity Too Large", "", "request_too_large", "request body too large"),
		},
		{
			name:     "413 body over limit",
			upstream: unusedUpstream,
			opts:     proxy.Options{MaxBodyBytes: 1},
			req:      authed + "Transfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
			want:     closing("413 Request Entity Too Large", "", "request_too_large", "request body too large"),
		},
		{
			name:     "415 content-encoding",
			upstream: unusedUpstream,
			req:      authed + "Content-Encoding: gzip\r\n" + body,
			want:     closing("415 Unsupported Media Type", "", "invalid_request_error", "content-encoding is not supported; send an uncompressed body"),
		},
		{
			name:     "429 rate limited",
			upstream: unusedUpstream,
			opts:     proxy.Options{RateLimiter: wireLimiter{lim: proxy.RateLimit{RetryAfter: 1500 * time.Millisecond}}},
			req:      authed + body,
			want:     closing("429 Too Many Requests", "2", "rate_limit_error", "rate limit exceeded"),
		},
		{
			name:     "502 upstream unavailable",
			upstream: goneURL,
			req:      authed + body,
			want:     keepAlive("502 Bad Gateway", "api_error", "upstream unavailable"),
		},
		{
			name:     "502 upstream rejected the gateway's key",
			upstream: rejecting.URL,
			req:      authed + body,
			want:     keepAlive("502 Bad Gateway", "api_error", "upstream authentication failed"),
		},
		{
			name:     "503 overloaded",
			upstream: held.URL,
			opts:     proxy.Options{Admission: proxy.NewAdmission(1)}, // shared, as in cmd/gateway
			hold:     true,
			req:      authed + body,
			want:     closing("503 Service Unavailable", "1", "overloaded_error", "gateway is at capacity"),
		},
		{
			name:     "503 rate limiter unavailable",
			upstream: unusedUpstream,
			opts: proxy.Options{
				RateLimiter:         wireLimiter{err: errors.New("redis down")},
				RateLimitFailClosed: true,
			},
			req:  authed + body,
			want: closing("503 Service Unavailable", "1", "api_error", "rate limiter unavailable"),
		},
		{
			name:     "504 upstream timed out",
			upstream: held.URL,
			opts:     proxy.Options{HeaderTimeout: 50 * time.Millisecond},
			req:      authed + body,
			want:     keepAlive("504 Gateway Timeout", "timeout_error", "upstream timed out"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw, _ := newGateway(t, tt.upstream, tt.opts)
			srv := httptest.NewServer(gw)
			defer srv.Close()
			if tt.hold {
				// Cancelled before srv.Close, which would wait for it.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/messages", strings.NewReader("{}"))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("X-Api-Key", gatewayKey)
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
			}
			if got := rawExchange(t, srv, tt.req); got != tt.want {
				t.Errorf("response:\n%q\nwant:\n%q", got, tt.want)
			}
		})
	}
}
