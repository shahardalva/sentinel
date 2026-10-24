package proxy

import (
	"net/http"
	"time"
)

// ErrorKind is the protocol-independent meaning of an error the gateway
// itself answers with. An ErrorFormatter turns it into a protocol's shape.
type ErrorKind int

const (
	ErrInvalidRequest  ErrorKind = iota // the request is malformed or unacceptable
	ErrAuthentication                   // the client's key is missing or wrong
	ErrRateLimit                        // the client is over its rate limit
	ErrRequestTooLarge                  // the request body is over the size limit
	ErrTimeout                          // the client or the upstream took too long
	ErrOverloaded                       // the gateway is at its concurrency cap
	ErrAPI                              // an upstream or internal failure
)

// ErrorFormatter renders the gateway's own error responses in a protocol's
// shape, so clients of that protocol can parse them. The core never builds
// an error body itself.
type ErrorFormatter interface {
	// FormatError returns the Content-Type and body of an error response.
	// message is safe to show the client.
	FormatError(status int, kind ErrorKind, message string) (contentType string, body []byte)
}

// ErrorFormatterFunc adapts a function to an ErrorFormatter.
type ErrorFormatterFunc func(status int, kind ErrorKind, message string) (contentType string, body []byte)

func (f ErrorFormatterFunc) FormatError(status int, kind ErrorKind, message string) (string, []byte) {
	return f(status, kind, message)
}

// rejectEarly answers a request whose body may still be unread. net/http
// drains up to 256 KiB of an unread body, with no deadline, before sending
// the response and again when the handler returns, so a client that stalls
// mid-body would hold the connection open indefinitely. Connection: close
// skips the first drain; the read deadline bounds the second.
func rejectEarly(w http.ResponseWriter, f ErrorFormatter, status int, kind ErrorKind, msg string) {
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(RejectDrainTimeout))
	w.Header().Set("Connection", "close")
	writeError(w, f, status, kind, msg)
}

// writeError writes an error response rendered by f.
func writeError(w http.ResponseWriter, f ErrorFormatter, status int, kind ErrorKind, msg string) {
	contentType, body := f.FormatError(status, kind, msg)
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
