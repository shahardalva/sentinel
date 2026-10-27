package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// MinTokenLen is the shortest admin token ValidateToken accepts.
const MinTokenLen = 32

// authWarnInterval spaces out the "admin unauthorized" warnings, so a
// client sending bad tokens can't flood the log.
const authWarnInterval = 10 * time.Second

// ValidateToken reports whether token can be the admin token: at least
// MinTokenLen characters, all printable ASCII other than space, so no
// whitespace, control or non-ASCII character that a client could send
// differently from how it was configured. The error never holds the token.
func ValidateToken(token string) error {
	if len(token) < MinTokenLen {
		return errors.New("admin token is shorter than 32 characters")
	}
	for i := 0; i < len(token); i++ {
		if c := token[i]; c <= ' ' || c >= 0x7f {
			return errors.New("admin token holds whitespace, a control character or a non-ASCII character")
		}
	}
	return nil
}

// requireToken rejects every request that doesn't present the admin token
// as an Authorization bearer token, before next runs, so nothing reads the
// body or calls the store for it. The digests are compared in constant
// time; comparing digests means the comparison is over the same length
// whatever was sent.
func (h *handler) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header)
		got := sha256.Sum256([]byte(presented))
		if subtle.ConstantTimeCompare(got[:], h.token[:]) != 1 || !ok {
			h.reject(r)
			w.Header().Set("WWW-Authenticate", "Bearer")
			rejectEarly(w, http.StatusUnauthorized, errAuthentication, "missing or invalid admin token")
			return
		}
		r.Header.Del("Authorization")
		next.ServeHTTP(w, r)
	})
}

// maxLoggedPath caps the path in the "admin unauthorized" warning: the
// client chooses it, up to the server's header limit.
const maxLoggedPath = 256

// reject logs a rejected request, if the throttle allows. The path is
// logged escaped, which is all ASCII, so cutting it at maxLoggedPath bytes
// can't split a character; a cut one carries path_truncated and its full
// length.
func (h *handler) reject(r *http.Request) {
	ok, suppressed := h.warn.Allow(h.now(), authWarnInterval)
	if !ok {
		return
	}
	p := r.URL.EscapedPath()
	attrs := []slog.Attr{slog.String("path", p)}
	if len(p) > maxLoggedPath {
		attrs = []slog.Attr{
			slog.String("path", p[:maxLoggedPath]),
			slog.Bool("path_truncated", true),
			slog.Int("path_len", len(p)),
		}
	}
	attrs = append(attrs,
		slog.String("remote_addr", r.RemoteAddr),
		slog.Int64("suppressed", suppressed),
	)
	h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin unauthorized", attrs...)
}

// bearerToken returns the token of the request's only Authorization
// header, if it is a non-empty bearer token. The scheme name is
// case-insensitive (RFC 9110 §11.1).
func bearerToken(h http.Header) (string, bool) {
	vals := h.Values("Authorization")
	if len(vals) != 1 {
		return "", false
	}
	scheme, tok, ok := strings.Cut(vals[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || tok == "" {
		return "", false
	}
	return tok, true
}
