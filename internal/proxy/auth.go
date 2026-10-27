package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// authReadTimeout bounds reads on a connection while its request is being
// authenticated, so a client without a key can't hold it open by sending a
// partial body. A var so tests can shorten it.
var authReadTimeout = 5 * time.Second

// authWarnInterval spaces out the "unauthorized" warnings, so a client
// sending bad keys can't flood the log.
const authWarnInterval = 10 * time.Second

// Auth checks client keys. One Auth is meant to guard every route, so the
// rate limit on its warnings holds across all of them. Create one with
// NewAuth or NewAuthSource.
type Auth struct {
	// keys supplies SHA-256 digests of the keys. Comparing digests means
	// every comparison is over the same length, and timing reveals nothing
	// about a key's length or contents.
	keys     KeySource
	logger   *slog.Logger
	recorder Recorder // may be nil
	warn     logThrottle
	now      func() time.Time // time.Now; tests replace it
}

// NewAuth returns an Auth accepting keys, which never change; empty keys
// are dropped, and with none left every request is rejected. rec, if not
// nil, is told of every rejection.
func NewAuth(keys []string, logger *slog.Logger, rec Recorder) *Auth {
	return NewAuthSource(fixedKeys{&KeySet{digests: DigestKeys(keys)}}, logger, rec)
}

// NewAuthSource returns an Auth accepting the keys src supplies, asking it
// for them on every request, so a request is checked against the set
// current when it arrives. A nil or empty KeySet rejects every request.
// rec, if not nil, is told of every rejection. NewAuthSource panics if src
// is nil.
func NewAuthSource(src KeySource, logger *slog.Logger, rec Recorder) *Auth {
	if src == nil {
		panic("proxy: NewAuthSource needs a non-nil KeySource")
	}
	return &Auth{keys: src, logger: logger, recorder: rec, now: time.Now}
}

// RequireKey rejects requests that don't present one of keys. It is
// NewAuth(keys, logger, nil).Require(errs, next), for a single route.
func RequireKey(keys []string, errs ErrorFormatter, logger *slog.Logger, next http.Handler) http.Handler {
	return NewAuth(keys, logger, nil).Require(errs, next)
}

// Require rejects requests that don't present one of a's keys, either as
// x-api-key (what the Anthropic SDKs send) or as an Authorization bearer
// token. The credential headers are removed before next runs, so later
// stages never see them. They see only a SHA-256 digest of the key, as the
// client's identity for rate limiting. A rejection is rendered by errs, the
// route's protocol, and logged at WARN at most once per authWarnInterval,
// with a count of the ones held back. Require panics if errs is nil, like
// http.Handle with a nil handler.
func (a *Auth) Require(errs ErrorFormatter, next http.Handler) http.Handler {
	if errs == nil {
		panic("proxy: RequireKey needs a non-nil ErrorFormatter")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Now().Add(authReadTimeout))

		presented := clientKey(r.Header)
		got := sha256.Sum256([]byte(presented))
		// One snapshot per request: a refresh during the loop can't mix two
		// sets. Every digest is compared, whether or not one already matched.
		match := 0
		for _, d := range a.keys.Keys().list() {
			match |= subtle.ConstantTimeCompare(got[:], d[:])
		}
		if presented == "" || match != 1 {
			a.reject(r)
			rejectEarly(w, errs, http.StatusUnauthorized, ErrAuthentication, "invalid x-api-key")
			return
		}

		// Cleared so it can't cut off next later, e.g. during a long stream;
		// next sets its own deadlines.
		_ = rc.SetReadDeadline(time.Time{})
		r = r.Clone(context.WithValue(r.Context(), clientIDKey{}, hex.EncodeToString(got[:])))
		r.Header.Del("X-Api-Key")
		r.Header.Del("Authorization")
		next.ServeHTTP(w, r)
	})
}

// reject records a rejected request and logs it, if the throttle allows.
func (a *Auth) reject(r *http.Request) {
	if a.recorder != nil {
		a.recorder.RecordAuthRejection(r.Context())
	}
	ok, suppressed := a.warn.allow(a.now(), authWarnInterval)
	if !ok {
		return
	}
	a.logger.LogAttrs(r.Context(), slog.LevelWarn, "unauthorized",
		slog.String("path", r.URL.Path),
		slog.String("remote_addr", r.RemoteAddr),
		slog.Int64("suppressed", suppressed),
	)
}

type clientIDKey struct{}

// clientID returns the authenticated client's identity set by RequireKey: the
// hex SHA-256 digest of its key, never the key itself. Empty if the request
// did not pass through RequireKey.
func clientID(ctx context.Context) string {
	id, _ := ctx.Value(clientIDKey{}).(string)
	return id
}

func clientKey(h http.Header) string {
	if k := h.Get("X-Api-Key"); k != "" {
		return k
	}
	// The scheme name is case-insensitive (RFC 9110 §11.1).
	scheme, tok, ok := strings.Cut(h.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return tok
}
