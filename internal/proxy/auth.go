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

// RequireKey rejects requests that don't present one of keys, either as
// x-api-key (what the Anthropic SDKs send) or as an Authorization bearer
// token. With no non-empty keys every request is rejected. The credential
// headers are removed before next runs, so later stages never see them.
// They see only a SHA-256 digest of the key, as the client's identity for
// rate limiting.
func RequireKey(keys []string, logger *slog.Logger, next http.Handler) http.Handler {
	// Compare SHA-256 digests so every comparison is over the same length and
	// timing reveals nothing about a key's length or contents.
	var digests [][sha256.Size]byte
	for _, k := range keys {
		if k != "" {
			digests = append(digests, sha256.Sum256([]byte(k)))
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.SetReadDeadline(time.Now().Add(authReadTimeout))

		presented := clientKey(r.Header)
		got := sha256.Sum256([]byte(presented))
		match := 0
		for _, d := range digests {
			match |= subtle.ConstantTimeCompare(got[:], d[:])
		}
		if presented == "" || match != 1 {
			logger.LogAttrs(r.Context(), slog.LevelWarn, "unauthorized",
				slog.String("path", r.URL.Path),
				slog.String("remote_addr", r.RemoteAddr),
			)
			rejectEarly(w, http.StatusUnauthorized, "authentication_error", "invalid x-api-key")
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
