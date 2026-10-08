package proxy

import (
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
)

// RequireKey rejects requests that don't present one of keys, either as
// x-api-key (what the Anthropic SDKs send) or as an Authorization bearer
// token. With no non-empty keys every request is rejected. The credential
// headers are removed before next runs, so later stages never see them.
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
			writeError(w, http.StatusUnauthorized, "authentication_error", "invalid x-api-key")
			return
		}

		r = r.Clone(r.Context())
		r.Header.Del("X-Api-Key")
		r.Header.Del("Authorization")
		next.ServeHTTP(w, r)
	})
}

func clientKey(h http.Header) string {
	if k := h.Get("X-Api-Key"); k != "" {
		return k
	}
	if tok, ok := strings.CutPrefix(h.Get("Authorization"), "Bearer "); ok {
		return tok
	}
	return ""
}
