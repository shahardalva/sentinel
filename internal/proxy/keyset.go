package proxy

import (
	"crypto/sha256"
	"time"
)

// KeySet is an immutable set of SHA-256 digests of client keys. Its
// digests can't be changed after NewKeySet, so a KeySource can hand the
// same KeySet to any number of requests at once. A nil or empty KeySet
// accepts no key.
type KeySet struct {
	digests [][sha256.Size]byte
}

// DigestKeys returns the SHA-256 digests of keys, dropping empty keys: an
// empty key is never accepted.
func DigestKeys(keys []string) [][sha256.Size]byte {
	var digests [][sha256.Size]byte
	for _, k := range keys {
		if k != "" {
			digests = append(digests, sha256.Sum256([]byte(k)))
		}
	}
	return digests
}

// NewKeySet returns a KeySet of a copy of digests.
func NewKeySet(digests [][sha256.Size]byte) *KeySet {
	return &KeySet{digests: append([][sha256.Size]byte(nil), digests...)}
}

// Len returns the number of digests in s; 0 for a nil KeySet.
func (s *KeySet) Len() int {
	if s == nil {
		return 0
	}
	return len(s.digests)
}

// list returns s's digests, for reading only; nil for a nil KeySet.
func (s *KeySet) list() [][sha256.Size]byte {
	if s == nil {
		return nil
	}
	return s.digests
}

// KeySource supplies the client keys an Auth accepts. Keys is called once
// per request, on the request's goroutine, before the request goes any
// further than hashing the presented key; it must be safe for concurrent
// use and cheap. A source that changes its keys returns a new KeySet rather
// than changing one already returned. One KeySource serves the whole
// process (see Recorder.RecordKeySetSize).
type KeySource interface {
	Keys() *KeySet
}

// fixedKeys is a KeySource whose keys never change, for NewAuth and
// RequireKey.
type fixedKeys struct{ set *KeySet }

func (f fixedKeys) Keys() *KeySet { return f.set }

// LogThrottle lets a repeated log line through at most once per interval
// and counts the ones it holds back, as the gateway's own warnings do. The
// zero value is ready to use. It is for packages outside proxy, such as
// internal/keys; see logThrottle. Exporting it from proxy is a stopgap
// while internal/keys is its only outside user.
type LogThrottle struct{ t logThrottle }

// Allow reports whether to log at now, and if so how many lines were held
// back since the last one. now must not be before the process started:
// pass time.Now(), or in a testing/synctest bubble, whose clock starts in
// 2000, a time offset from one taken outside it.
func (t *LogThrottle) Allow(now time.Time, interval time.Duration) (ok bool, suppressed int64) {
	return t.t.allow(now, interval)
}
