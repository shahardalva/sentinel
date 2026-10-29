package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// swapSource is a KeySource whose set the test replaces, counting reads.
type swapSource struct {
	set   atomic.Pointer[KeySet]
	reads atomic.Int64
}

func (s *swapSource) Keys() *KeySet {
	s.reads.Add(1)
	return s.set.Load()
}

func setOf(keys ...string) *KeySet {
	var d [][sha256.Size]byte
	for _, k := range keys {
		d = append(d, sha256.Sum256([]byte(k)))
	}
	return NewKeySet(d)
}

func authStatus(h http.Handler, key string) int {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

// Each request is checked against the set current when it arrives, read
// once, and the client's identity is still the hex digest of its key, so
// rate-limit buckets don't change.
func TestAuthSource_ChecksCurrentSet(t *testing.T) {
	src := &swapSource{}
	src.set.Store(setOf("a", "b"))
	var id string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { id = clientID(r.Context()) })
	h := NewAuthSource(src, slog.New(slog.DiscardHandler), nil).Require(plainErrors, next)

	if got := authStatus(h, "b"); got != http.StatusOK {
		t.Fatalf("b: %d, want 200", got)
	}
	if sum := sha256.Sum256([]byte("b")); id != hex.EncodeToString(sum[:]) {
		t.Errorf("clientID = %q, want the hex SHA-256 of the key", id)
	}
	if n := src.reads.Load(); n != 1 {
		t.Errorf("Keys called %d times for one request, want 1", n)
	}

	src.set.Store(setOf("a"))
	if got := authStatus(h, "b"); got != http.StatusUnauthorized {
		t.Errorf("b after removal: %d, want 401", got)
	}
	if got := authStatus(h, "a"); got != http.StatusOK {
		t.Errorf("a after removal: %d, want 200", got)
	}
}

// A nil or empty set rejects every request, including one with no key,
// whose digest is that of "".
func TestAuthSource_EmptySetRejects(t *testing.T) {
	for name, set := range map[string]*KeySet{"nil": nil, "empty": NewKeySet(nil), "empty-key digest": setOf("")} {
		t.Run(name, func(t *testing.T) {
			src := &swapSource{}
			src.set.Store(set)
			h := NewAuthSource(src, slog.New(slog.DiscardHandler), nil).Require(plainErrors, http.NotFoundHandler())
			for _, k := range []string{"", "k"} {
				if got := authStatus(h, k); got != http.StatusUnauthorized {
					t.Errorf("key %q: %d, want 401", k, got)
				}
			}
		})
	}
}

func TestNewKeySet_Copies(t *testing.T) {
	d := [][sha256.Size]byte{sha256.Sum256([]byte("k"))}
	s := NewKeySet(d)
	d[0] = sha256.Sum256([]byte("other"))
	if s.list()[0] != sha256.Sum256([]byte("k")) {
		t.Error("NewKeySet kept the caller's slice")
	}
	if s.Len() != 1 || (*KeySet)(nil).Len() != 0 {
		t.Error("Len wrong")
	}
}

func TestNewAuthSource_NilPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewAuthSource(nil) did not panic")
		}
	}()
	NewAuthSource(nil, slog.New(slog.DiscardHandler), nil)
}

func TestUnavailableKeySet(t *testing.T) {
	u := UnavailableKeySet()
	if !u.Unavailable() || u.Len() != 0 || u.list() != nil {
		t.Errorf("UnavailableKeySet: unavailable %v, len %d; want true, 0, no digests", u.Unavailable(), u.Len())
	}
	var nilSet *KeySet
	for name, s := range map[string]*KeySet{"nil": nilSet, "empty": NewKeySet(nil), "one": NewKeySet(DigestKeys([]string{"k"}))} {
		if s.Unavailable() {
			t.Errorf("%s set is unavailable", name)
		}
	}
}
