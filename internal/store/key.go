package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
)

const (
	// KeyPrefix starts every client key the store issues, so a leaked key
	// is recognizable to secret scanners and people.
	KeyPrefix = "snl_"
	// keyBytes of crypto/rand make up a key: 256 bits, so a fast hash is
	// enough to store it (ADR 0005).
	keyBytes = 32
	// prefixLen is how much of a key is stored in the clear, to tell keys
	// apart in listings: "snl_" and 8 characters, 48 of the 256 bits.
	prefixLen = 12
)

// newKey returns a fresh client key, its displayable prefix and its
// SHA-256 digest, the same digest proxy.DigestKeys computes, so proxy.Auth
// accepts the key once the digest is in the key set.
func newKey() (key, prefix string, digest [sha256.Size]byte) {
	b := make([]byte, keyBytes)
	_, _ = rand.Read(b) // never fails; crashes the program if it can't
	key = KeyPrefix + base64.RawURLEncoding.EncodeToString(b)
	return key, key[:prefixLen], sha256.Sum256([]byte(key))
}
