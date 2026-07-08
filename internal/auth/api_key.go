package auth

import (
	"crypto/sha256"
	"encoding/hex"
)

// apiKeyLookup returns a stable SHA-256 hex digest for indexed API key lookup without storing plaintext.
func apiKeyLookup(rawKey string) string {
	sum := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(sum[:])
}
