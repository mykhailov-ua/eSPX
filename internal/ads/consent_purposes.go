package ads

import (
	"crypto/sha256"
	"encoding/hex"
)

// Consent purpose bit flags (16-bit mask, M6.2–M6.3).
const (
	ConsentPurposeAdStorage       int16 = 1 << 0
	ConsentPurposeAnalytics       int16 = 1 << 1
	ConsentRedisKeyPrefix               = "consent:user:"
	ConsentDefaultUpdateChannel           = "consent:update"
)

// HashUserID derives a stable SHA-256 digest for consent and erasure keys.
func HashUserID(userID string) []byte {
	sum := sha256.Sum256([]byte(userID))
	return sum[:]
}

// HashUserIDHex returns hex-encoded HashUserID for Redis keys and pub/sub payloads.
func HashUserIDHex(userID string) string {
	return hex.EncodeToString(HashUserID(userID))
}

// ConsentFlagsFromPurposes maps purpose bits to Postgres storage flags.
func ConsentFlagsFromPurposes(purposes int16) (adStorage, analyticsStorage bool) {
	adStorage = purposes&ConsentPurposeAdStorage != 0
	analyticsStorage = purposes&ConsentPurposeAnalytics != 0
	return adStorage, analyticsStorage
}
