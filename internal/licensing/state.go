package licensing

import (
	"time"
)

// DetermineState computes the LicenseState based on claims, current time, and a revocation check.
func DetermineState(claims *LicenseClaims, now time.Time, revoked bool) LicenseState {
	if revoked {
		return StateRevoked
	}
	if claims == nil {
		return StateExpired
	}
	if now.Before(claims.ValidFrom) {
		return StateExpired // Or not yet valid, which defaults to expired for enforcement
	}
	if now.Before(claims.ValidUntil) {
		return StateActive
	}
	graceDuration := time.Duration(claims.GraceDays) * 24 * time.Hour
	if now.Before(claims.ValidUntil.Add(graceDuration)) {
		return StateGrace
	}
	return StateExpired
}
