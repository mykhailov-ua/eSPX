package licensing

import "time"

// LicenseClaims represents the payload of the Ed25519-signed product license.
type LicenseClaims struct {
	Issuer       string     `json:"iss"`
	Subject      string     `json:"sub"`
	KeyID        string     `json:"kid"`
	DeploymentID string     `json:"deployment_id"`
	CustomerName string     `json:"customer_name"`
	Plan         string     `json:"plan"` // starter|growth|enterprise
	ValidFrom    time.Time  `json:"valid_from"`
	ValidUntil   time.Time  `json:"valid_until"`
	GraceDays    int        `json:"grace_days"`
	Limits       Limits     `json:"limits"`
	Features     FeatureSet `json:"features"`
	Bind         struct {
		Mode        string `json:"mode"`
		Fingerprint string `json:"fingerprint"`
	} `json:"bind"`
	SupportTier string `json:"support_tier"`
}

// LicenseState represents the operating state of the license.
type LicenseState string

const (
	StateActive  LicenseState = "ACTIVE"
	StateGrace   LicenseState = "GRACE"
	StateExpired LicenseState = "EXPIRED"
	StateRevoked LicenseState = "REVOKED"
)
