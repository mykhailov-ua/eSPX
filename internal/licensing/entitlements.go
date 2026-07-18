package licensing

// Limits holds quantitative caps for the installation or a single customer.
type Limits struct {
	MaxRPS              uint64 `json:"max_rps"`
	MaxRequestsPerDay   uint64 `json:"max_requests_per_day"`
	MaxActiveCampaigns  uint64 `json:"max_active_campaigns"`
	MaxRegions          uint64 `json:"max_regions"`
	MaxTenants          uint64 `json:"max_tenants"`
	MaxEventsPerMonth   uint64 `json:"max_events_per_month"`
	MaxAPIKeys          uint64 `json:"max_api_keys"`
	MaxExportChunkBytes uint64 `json:"max_export_chunk_bytes"`
	QuotaResetTimezone  string `json:"quota_reset_timezone"`
}

// FeatureSet holds boolean capabilities.
type FeatureSet struct {
	RtbLive       bool `json:"rtb_live"`
	MlFraudBoost  bool `json:"ml_fraud_boost"`
	MultiRegion   bool `json:"multi_region"`
	SlotMigration bool `json:"slot_migration"`
}

// Entitlements bundles Limits and FeatureSet.
type Entitlements struct {
	Limits   Limits     `json:"limits"`
	Features FeatureSet `json:"features"`
}

// LimitsDTO is a DTO copy of Limits for JSON stability.
type LimitsDTO struct {
	MaxRPS              uint64 `json:"max_rps"`
	MaxRequestsPerDay   uint64 `json:"max_requests_per_day"`
	MaxActiveCampaigns  uint64 `json:"max_active_campaigns"`
	MaxRegions          uint64 `json:"max_regions"`
	MaxTenants          uint64 `json:"max_tenants"`
	MaxEventsPerMonth   uint64 `json:"max_events_per_month"`
	MaxAPIKeys          uint64 `json:"max_api_keys"`
	MaxExportChunkBytes uint64 `json:"max_export_chunk_bytes"`
	QuotaResetTimezone  string `json:"quota_reset_timezone"`
}

// FeatureSetDTO is a DTO copy of FeatureSet for JSON stability.
type FeatureSetDTO struct {
	RtbLive       bool `json:"rtb_live"`
	MlFraudBoost  bool `json:"ml_fraud_boost"`
	MultiRegion   bool `json:"multi_region"`
	SlotMigration bool `json:"slot_migration"`
}

// LicenseStatusDTO represents the DTO for GET /api/v1/license/status.
type LicenseStatusDTO struct {
	DeploymentID   string        `json:"deployment_id"`
	LicenseID      string        `json:"license_id"`
	Plan           string        `json:"plan"`  // starter|growth|enterprise
	State          string        `json:"state"` // ACTIVE|GRACE|EXPIRED|REVOKED
	ValidUntil     string        `json:"valid_until"`
	GraceEndsAt    string        `json:"grace_ends_at,omitempty"`
	Limits         LimitsDTO     `json:"limits"`
	Features       FeatureSetDTO `json:"features"`
	LastVerifiedAt string        `json:"last_verified_at"`
	RefreshMode    string        `json:"refresh_mode"` // file|online
	LastRefreshErr string        `json:"last_refresh_error,omitempty"`
}
