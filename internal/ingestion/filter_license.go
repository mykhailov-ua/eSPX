package ingestion

import (
	"context"

	"espx/internal/campaignmodel"
	"espx/internal/licensing"
)

type licenseStateReader interface {
	GetLicenseState() (licensing.LicenseState, licensing.Entitlements)
}

// LicenseFilter rejects ingest only when the deployment license is EXPIRED or REVOKED.
// GRACE and ACTIVE continue per LICENSING.md §7.1 (zero network, atomic snapshot read).
type LicenseFilter struct {
	registry licenseStateReader
}

func NewLicenseFilter(registry licenseStateReader) *LicenseFilter {
	return &LicenseFilter{registry: registry}
}

func (f *LicenseFilter) Check(_ context.Context, _ *campaignmodel.Event) error {
	if f == nil || f.registry == nil {
		return nil
	}
	state, _ := f.registry.GetLicenseState()
	if state == licensing.StateExpired || state == licensing.StateRevoked {
		return ErrLicenseExpired
	}
	return nil
}
