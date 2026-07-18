package licensing

// Effective calculates the merged entitlements between a product license (deployment)
// and a tenant subscription (customer) per MANAGEMENT.md §18.
func Effective(dep, cust Entitlements) Entitlements {
	var eff Entitlements
	eff.Limits.MaxRPS = minNonZero(dep.Limits.MaxRPS, cust.Limits.MaxRPS)
	eff.Limits.MaxRequestsPerDay = minNonZero(dep.Limits.MaxRequestsPerDay, cust.Limits.MaxRequestsPerDay)
	eff.Limits.MaxActiveCampaigns = minNonZero(dep.Limits.MaxActiveCampaigns, cust.Limits.MaxActiveCampaigns)
	eff.Limits.MaxRegions = minNonZero(dep.Limits.MaxRegions, cust.Limits.MaxRegions)
	eff.Limits.MaxTenants = minNonZero(dep.Limits.MaxTenants, cust.Limits.MaxTenants)
	eff.Limits.MaxEventsPerMonth = minNonZero(dep.Limits.MaxEventsPerMonth, cust.Limits.MaxEventsPerMonth)
	eff.Limits.MaxAPIKeys = minNonZero(dep.Limits.MaxAPIKeys, cust.Limits.MaxAPIKeys)
	eff.Limits.MaxExportChunkBytes = minNonZero(dep.Limits.MaxExportChunkBytes, cust.Limits.MaxExportChunkBytes)

	eff.Limits.QuotaResetTimezone = cust.Limits.QuotaResetTimezone
	if eff.Limits.QuotaResetTimezone == "" {
		eff.Limits.QuotaResetTimezone = dep.Limits.QuotaResetTimezone
	}
	if eff.Limits.QuotaResetTimezone == "" {
		eff.Limits.QuotaResetTimezone = "UTC"
	}

	eff.Features.RtbLive = dep.Features.RtbLive && cust.Features.RtbLive
	eff.Features.MlFraudBoost = dep.Features.MlFraudBoost && cust.Features.MlFraudBoost
	eff.Features.MultiRegion = dep.Features.MultiRegion && cust.Features.MultiRegion
	eff.Features.SlotMigration = dep.Features.SlotMigration && cust.Features.SlotMigration

	return eff
}

func minNonZero(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}
