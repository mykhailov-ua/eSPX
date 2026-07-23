package licensing

// Effective calculates the merged entitlements between a product license (deployment)
// and a tenant subscription (customer) per MANAGEMENT.md section 18.
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

	eff.VolumeBand = dep.VolumeBand
	if eff.VolumeBand == "" {
		eff.VolumeBand = cust.VolumeBand
	}

	depFeat := dep.Features.Normalized()
	custFeat := cust.Features.Normalized()
	eff.Features.RtbLive = depFeat.RtbLive && custFeat.RtbLive
	eff.Features.OpenRTBEngine = depFeat.OpenRTBEnabled() && custFeat.OpenRTBEnabled()
	eff.Features.IvtMLDetector = depFeat.IvtMLDetector && custFeat.IvtMLDetector
	eff.Features.EbpfXDPEdge = depFeat.EbpfXDPEdge && custFeat.EbpfXDPEdge
	eff.Features.MlFraudBoost = depFeat.MlFraudBoost && custFeat.MlFraudBoost
	eff.Features.MultiRegion = depFeat.MultiRegion && custFeat.MultiRegion
	eff.Features.SlotMigration = depFeat.SlotMigration && custFeat.SlotMigration
	eff.Features.MarginGuard = depFeat.MarginGuard && custFeat.MarginGuard

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
