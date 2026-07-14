package ads

import (
	"espx/internal/domain"

	"github.com/google/uuid"
)

// quotaRefillSample selects ~1% of campaigns for full-path quota refill probes (§9.3 Tier C).
func quotaRefillSample(campaignID uuid.UUID) bool {
	return campaignID[0]%100 == 0
}

func ttcEnabled(ttcMinMsAny any) bool {
	if ttcMinMsAny == nil || ttcMinMsAny == zeroAny {
		return false
	}
	switch v := ttcMinMsAny.(type) {
	case int64:
		return v > 0
	case int:
		return v > 0
	default:
		return false
	}
}

// needsFullLuaPath routes to filter_full.lua when Tier B cannot satisfy campaign/event constraints.
func (f *UnifiedFilter) needsFullLuaPath(evt *domain.Event, campInfo *domain.Campaign) bool {
	if !f.fastPathEnabled.Load() {
		return true
	}
	if f.rateLimit > 0 {
		return true
	}
	if campInfo.FreqLimit > 0 && evt.UserID != "" {
		return true
	}
	if campInfo.PacingMode == domain.PacingModeEven {
		return true
	}
	if ttcEnabled(f.ttcMinMsAny) {
		if evt.Type == "click" || evt.Type == "impression" {
			return true
		}
	}
	if f.quotaEnabledAny == oneAny && quotaRefillSample(evt.CampaignID) {
		return true
	}
	return false
}
