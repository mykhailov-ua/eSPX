package ingestion

import (
	"errors"
	"strings"

	"espx/internal/config"
)

var ErrInvalidRtbBudgetAuthority = errors.New("rtb_budget_authority must be rtb or lua")

const systemSettingRtbBudgetAuthority = "rtb_budget_authority"

// BudgetAuthorityFromSettings resolves RTB budget owner from env plus optional system_settings override.
// Values: rtb (in-process RTB store) or lua/redis (unified-filter.lua).
func BudgetAuthorityFromSettings(cfg *config.Config, setting string) BudgetAuthority {
	if cfg == nil || !cfg.RtbEnabled() {
		return BudgetAuthorityShadow
	}
	if !cfg.RtbLiveSelectsCampaign() {
		return BudgetAuthorityShadow
	}
	raw := strings.TrimSpace(setting)
	if raw == "" {
		raw = cfg.RtbBudgetAuthority
	}
	switch strings.ToLower(raw) {
	case "rtb":
		return BudgetAuthorityRTB
	case "lua", "redis", "":
		return BudgetAuthorityRedis
	default:
		return BudgetAuthorityRedis
	}
}

// RtbSkipLuaBudgetDebit reports whether unified-filter.lua should skip budget debits.
func RtbSkipLuaBudgetDebit(cfg *config.Config, setting string) bool {
	return BudgetAuthorityFromSettings(cfg, setting) == BudgetAuthorityRTB
}

// NormalizeRtbBudgetAuthoritySetting validates admin setting values.
func NormalizeRtbBudgetAuthoritySetting(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "rtb":
		return "rtb", nil
	case "lua", "redis":
		return "lua", nil
	case "":
		return "", nil
	default:
		return "", ErrInvalidRtbBudgetAuthority
	}
}
