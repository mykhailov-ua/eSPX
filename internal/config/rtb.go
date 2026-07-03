package config

import "strings"

// RtbMode controls in-process auction participation on the tracker hot path.
type RtbMode string

const (
	RtbModeOff    RtbMode = "off"
	RtbModeShadow RtbMode = "shadow"
	RtbModeLive   RtbMode = "live"
)

// ParseRtbMode normalizes RTB_MODE env values.
func ParseRtbMode(raw string) RtbMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "shadow":
		return RtbModeShadow
	case "live":
		return RtbModeLive
	default:
		return RtbModeOff
	}
}

// RtbEnabled reports whether the tracker should run in-process auctions at all.
func (c *Config) RtbEnabled() bool {
	return c != nil && ParseRtbMode(c.RtbMode) != RtbModeOff
}

// RtbLiveSelectsCampaign reports whether auction winners replace client campaign_id.
func (c *Config) RtbLiveSelectsCampaign() bool {
	return c != nil && ParseRtbMode(c.RtbMode) == RtbModeLive
}

// RtbBudgetAuthoritative reports whether rtb.CheckAndSpend owns budget debits (Lua budget skip).
func (c *Config) RtbBudgetAuthoritative() bool {
	if c == nil || !c.RtbLiveSelectsCampaign() {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.RtbBudgetAuthority), "rtb")
}

// RtbTargetingIndexEnabled reports whether geo+device+category inverted index is active (staging).
func (c *Config) RtbTargetingIndexEnabled() bool {
	return c != nil && c.RtbTargetingIndex
}
