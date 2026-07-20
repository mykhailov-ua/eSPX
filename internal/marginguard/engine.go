package marginguard

import (
	"fmt"

	"github.com/google/uuid"
)

type Policy struct {
	ID             uuid.UUID `json:"id"`
	CampaignID     uuid.UUID `json:"campaign_id"`
	Name           string    `json:"name"`
	MinClicks      int       `json:"min_clicks"`
	RoiFloorPct    float64   `json:"roi_floor_pct"`
	ZeroConvStreak int       `json:"zero_conv_streak"`
	IsActive       bool      `json:"is_active"`
}

type PlacementStats struct {
	CampaignID   uuid.UUID
	PlacementID  string
	SpendMicro   int64
	RevenueMicro int64
	Clicks       int64
	Conversions  int64
}

type Action string

const (
	ActionPause Action = "pause"
	ActionAlert Action = "alert"
)

type Decision struct {
	PolicyID    uuid.UUID
	CampaignID  uuid.UUID
	PlacementID string
	Action      Action
	Reason      string
	Metrics     map[string]any
}

func Evaluate(policy *Policy, stats *PlacementStats) (*Decision, bool) {
	if !policy.IsActive {
		return nil, false
	}

	if stats.Clicks < int64(policy.MinClicks) {
		return nil, false
	}

	roi := 0.0
	if stats.SpendMicro > 0 {
		profit := stats.RevenueMicro - stats.SpendMicro
		roi = float64(profit) / float64(stats.SpendMicro) * 100
	} else if stats.RevenueMicro > 0 {
		roi = 100.0 // Infinite ROI if revenue exists but no spend
	}

	metrics := map[string]any{
		"spend_micro":   stats.SpendMicro,
		"revenue_micro": stats.RevenueMicro,
		"clicks":        stats.Clicks,
		"conversions":   stats.Conversions,
		"roi_pct":       roi,
	}

	// Rule 1: ROI floor
	if roi < policy.RoiFloorPct {
		return &Decision{
			PolicyID:    policy.ID,
			CampaignID:  policy.CampaignID,
			PlacementID: stats.PlacementID,
			Action:      ActionPause,
			Reason:      fmt.Sprintf("ROI %.2f%% below floor %.2f%%", roi, policy.RoiFloorPct),
			Metrics:     metrics,
		}, true
	}

	// Rule 2: Zero conversion streak
	if stats.Conversions == 0 && stats.Clicks >= int64(policy.ZeroConvStreak) {
		return &Decision{
			PolicyID:    policy.ID,
			CampaignID:  policy.CampaignID,
			PlacementID: stats.PlacementID,
			Action:      ActionPause,
			Reason:      fmt.Sprintf("Zero conversions over %d clicks", stats.Clicks),
			Metrics:     metrics,
		}, true
	}

	return nil, false
}
