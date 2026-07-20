package marginguard

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestEvaluate(t *testing.T) {
	policy := &Policy{
		ID:             uuid.New(),
		CampaignID:     uuid.New(),
		Name:           "Test Policy",
		MinClicks:      50,
		RoiFloorPct:    -30.0,
		ZeroConvStreak: 100,
		IsActive:       true,
	}

	tests := []struct {
		name     string
		stats    *PlacementStats
		expected bool
		action   Action
		reason   string
	}{
		{
			name: "Below min clicks",
			stats: &PlacementStats{
				Clicks:       49,
				SpendMicro:   1000000,
				RevenueMicro: 0,
			},
			expected: false,
		},
		{
			name: "Good ROI",
			stats: &PlacementStats{
				Clicks:       50,
				SpendMicro:   1000000,
				RevenueMicro: 800000, // ROI = -20%
			},
			expected: false,
		},
		{
			name: "Bad ROI -30%",
			stats: &PlacementStats{
				Clicks:       50,
				SpendMicro:   1000000,
				RevenueMicro: 700000, // ROI = -30%
			},
			expected: false, // ROI floor is < -30%, not <=
		},
		{
			name: "Bad ROI -31%",
			stats: &PlacementStats{
				Clicks:       50,
				SpendMicro:   1000000,
				RevenueMicro: 690000, // ROI = -31%
			},
			expected: true,
			action:   ActionPause,
			reason:   "ROI -31.00% below floor -30.00%",
		},
		{
			name: "Zero conv streak triggered",
			stats: &PlacementStats{
				Clicks:       100,
				SpendMicro:   1000000,
				RevenueMicro: 900000, // ROI = -10%, OK
				Conversions:  0,
			},
			expected: true,
			action:   ActionPause,
			reason:   "Zero conversions over 100 clicks",
		},
		{
			name: "Zero conv streak not triggered",
			stats: &PlacementStats{
				Clicks:       99,
				SpendMicro:   1000000,
				RevenueMicro: 900000,
				Conversions:  0,
			},
			expected: false,
		},
		{
			name: "Inactive policy",
			stats: &PlacementStats{
				Clicks:       1000,
				SpendMicro:   1000000,
				RevenueMicro: 0,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Inactive policy" {
				policy.IsActive = false
				defer func() { policy.IsActive = true }()
			}
			decision, triggered := Evaluate(policy, tt.stats)
			assert.Equal(t, tt.expected, triggered)
			if tt.expected {
				assert.Equal(t, tt.action, decision.Action)
				assert.Equal(t, tt.reason, decision.Reason)
				assert.Equal(t, policy.ID, decision.PolicyID)
				assert.Equal(t, policy.CampaignID, decision.CampaignID)
			}
		})
	}
}

func BenchmarkEvaluate(b *testing.B) {
	policy := &Policy{
		ID:             uuid.New(),
		CampaignID:     uuid.New(),
		MinClicks:      50,
		RoiFloorPct:    -30.0,
		ZeroConvStreak: 100,
		IsActive:       true,
	}
	stats := &PlacementStats{
		Clicks:       100,
		SpendMicro:   1_000_000,
		RevenueMicro: 690_000,
		Conversions:  0,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Evaluate(policy, stats)
	}
}
