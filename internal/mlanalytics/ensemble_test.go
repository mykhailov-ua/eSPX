package mlanalytics

import (
	"testing"

	"espx/internal/edge/perimeter"

	"github.com/stretchr/testify/assert"
)

func TestClampFraudScore(t *testing.T) {
	tests := []struct {
		name  string
		score int
		want  int
	}{
		{name: "negative", score: -5, want: 0},
		{name: "zero", score: 0, want: 0},
		{name: "mid", score: 45, want: 45},
		{name: "max", score: 100, want: 100},
		{name: "overflow", score: 150, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ClampFraudScore(tt.score))
		})
	}
}

func TestMapFraudScoreTier_matchesMapFraudRLTier(t *testing.T) {
	tests := []struct {
		name     string
		score    int
		wantTier FraudTier
	}{
		{name: "pass_low", score: 0, wantTier: FraudTierPass},
		{name: "pass_boundary", score: 30, wantTier: FraudTierPass},
		{name: "suspect_low", score: 31, wantTier: FraudTierSuspect},
		{name: "suspect_boundary", score: 60, wantTier: FraudTierSuspect},
		{name: "ivt_low", score: 61, wantTier: FraudTierIVT},
		{name: "ivt_boundary", score: 80, wantTier: FraudTierIVT},
		{name: "block_low", score: 81, wantTier: FraudTierBlock},
		{name: "block_high", score: 100, wantTier: FraudTierBlock},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTier, gotScore := MapFraudScoreTier(tt.score)
			edgeTier, edgeScore := perimeter.MapFraudRLTier(tt.score)

			assert.Equal(t, tt.wantTier, gotTier)
			assert.Equal(t, edgeTier, perimeter.FraudRLTier(gotTier))
			assert.Equal(t, edgeScore, gotScore)
		})
	}
}

func TestMapProbabilityTier_boundaries(t *testing.T) {
	tests := []struct {
		name        string
		probability float64
		wantTier    FraudTier
		wantScore   int
	}{
		{name: "pass", probability: 0.10, wantTier: FraudTierPass, wantScore: 10},
		{name: "pass_boundary", probability: 0.30, wantTier: FraudTierPass, wantScore: 30},
		{name: "suspect", probability: 0.45, wantTier: FraudTierSuspect, wantScore: 45},
		{name: "suspect_boundary", probability: 0.60, wantTier: FraudTierSuspect, wantScore: 60},
		{name: "ivt", probability: 0.70, wantTier: FraudTierIVT, wantScore: 70},
		{name: "ivt_boundary", probability: 0.80, wantTier: FraudTierIVT, wantScore: 80},
		{name: "block", probability: 0.90, wantTier: FraudTierBlock, wantScore: 90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTier, gotScore := MapProbabilityTier(tt.probability)
			edgeTier, edgeScore := perimeter.MapFraudRLTier(tt.wantScore)

			assert.Equal(t, tt.wantTier, gotTier)
			assert.Equal(t, tt.wantScore, gotScore)
			assert.Equal(t, edgeTier, perimeter.FraudRLTier(gotTier))
			assert.Equal(t, edgeScore, gotScore)
		})
	}
}

func TestEnsemble_averageWeights(t *testing.T) {
	left, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		t.Fatalf("load left scorer: %v", err)
	}
	right, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		t.Fatalf("load right scorer: %v", err)
	}

	ensemble := NewEnsemble(left, right)
	rows := []FeatureRow{{
		Events: 100,
		Clicks: 10,
	}}

	scores, err := ensemble.ScoreBatch(t.Context(), rows)
	if err != nil {
		t.Fatalf("ensemble score: %v", err)
	}

	single, err := left.ScoreBatch(t.Context(), rows)
	if err != nil {
		t.Fatalf("single score: %v", err)
	}

	assert.Len(t, scores, 1)
	assert.InDelta(t, single[0], scores[0], 1e-9)
}
