package fraudscoring

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLGBMScorer(t *testing.T) {
	scorer, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		t.Fatalf("failed to create LGBMScorer: %v", err)
	}

	assert.Equal(t, "lightgbm", scorer.Name())
	assert.Equal(t, 7, scorer.Dims())

	rows := []FeatureRow{
		{
			WindowStart:      time.Now(),
			IPAddress:        "1.2.3.4",
			CampaignID:       "00000000-0000-0000-0000-000000000001",
			Events:           10,
			Clicks:           2, // Clicks < 5.0 -> left_child -> leaf_value = 0.1
			SpendMicro:       1000000,
			BudgetLimitMicro: 5000000,
			UniqueUsers:      1,
			UniqueUAs:        1,
		},
		{
			WindowStart:      time.Now(),
			IPAddress:        "5.6.7.8",
			CampaignID:       "00000000-0000-0000-0000-000000000002",
			Events:           100,
			Clicks:           10, // Clicks >= 5.0 -> right_child -> leaf_value = 0.9
			SpendMicro:       10000000,
			BudgetLimitMicro: 50000000,
			UniqueUsers:      5,
			UniqueUAs:        2,
		},
	}

	scores, err := scorer.ScoreBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("ScoreBatch failed: %v", err)
	}

	assert.Len(t, scores, 2)
	// LightGBM binary classification output is usually transformed using sigmoid.
	// Since go-lgbm applies the sigmoid transformation automatically unless raw predictions are requested:
	// sigmoid(0.1) = 1 / (1 + e^-0.1) = 0.52497
	// sigmoid(0.9) = 1 / (1 + e^-0.9) = 0.71094
	assert.InDelta(t, 0.52497, scores[0], 1e-4)
	assert.InDelta(t, 0.71094, scores[1], 1e-4)
}

func TestEnsembleScorer(t *testing.T) {
	scorer1, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		t.Fatalf("failed to create scorer1: %v", err)
	}

	scorer2, err := NewLGBMScorer("testdata/model.txt")
	if err != nil {
		t.Fatalf("failed to create scorer2: %v", err)
	}

	ensemble := NewEnsemble(scorer1, scorer2)
	assert.Equal(t, "ensemble", ensemble.Name())
	assert.Equal(t, 7, ensemble.Dims())

	rows := []FeatureRow{
		{
			WindowStart:      time.Now(),
			IPAddress:        "1.2.3.4",
			CampaignID:       "00000000-0000-0000-0000-000000000001",
			Events:           10,
			Clicks:           2,
			SpendMicro:       1000000,
			BudgetLimitMicro: 5000000,
			UniqueUsers:      1,
			UniqueUAs:        1,
		},
	}

	scores, err := ensemble.ScoreBatch(context.Background(), rows)
	if err != nil {
		t.Fatalf("ScoreBatch failed: %v", err)
	}

	assert.Len(t, scores, 1)
	assert.InDelta(t, 0.52497, scores[0], 1e-4)
}
