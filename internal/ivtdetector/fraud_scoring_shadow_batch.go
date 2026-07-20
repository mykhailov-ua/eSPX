package ivtdetector

import (
	"context"
	"fmt"
	"time"

	"espx/internal/fraudscoring"
)

func (r *fraudScoringRule) insertShadowScores(ctx context.Context, featureRows []fraudscoring.FeatureRow, scores []float64) error {
	if r.writeConn == nil || len(scores) == 0 {
		return nil
	}
	if len(scores) != len(featureRows) {
		return fmt.Errorf("shadow score count mismatch: %d scores, %d rows", len(scores), len(featureRows))
	}

	batch, err := r.writeConn.PrepareBatch(ctx, `
INSERT INTO ml_shadow_scores (ip_address, score, model_name, created_at)`)
	if err != nil {
		return fmt.Errorf("prepare ml_shadow_scores batch: %w", err)
	}

	modelName := r.scorer.Name()
	now := time.Now().UTC()
	for i, score := range scores {
		if err := batch.Append(featureRows[i].IPAddress, score, modelName, now); err != nil {
			return fmt.Errorf("append ml_shadow_scores row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send ml_shadow_scores batch: %w", err)
	}
	return nil
}
