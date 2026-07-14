package mlanalytics

import (
	"context"
	"log/slog"
)

// Fraud tier boundaries align with edge MapFraudRLTier (30/60/80).
const (
	FraudTierPassMax    = 30
	FraudTierSuspectMax = 60
	FraudTierIVTMax     = 80
)

// FraudTier is the enforcement band derived from a clamped fraud score.
type FraudTier string

const (
	FraudTierPass    FraudTier = "pass"
	FraudTierSuspect FraudTier = "suspect"
	FraudTierIVT     FraudTier = "ivt"
	FraudTierBlock   FraudTier = "block"
)

// ClampFraudScore bounds a fraud score to [0, 100].
func ClampFraudScore(score int) int {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

// ProbabilityToFraudScore maps a model probability in [0, 1] to an integer fraud score.
func ProbabilityToFraudScore(probability float64) int {
	if probability < 0 {
		probability = 0
	}
	if probability > 1 {
		probability = 1
	}
	return ClampFraudScore(int(probability*100 + 0.5))
}

// MapFraudScoreTier maps a clamped fraud score to a tier.
func MapFraudScoreTier(score int) (FraudTier, int) {
	score = ClampFraudScore(score)
	switch {
	case score <= FraudTierPassMax:
		return FraudTierPass, score
	case score <= FraudTierSuspectMax:
		return FraudTierSuspect, score
	case score <= FraudTierIVTMax:
		return FraudTierIVT, score
	default:
		return FraudTierBlock, score
	}
}

// MapProbabilityTier maps a model probability to a tier and fraud score.
func MapProbabilityTier(probability float64) (FraudTier, int) {
	return MapFraudScoreTier(ProbabilityToFraudScore(probability))
}

// Ensemble combines multiple scorers and averages their predictions.
type Ensemble struct {
	scorers []Scorer
}

// NewEnsemble creates a new Ensemble with the given scorers.
func NewEnsemble(scorers ...Scorer) *Ensemble {
	return &Ensemble{scorers: scorers}
}

// Name returns the name of the ensemble.
func (ensemble *Ensemble) Name() string {
	return "ensemble"
}

// Dims returns the dimensions of the first scorer (or 0 if empty).
func (ensemble *Ensemble) Dims() int {
	if len(ensemble.scorers) == 0 {
		return 0
	}
	return ensemble.scorers[0].Dims()
}

// ScoreBatch scores a batch of FeatureRow by averaging the scores of all scorers.
func (ensemble *Ensemble) ScoreBatch(ctx context.Context, rows []FeatureRow) ([]float64, error) {
	if len(ensemble.scorers) == 0 {
		return make([]float64, len(rows)), nil
	}

	// Score with the first scorer
	scores, err := ensemble.scorers[0].ScoreBatch(ctx, rows)
	if err != nil {
		slog.Error("ensemble scorer failed", "scorer", ensemble.scorers[0].Name(), "error", err)
		return nil, err
	}

	// Average with subsequent scorers
	for i := 1; i < len(ensemble.scorers); i++ {
		scorerScores, err := ensemble.scorers[i].ScoreBatch(ctx, rows)
		if err != nil {
			slog.Error("ensemble scorer failed", "scorer", ensemble.scorers[i].Name(), "error", err)
			return nil, err
		}
		for j := range scores {
			scores[j] += scorerScores[j]
		}
	}

	// Divide by the number of scorers to get the average
	n := float64(len(ensemble.scorers))
	for j := range scores {
		scores[j] /= n
	}

	return scores, nil
}
