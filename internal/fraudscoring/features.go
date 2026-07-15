package fraudscoring

import "time"

// FeatureRow represents a single row of features fetched from ClickHouse.
type FeatureRow struct {
	WindowStart      time.Time
	IPAddress        string
	CampaignID       string
	Events           uint64
	Clicks           uint64
	SpendMicro       int64
	BudgetLimitMicro int64
	UniqueUsers      uint64
	UniqueUAs        uint64
}

// ToVector converts the FeatureRow into a float64 slice (feature vector) for the model.
// We use float64 because go-lgbm expects float64 slice for PredictDense.
func (featureRow *FeatureRow) ToVector() []float64 {
	ctr := 0.0
	if featureRow.Events > 0 {
		ctr = float64(featureRow.Clicks) / float64(featureRow.Events)
	}
	spendNorm := float64(featureRow.SpendMicro) / 1e6
	spendRatio := 0.0
	if featureRow.BudgetLimitMicro > 0 {
		spendRatio = float64(featureRow.SpendMicro) / float64(featureRow.BudgetLimitMicro)
	}
	return []float64{
		float64(featureRow.Events),
		float64(featureRow.Clicks),
		ctr,
		spendNorm,
		spendRatio,
		float64(featureRow.UniqueUsers),
		float64(featureRow.UniqueUAs),
	}
}
