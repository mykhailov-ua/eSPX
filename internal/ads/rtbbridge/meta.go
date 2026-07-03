package rtbbridge

import "github.com/google/uuid"

// CampaignMeta carries auction inputs for weighted campaign selection and sharding.
type CampaignMeta struct {
	ID                uuid.UUID
	BidMicro          int64
	CTR               float64
	RemainingBudget   int64
	TotalBudget       int64
	PeakTrafficFactor float64
}
