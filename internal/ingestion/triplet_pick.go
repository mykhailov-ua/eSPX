package ingestion

import (
	"github.com/google/uuid"
)

// CampaignTripletPick mirrors hot-path A/B/R selection for chaos tests (M2).
type CampaignTripletPick struct {
	PrimaryA int16
	PrimaryB int16
	Reserve  int16
}

// PickShard returns primary A, B, or reserve using the same 40/40/20 split as unified-filter.
func (c *CampaignTripletPick) PickShard(campaignID, userID string) int {
	if c == nil {
		return 0
	}
	id, err := uuid.Parse(campaignID)
	if err != nil {
		return int(c.PrimaryA)
	}
	hash := ComputeCompositeHashUUID(id, []byte(userID))
	pct := hash % 100
	if pct < 40 {
		return int(c.PrimaryA)
	}
	if pct < 80 {
		return int(c.PrimaryB)
	}
	return int(c.Reserve)
}
