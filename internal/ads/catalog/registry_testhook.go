package catalog

import (
	"github.com/google/uuid"
)

// SetCampaignDaypartForTest overrides daypart hours on a registered campaign in tests.
func (r *Registry) SetCampaignDaypartForTest(campID uuid.UUID, hours map[int16]struct{}) {
	snap, _ := r.data.Load().(map[uuid.UUID]campaignInfo)
	info := snap[campID]
	info.campaign.DaypartHours = hours
	snap[campID] = info
	r.data.Store(snap)
}
