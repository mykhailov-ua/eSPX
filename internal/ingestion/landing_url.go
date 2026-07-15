package ingestion

import (
	"espx/internal/campaignmodel"
)

// ResolveLandingURL picks a brand creative URL for accepted click responses.
func ResolveLandingURL(registry campaignmodel.CampaignRegistry, store *BrandCreativeStore, evt *campaignmodel.Event) string {
	if store == nil || registry == nil || evt.Type != "click" {
		return ""
	}
	camp, ok := registry.GetCampaign(evt.CampaignID)
	if !ok || camp.BrandID == nil {
		return ""
	}
	return store.SelectLandingURL(*camp.BrandID, evt.UserID)
}
