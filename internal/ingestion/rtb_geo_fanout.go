package ingestion

import (
	"sort"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"
)

// sortedTargetCountries returns campaign target countries in stable sorted order.
func sortedTargetCountries(camp *campaignmodel.Campaign) []string {
	if camp == nil || len(camp.TargetCountries) == 0 {
		return nil
	}
	out := make([]string, 0, len(camp.TargetCountries))
	for c := range camp.TargetCountries {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// fanOutRtbCatalogRows emits one catalog row per target country (R10 multi-country fan-out).
func fanOutRtbCatalogRows(camp *campaignmodel.Campaign, base RtbCampaignInput) []rtb.CampaignData {
	countries := sortedTargetCountries(camp)
	if len(countries) == 0 {
		return []rtb.CampaignData{CampaignDataFromDomain(camp, base)}
	}
	out := make([]rtb.CampaignData, 0, len(countries))
	for _, country := range countries {
		inp := base
		inp.GeoHash = GeoHashFromCountry(country)
		out = append(out, CampaignDataFromDomain(camp, inp))
	}
	return out
}
