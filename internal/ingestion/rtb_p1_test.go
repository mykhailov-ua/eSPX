package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanOutRtbCatalogRows_multiCountry(t *testing.T) {
	camp := &campaignmodel.Campaign{
		TargetCountries: map[string]struct{}{"US": {}, "DE": {}, "FR": {}},
		BudgetLimit:     1_000_000,
	}
	base := RtbCampaignInput{BidMicro: 100, CTRPPM: CTRPPMUnit, Weight: 1, BoostPPM: CTRPPMUnit}
	rows := fanOutRtbCatalogRows(camp, base)
	require.Len(t, rows, 3)
	geos := make(map[uint32]struct{}, 3)
	for _, row := range rows {
		geos[row.GeoHashVal] = struct{}{}
	}
	assert.Len(t, geos, 3)
	assert.Equal(t, GeoHashFromCountry("DE"), rows[0].GeoHashVal)
	assert.Equal(t, GeoHashFromCountry("FR"), rows[1].GeoHashVal)
	assert.Equal(t, GeoHashFromCountry("US"), rows[2].GeoHashVal)
}

func TestBoostPPMFromUint8(t *testing.T) {
	assert.Equal(t, uint32(CTRPPMUnit), BoostPPMFromUint8(0))
	assert.Equal(t, uint32(CTRPPMUnit+500_000), BoostPPMFromUint8(50))
}

func TestValidateSchainNodes_allowlist(t *testing.T) {
	allow := BuildSupplyChainAllowlistFromSellers(
		[]string{"example.com"},
		[]string{"pub123"},
	)
	var nodes SchainNodes
	nodes.Count = 1
	copy(nodes.Nodes[0].ASI[:], "example.com")
	nodes.Nodes[0].ASILen = 11
	copy(nodes.Nodes[0].SID[:], "pub123")
	nodes.Nodes[0].SIDLen = 6
	assert.True(t, ValidateSchainNodes(nodes, allow))

	copy(nodes.Nodes[0].SID[:], "bad")
	nodes.Nodes[0].SIDLen = 3
	assert.False(t, ValidateSchainNodes(nodes, allow))
}

func TestEffectiveScoreWithBoost_ranking(t *testing.T) {
	base := int64(1_000_000)
	boosted := effectiveScoreWithBoost(1_000_000, rtb.CTRPPMUnit, BoostPPMFromUint8(10))
	assert.Greater(t, boosted, base)
}

func effectiveScoreWithBoost(bid int64, ctrPPM, boostPPM uint32) int64 {
	return bid * int64(ctrPPM) * int64(boostPPM) / int64(CTRPPMUnit) / int64(CTRPPMUnit)
}
