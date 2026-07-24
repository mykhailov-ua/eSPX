package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunOpenRTBBid_integration(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 50_000_000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 2_000_000, DeviceMask: 7, CategoryMask: 3, GeoHash: geo, Weight: 1},
		},
	)
	body := []byte(`{"id":"b1","tmax":300,"imp":[{"id":"1","bidfloor":0.5}],"device":{"devicetype":2},"site":{"cat":["IAB1"]}}`)
	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeLive,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	out := runOpenRTBBid(proc, body, []byte("b1"), "8.8.8.8")
	require.True(t, out.HasBid, "reason=%v", out.NoBid)
	assert.Equal(t, winnerID, out.CampaignID)
	assert.Greater(t, out.PriceMicro, int64(0))
}

func TestOpenRTBBid_gnetHandler(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 50_000_000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 2_000_000, DeviceMask: 7, CategoryMask: 3, GeoHash: geo, Weight: 1},
		},
	)
	body := `{"id":"b1","tmax":300,"imp":[{"id":"1","bidfloor":0.5}],"device":{"devicetype":2}}`
	wire := BuildGnetHTTP("POST", "/openrtb/bid", map[string]string{
		"Content-Type":   "application/json",
		"Content-Length": itoa(len(body)),
	}, []byte(body))
	cfg := &config.Config{MaxRequestBodySize: 1 << 20}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.trackProc.rtbCatalog = catalog
	h.trackProc.rtbMode = rtbModeLive
	h.trackProc.ingestGeo = &staticGeoProvider{country: "US"}
	_, conn := ServeGnetHarness(h, wire)
	resp := conn.Written()
	assert.Contains(t, string(resp), "200 OK")
	assert.Contains(t, string(resp), "seatbid")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
