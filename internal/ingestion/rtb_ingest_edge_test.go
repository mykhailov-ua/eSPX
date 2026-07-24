package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenRTBIngest_TruncatedPayload does not panic on truncated OpenRTB JSON (M6-20).
func TestOpenRTBIngest_TruncatedPayload(t *testing.T) {
	payload := []byte(`{"openrtb":{"ver":"3.0","item":[{"id":"1","flr":`)
	_, _, _, ok := ParseOpenRTB3Payload(payload)
	assert.True(t, ok)
}

// TestOpenRTBIngest_MultiImp uses the first flr field in the payload (M6-20).
func TestOpenRTBIngest_MultiImp(t *testing.T) {
	payload := []byte(`{"openrtb":{"ver":"3.0","item":[{"id":"a","flr":1.5},{"id":"b","flr":9.9}]}}`)
	minBid, deviceType, categoryMask, ok := ParseOpenRTB3Payload(payload)
	require.True(t, ok)
	assert.Equal(t, int64(1500000), minBid)
	assert.Equal(t, uint8(1), deviceType)
	assert.Equal(t, uint64(1), categoryMask)
}

// TestOpenRTBIngest_LiveWithOpenRTBPayload runs live RTB when track payload embeds OpenRTB (M6-20).
func TestOpenRTBIngest_LiveWithOpenRTBPayload(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)
	winnerID := uuid.New()
	geo := GeoHashFromCountry("US")
	catalog.SyncActiveCampaigns(
		[]*campaignmodel.Campaign{{ID: winnerID, BudgetLimit: 10_000_000_000, TargetCountries: map[string]struct{}{"US": {}}}},
		map[uuid.UUID]RtbCampaignInput{
			winnerID: {BidMicro: 2_000_000, DeviceMask: 1, CategoryMask: 1, GeoHash: geo, Weight: 1},
		},
	)

	proc := trackProcessor{
		rtbCatalog: catalog,
		rtbMode:    rtbModeLive,
		ingestGeo:  &staticGeoProvider{country: "US"},
	}
	clientID := uuid.New()
	evt := &campaignmodel.Event{
		CampaignID: clientID,
		IP:         "8.8.8.8",
		Payload:    []byte(`{"openrtb":{"ver":"3.0","item":[{"id":"1","flr":1.0}]}}`),
	}
	ensureIngestGeo(proc.ingestGeo, evt)
	out, handled := applyRtbAuction(proc, evt, []byte("desktop"))
	assert.False(t, handled)
	assert.Equal(t, winnerID, evt.CampaignID)
	targeting := buildRtbTargeting(evt, []byte("desktop"), 0, proc.rtbCatalog)
	assert.Equal(t, int64(1000000), targeting.PublisherFloorMicro)
	assert.Equal(t, trackOutcome{}, out)
}
