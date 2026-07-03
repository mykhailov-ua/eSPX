package ads

import (
	"testing"

	"espx/internal/domain"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCampaignIDFromUUID_stablePrefix(t *testing.T) {
	id := uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef")
	got := CampaignIDFromUUID(id)
	assert.Equal(t, rtb.CampaignID(0x0123456789abcdef), got)
}

func TestGeoHashFromCountry(t *testing.T) {
	ua := GeoHashFromCountry("UA")
	us := GeoHashFromCountry("US")
	assert.NotZero(t, ua)
	assert.NotZero(t, us)
	assert.NotEqual(t, ua, us)
}

func TestDeviceMaskFromType(t *testing.T) {
	assert.Equal(t, uint8(2), DeviceMaskFromType([]byte("mobile")))
	assert.Equal(t, uint8(4), DeviceMaskFromType([]byte("tablet")))
	assert.Equal(t, uint8(1), DeviceMaskFromType([]byte("desktop")))
}

func TestBuildRtbCatalogRows_remainingBudget(t *testing.T) {
	id := uuid.New()
	camp := &domain.Campaign{
		ID:           id,
		BudgetLimit:  1000,
		CurrentSpend: 250,
	}
	rows := BuildRtbCatalogRows([]*domain.Campaign{camp}, map[uuid.UUID]RtbCampaignInput{
		id: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: 7, Weight: 1},
	})
	require.Len(t, rows, 1)
	assert.Equal(t, int64(750), rows[0].Budget)
	assert.Equal(t, CampaignIDFromUUID(id), rows[0].ID)
}

func TestRtbCatalog_shadowDoesNotSpend(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)

	id := uuid.New()
	camp := &domain.Campaign{ID: id, BudgetLimit: 1000, CurrentSpend: 0}
	inputs := map[uuid.UUID]RtbCampaignInput{
		id: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: 7, Weight: 1},
	}
	catalog.SyncActiveCampaigns([]*domain.Campaign{camp}, inputs)

	evt := &domain.Event{}
	res, reason := catalog.RunAuction(evt, RtbTargetingInput{
		GeoHash:             7,
		DeviceType:          1,
		CategoryMask:        1,
		PublisherFloorMicro: 50,
	})
	require.True(t, reason.OK())
	assert.Equal(t, CampaignIDFromUUID(id), res.CampaignID)
	assert.Equal(t, int64(1000), store.GetBudget(CampaignIDFromUUID(id)))
}

func TestRtbCatalog_liveSpend(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityRTB)

	id := uuid.New()
	camp := &domain.Campaign{ID: id, BudgetLimit: 1000, CurrentSpend: 0}
	inputs := map[uuid.UUID]RtbCampaignInput{
		id: {BidMicro: 100, DeviceMask: 1, CategoryMask: 1, GeoHash: 7, Weight: 1},
	}
	catalog.SyncActiveCampaigns([]*domain.Campaign{camp}, inputs)

	evt := &domain.Event{}
	_, reason := catalog.RunAuction(evt, RtbTargetingInput{
		GeoHash:             7,
		DeviceType:          1,
		CategoryMask:        1,
		PublisherFloorMicro: 50,
	})
	require.True(t, reason.OK())
	assert.Equal(t, int64(950), store.GetBudget(CampaignIDFromUUID(id)))
}
