package ads

import (
	"context"
	"testing"

	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/rtb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRtbInputsFromRegistry_customerPoolAndHybridBid(t *testing.T) {
	cfg := &config.Config{ClickAmount: 50}
	customerID := uuid.New()
	campA := &domain.Campaign{
		ID: uuid.New(), CustomerID: customerID, Status: domain.CampaignStatusActive,
		BudgetLimit: 1000, CurrentSpend: 200,
		TargetCountries: map[string]struct{}{"US": {}},
	}
	campB := &domain.Campaign{
		ID: uuid.New(), CustomerID: customerID, Status: domain.CampaignStatusActive,
		BudgetLimit: 500, CurrentSpend: 100,
		TargetCountries: map[string]struct{}{"US": {}},
	}
	registry := &Registry{}
	registry.data.Store(&campaignMapSnapshot{byID: map[uuid.UUID]campaignInfo{
		campA.ID: {campaign: campA, status: db.CampaignStatusTypeACTIVE},
		campB.ID: {campaign: campB, status: db.CampaignStatusTypeACTIVE},
	}})

	metaByID := map[uuid.UUID]*CampaignMeta{
		campA.ID: {ID: campA.ID, BidMicro: 300, CTR: 0.1, RemainingBudget: 800, TotalBudget: 1000},
	}
	pools := buildCustomerBudgetPools([]*domain.Campaign{campA, campB})
	assert.Equal(t, int64(1200), pools[customerID])

	inputs := BuildRtbInputsFromRegistry(registry, cfg, metaByID, pools)
	require.Contains(t, inputs, campA.ID)
	assert.Equal(t, int64(300), inputs[campA.ID].BidMicro)
	assert.Equal(t, uint32(100_000), inputs[campA.ID].CTRPPM)
	assert.Equal(t, int64(1200), inputs[campA.ID].CustomerBudget)
}

func TestSyncRtbCatalog_hybridOverridesBid(t *testing.T) {
	cfg := &config.Config{ClickAmount: 100}
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)
	hybrid := NewHybridBalancer(6, 1000)

	id := uuid.New()
	camp := &domain.Campaign{
		ID: id, Status: domain.CampaignStatusActive,
		BudgetLimit: 5000, TargetCountries: map[string]struct{}{"US": {}},
	}
	registry := &Registry{}
	registry.data.Store(&campaignMapSnapshot{byID: map[uuid.UUID]campaignInfo{
		id: {campaign: camp, status: db.CampaignStatusTypeACTIVE},
	}})

	SyncRtbCatalog(context.Background(), registry, catalog, cfg, hybrid, RtbBudgetSync{})

	geo := GeoHashFromCountry("US")
	res, reason := catalog.RunAuction(&domain.Event{}, RtbTargetingInput{
		GeoHash: geo, DeviceType: 1, CategoryMask: 1, PublisherFloorMicro: 50,
	})
	require.True(t, reason.OK())
	assert.Equal(t, CampaignIDFromUUID(id), res.CampaignID)
}
