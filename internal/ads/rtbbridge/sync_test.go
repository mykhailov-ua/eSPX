package rtbbridge

import (
	"testing"

	"espx/internal/ads/catalog"
	"espx/internal/config"
	"espx/internal/domain"

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
	registry := catalog.NewRegistry(nil)
	registry.Add(campA.ID, customerID, nil, "", domain.PacingModeAsap, campA.BudgetLimit, "UTC", 0, 0, nil)
	registry.Add(campB.ID, customerID, nil, "", domain.PacingModeAsap, campB.BudgetLimit, "UTC", 0, 0, nil)

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
