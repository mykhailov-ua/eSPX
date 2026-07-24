package ingestion

import (
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRtbSync_reserveMicro(t *testing.T) {
	camp := &campaignmodel.Campaign{
		ID:           uuid.New(),
		BudgetLimit:  10_000,
		ReserveMicro: 50_000,
		Status:       campaignmodel.CampaignStatusActive,
	}
	cfg := &config.Config{ClickAmount: 100}
	input := rtbInputForCampaign(camp, cfg, nil, 0, nil, nil)
	assert.Equal(t, int64(50_000), input.ReserveMicro)
}

func TestEnrichTargetingDeal_pmp(t *testing.T) {
	store := rtb.NewBudgetStore()
	catalog := NewRtbCatalog(store, BudgetAuthorityShadow)
	catalog.UpdateDeals([]rtb.DealData{{
		DealID:     "deal-x",
		FloorMicro: 100,
		GeoMask:    rtb.GeoBitFromHash(GeoHashFromCountry("US")),
		CatMask:    1,
		PacingOpen: rtb.PacingOpen,
		Seats:      2,
	}})
	targeting := catalog.enrichTargetingDeal(RtbTargetingInput{
		DealID:       "deal-x",
		GeoHash:      GeoHashFromCountry("US"),
		CategoryMask: 1,
		SeatCount:    2,
	})
	assert.Equal(t, rtb.NoBidNone, targeting.DealBlock)
}
