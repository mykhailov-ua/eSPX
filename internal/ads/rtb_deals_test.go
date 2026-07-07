package ads

import (
	"context"
	"testing"

	"espx/internal/ads/db"
	"espx/internal/database"
	"espx/internal/rtb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReloadRtbDeals_buildsDealIndex(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO customers (id, name) VALUES ($1, 'deal-test')`, customerID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO rtb_deals (deal_id, floor_micro, geo_mask, cat_mask, pacing, customer_id)
		VALUES ('pmp-001', 500000, 15, 7, 1, $1)`, customerID)
	require.NoError(t, err)

	catalog := NewRtbCatalog(rtb.NewBudgetStore(), BudgetAuthorityShadow)
	require.NoError(t, ReloadRtbDeals(ctx, db.New(pool), catalog))
	assert.Equal(t, 1, catalog.DealCount())

	deal, ok := catalog.LookupDeal("pmp-001")
	require.True(t, ok)
	assert.Equal(t, int64(500000), deal.FloorMicro)
	assert.Equal(t, uint64(15), deal.GeoMask)
	assert.Equal(t, rtb.PacingOpen, deal.PacingOpen)
}

func TestRtbCatalogReloadChannel_default(t *testing.T) {
	assert.Equal(t, "rtb:catalog:reload", RtbCatalogReloadChannel(nil))
}
