package rtbbridge

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads/sharding"
	adstest "espx/internal/ads/testutil"
	"espx/internal/domain"
	"espx/internal/rtb"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncRTBBudgetState_fromRegistry(t *testing.T) {
	store := rtb.NewBudgetStore()
	customerID := uuid.New()
	camp := &domain.Campaign{
		ID: uuid.New(), CustomerID: customerID,
		BudgetLimit: 1000, CurrentSpend: 250,
	}
	pools := map[uuid.UUID]int64{customerID: 750}
	SyncRTBBudgetState(context.Background(), store, []*domain.Campaign{camp}, pools, RtbBudgetSync{})

	assert.Equal(t, int64(750), store.GetBudget(CampaignIDFromUUID(camp.ID)))
	slot, ok := store.CustomerSlot(CustomerIDFromCustomerUUID(customerID))
	require.True(t, ok)
	assert.Equal(t, int64(750), store.LoadCustomerBudget(slot))
}

func TestSyncRTBBudgetState_fromRedis(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &adstest.MockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, int64(4_200_000), 0).Err())

	store := rtb.NewBudgetStore()
	campCopy := *camp
	campCopy.ID = campID
	campCopy.BudgetLimit = 9_000_000
	campCopy.CurrentSpend = 0

	SyncRTBBudgetState(ctx, store, []*domain.Campaign{&campCopy}, nil, RtbBudgetSync{
		Authority: BudgetAuthorityRTB,
		Redis:     []redis.UniversalClient{rdb},
		Sharder:   sharding.NewJumpHashSharder(1),
	})

	assert.Equal(t, int64(4_200_000), store.GetBudget(CampaignIDFromUUID(campID)))
}

func TestLoadRedisDailySpend(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := adstest.SetupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &adstest.MockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	campCopy := *camp
	campCopy.ID = campID

	keyBuf := append([]byte(nil), campCopy.DailySpendKeyPrefix...)
	keyBuf = append(keyBuf, time.Now().UTC().Format("2006-01-02")...)
	require.NoError(t, rdb.Set(ctx, string(keyBuf), int64(500_000), 0).Err())

	spent, ok := loadRedisDailySpend(ctx, []redis.UniversalClient{rdb}, sharding.NewJumpHashSharder(1), &campCopy)
	require.True(t, ok)
	assert.Equal(t, int64(500_000), spent)
}
