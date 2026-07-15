package ingestion

import (
	"context"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/rtb"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// RtbBudgetSync carries cold-path Redis readers for RTB budget mirror updates.
type RtbBudgetSync struct {
	Authority BudgetAuthority
	Redis     []redis.UniversalClient
	Sharder   Sharder
}

// SyncRTBBudgetState mirrors registry and optional Redis budget keys into the rtb BudgetStore.
func SyncRTBBudgetState(
	ctx context.Context,
	store *rtb.BudgetStore,
	campaigns []*campaignmodel.Campaign,
	customerPools map[uuid.UUID]int64,
	sync RtbBudgetSync,
) {
	if store == nil || len(campaigns) == 0 {
		return
	}
	readRedis := sync.Authority == BudgetAuthorityRTB && len(sync.Redis) > 0 && sync.Sharder != nil

	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		campID := CampaignIDFromUUID(camp.ID)
		remaining := RemainingBudgetMicro(camp)
		if readRedis && camp.BudgetCampaignKey != "" {
			if redisRem, ok := loadRedisCampaignBudget(ctx, sync.Redis, sync.Sharder, camp); ok {
				remaining = redisRem
			}
		}
		store.SetBudget(campID, remaining)

		if readRedis && camp.DailySpendKeyPrefix != "" {
			if spent, ok := loadRedisDailySpend(ctx, sync.Redis, sync.Sharder, camp); ok {
				if idx, exists := store.CampaignSlot(campID); exists {
					store.SetDailySpend(idx, spent)
				}
			}
		}
	}

	for customerID, pool := range customerPools {
		if pool < 0 {
			pool = 0
		}
		store.SetCustomerBudget(CustomerIDFromCustomerUUID(customerID), pool)
	}
}

func loadRedisCampaignBudget(
	ctx context.Context,
	rdbs []redis.UniversalClient,
	sharder Sharder,
	camp *campaignmodel.Campaign,
) (int64, bool) {
	shard := sharder.GetShard(camp.ID)
	if shard < 0 || shard >= len(rdbs) {
		return 0, false
	}
	val, err := rdbs[shard].Get(ctx, camp.BudgetCampaignKey).Int64()
	if err != nil {
		return 0, false
	}
	if val < 0 {
		val = 0
	}
	return val, true
}

func loadRedisDailySpend(
	ctx context.Context,
	rdbs []redis.UniversalClient,
	sharder Sharder,
	camp *campaignmodel.Campaign,
) (int64, bool) {
	shard := sharder.GetShard(camp.ID)
	if shard < 0 || shard >= len(rdbs) {
		return 0, false
	}
	loc := camp.Location
	if loc == nil {
		loc = time.UTC
	}
	keyBuf := make([]byte, 0, len(camp.DailySpendKeyPrefix)+8)
	keyBuf = append(keyBuf, camp.DailySpendKeyPrefix...)
	keyBuf = appendDate(keyBuf, time.Now().In(loc))
	key := string(keyBuf)

	val, err := rdbs[shard].Get(ctx, key).Int64()
	if err != nil {
		return 0, false
	}
	if val < 0 {
		val = 0
	}
	return val, true
}
