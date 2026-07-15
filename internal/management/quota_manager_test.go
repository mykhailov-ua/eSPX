package management

import (
	"context"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestQuotaManager_refillCampaign_modes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mode           string
		wantRedisQuota int64
		wantPGReserved int64
	}{
		{
			name:           "live credits Redis and Postgres",
			mode:           "live",
			wantRedisQuota: quotaChaosChunkMicro,
			wantPGReserved: quotaChaosChunkMicro,
		},
		{
			name:           "shadow reserves Postgres only",
			mode:           "shadow",
			wantRedisQuota: 0,
			wantPGReserved: quotaChaosChunkMicro,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if testing.Short() {
				t.Skip("integration test")
			}

			ctx := context.Background()
			pool, cleanupDB := database.SetupTestDB(t)
			defer cleanupDB()
			rdb, cleanupRedis := database.SetupTestRedis(t)
			defer cleanupRedis()

			customerID := uuid.New()
			campaignID := uuid.New()
			_, err := pool.Exec(ctx, `
				INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'quota-mode', 0, 'USD')`,
				ingestion.ToUUID(customerID))
			require.NoError(t, err)
			seedQuotaChaosCampaign(t, pool, campaignID, customerID, 10_000_000)

			cfg := &config.Config{
				QuotaMode:               tc.mode,
				QuotaChunkSize:          quotaChaosChunkMicro,
				QuotaRefillThresholdPct: 20,
			}
			svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
			qm := NewQuotaManager(svc)

			lockKey := "budget:refill_lock:" + campaignID.String()
			require.NoError(t, rdb.Set(ctx, lockKey, "1", 10*time.Second).Err())
			require.NoError(t, qm.refillCampaign(ctx, campaignID, 0, rdb))

			quotaRepo := ingestion.NewQuotaRepo(pool)
			pgQuota, err := quotaRepo.GetQuota(ctx, svc.sharder, campaignID)
			require.NoError(t, err)
			require.Equal(t, tc.wantPGReserved, pgQuota.ReservedAmount)

			redisQuota, err := rdb.Get(ctx, "budget:quota:"+campaignID.String()).Int64()
			if tc.wantRedisQuota == 0 {
				require.Error(t, err)
				require.ErrorIs(t, err, redis.Nil)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantRedisQuota, redisQuota)
			}
		})
	}
}

func TestNewQuotaManager_defaultsChunkSize(t *testing.T) {
	t.Parallel()

	svc := &Service{cfg: &config.Config{}}
	qm := NewQuotaManager(svc)
	require.Equal(t, int64(5_000_000), qm.chunkSize)
	require.Equal(t, 20, qm.thresholdPct)
}
