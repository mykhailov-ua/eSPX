package management

import (
	"context"
	"strconv"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestChaos_QuotaDriftRepair enqueues QUOTA_REPAIR when PG reserved exceeds Redis after a crash gap.
func TestChaos_QuotaDriftRepair(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	customerID := uuid.New()
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'quota-drift', 0, 'USD')`,
		ads.ToUUID(customerID))
	require.NoError(t, err)
	seedQuotaChaosCampaign(t, pool, campaignID, customerID, 10_000_000)

	const reserved = quotaChaosChunkMicro
	_, err = pool.Exec(ctx, `
		INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size, updated_at)
		VALUES (0, $1, $2, $3, NOW() - INTERVAL '35 seconds')`,
		ads.ToUUID(campaignID), reserved, quotaChaosChunkMicro)
	require.NoError(t, err)

	cfg := &config.Config{
		QuotaMode:       "live",
		QuotaChunkSize:  quotaChaosChunkMicro,
		QuotaAutoRepair: true,
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	worker := NewReconWorker(svc, time.Hour)

	start := time.Now()
	worker.RepairQuotaDrift(ctx)

	var outboxID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT id FROM outbox_events WHERE event_type = 'QUOTA_REPAIR' ORDER BY id DESC LIMIT 1`,
	).Scan(&outboxID))

	ob := NewOutboxWorker(svc)
	require.NoError(t, ob.ProcessOutbox(ctx))

	redisQuota, err := rdb.Get(ctx, "budget:quota:"+campaignID.String()).Int64()
	require.NoError(t, err)
	require.Equal(t, reserved, redisQuota)

	var spend, limit int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT current_spend, budget_limit FROM campaigns WHERE id = $1`, ads.ToUUID(campaignID),
	).Scan(&spend, &limit))
	require.LessOrEqual(t, spend, limit)

	var auditCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM admin_audit_log WHERE action = 'QUOTA_REPAIR_TOPUP' AND target_id = $1`,
		ads.ToUUID(campaignID)).Scan(&auditCount))
	require.GreaterOrEqual(t, auditCount, 1)

	elapsed := time.Since(start)
	logChaosProof(t, "quota_drift_repair", map[string]string{
		"subsystem":        "management_quota_recon",
		"repair_micro":     strconv.FormatInt(reserved, 10),
		"redis_quota":      strconv.FormatInt(redisQuota, 10),
		"elapsed_ms":       strconv.FormatInt(elapsed.Milliseconds(), 10),
		"budget_invariant": "true",
		"baseline_ok":      "true",
	})
}

// TestChaos_QuotaDeadShardTransientBlip does not release PG reservations on a short Redis outage.
func TestChaos_QuotaDeadShardTransientBlip(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupMgmtChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	campaignID := uuid.New()
	customerID := uuid.New()
	_, err := infra.Pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'quota-blip', 0, 'USD')`,
		ads.ToUUID(customerID))
	require.NoError(t, err)
	seedQuotaChaosCampaign(t, infra.Pool, campaignID, customerID, 5_000_000)

	const stuckReserved int64 = 2_000_000
	_, err = infra.Pool.Exec(ctx, `
		INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size, updated_at)
		VALUES (0, $1, $2, $3, NOW() - INTERVAL '90 seconds')`,
		ads.ToUUID(campaignID), stuckReserved, quotaChaosChunkMicro)
	require.NoError(t, err)

	cfg := &config.Config{QuotaMode: "live", QuotaChunkSize: quotaChaosChunkMicro, QuotaAutoRepair: true}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	worker := NewReconWorkerWithQuorum(svc, time.Hour, 200*time.Millisecond)
	worker.Quorum().SetBreakerPctFunc(func(context.Context, int) float64 { return 1.0 })

	stopMgmtContainer(t, infra.RedisContainer)
	time.Sleep(50 * time.Millisecond)
	startMgmtContainer(t, infra.RedisContainer)
	infra.refreshRedisClient(t)

	worker.ReconcileQuotas(ctx)

	var reservedAfter int64
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT reserved_amount FROM campaign_quotas WHERE shard_id = 0 AND campaign_id = $1`,
		ads.ToUUID(campaignID)).Scan(&reservedAfter))
	require.Equal(t, stuckReserved, reservedAfter, "transient blip must not release reservations")

	logChaosProof(t, "quota_dead_shard_transient_blip", map[string]string{
		"subsystem":      "management_quota_recon",
		"reserved_after": strconv.FormatInt(reservedAfter, 10),
		"released":       "false",
	})
}
