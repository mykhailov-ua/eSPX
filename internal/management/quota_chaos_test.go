package management

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const quotaChaosChunkMicro int64 = 1_000_000

func seedQuotaChaosCampaign(t *testing.T, pool *pgxpool.Pool, campaignID, customerID uuid.UUID, budgetLimit int64) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'quota-chaos', $2, 0, 'ACTIVE', $3, 'ASAP', 'UTC', 86400)
		ON CONFLICT (id) DO NOTHING`,
		ingestion.ToUUID(campaignID), budgetLimit, ingestion.ToUUID(customerID))
	require.NoError(t, err)
}

// TestChaos_QuotaRefillRace proves concurrent refill workers collapse to a single chunk via GETDEL lock claim.
func TestChaos_QuotaRefillRace(t *testing.T) {
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
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'quota-chaos', 0, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	seedQuotaChaosCampaign(t, pool, campaignID, customerID, 10_000_000)

	cfg := &config.Config{
		QuotaMode:               "live",
		QuotaChunkSize:          quotaChaosChunkMicro,
		QuotaRefillThresholdPct: 20,
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	qm := NewQuotaManager(svc)

	lockKey := "budget:refill_lock:" + campaignID.String()
	require.NoError(t, rdb.Set(ctx, lockKey, "1", 10*time.Second).Err())

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			_ = qm.refillCampaign(ctx, campaignID, 0, rdb)
		}()
	}
	wg.Wait()

	quotaRepo := ingestion.NewQuotaRepo(pool)
	pgQuota, err := quotaRepo.GetQuota(ctx, svc.sharder, campaignID)
	require.NoError(t, err)
	require.Equal(t, quotaChaosChunkMicro, pgQuota.ReservedAmount, "exactly one PG chunk must be reserved")

	redisQuota, err := rdb.Get(ctx, "budget:quota:"+campaignID.String()).Int64()
	require.NoError(t, err)
	require.Equal(t, quotaChaosChunkMicro, redisQuota, "exactly one Redis chunk must be credited")

	exists, err := rdb.Exists(ctx, lockKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists, "refill lock must be consumed")

	logChaosProof(t, "quota_refill_race", map[string]string{
		"subsystem":         "management_quota",
		"workers":           strconv.Itoa(workers),
		"pg_reserved":       strconv.FormatInt(pgQuota.ReservedAmount, 10),
		"redis_quota":       strconv.FormatInt(redisQuota, 10),
		"baseline_ok":       "true",
		"budget_consistent": "true",
	})
}

// TestChaos_QuotaDeadShardRelease releases PG reservations only after dead-shard quorum (M3).
func TestChaos_QuotaDeadShardRelease(t *testing.T) {
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
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'quota-dead-shard', 0, 'USD')`,
		ingestion.ToUUID(customerID))
	require.NoError(t, err)
	seedQuotaChaosCampaign(t, infra.Pool, campaignID, customerID, 5_000_000)

	const stuckReserved int64 = 2_000_000
	_, err = infra.Pool.Exec(ctx, `
		INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size, updated_at)
		VALUES (0, $1, $2, $3, NOW() - INTERVAL '90 seconds')`,
		ingestion.ToUUID(campaignID), stuckReserved, quotaChaosChunkMicro)
	require.NoError(t, err)

	cfg := &config.Config{QuotaMode: "live", QuotaChunkSize: quotaChaosChunkMicro, QuotaAutoRepair: true}
	svc := newBareService(t, infra.Pool, []redis.UniversalClient{infra.Redis}, cfg)
	quorumDur := 150 * time.Millisecond
	worker := NewReconWorkerWithQuorum(svc, time.Hour, quorumDur)
	worker.Quorum().SetBreakerPctFunc(func(context.Context, int) float64 { return 1.0 })

	stopMgmtContainer(t, infra.RedisContainer)
	requireMgmtFaultActive(t, func() bool {
		return infra.Redis.Ping(ctx).Err() != nil
	}, "redis ping must fail after stop")

	deadline := time.Now().Add(quorumDur + 100*time.Millisecond)
	for time.Now().Before(deadline) {
		worker.Quorum().ObserveShard(ctx, 0, infra.Redis)
		time.Sleep(25 * time.Millisecond)
	}
	require.True(t, worker.Quorum().DeadShardConfirmed(0), "dead shard quorum must confirm after sustained outage")

	worker.ReconcileQuotas(ctx)

	var reservedAfter int64
	require.NoError(t, infra.Pool.QueryRow(ctx, `
		SELECT reserved_amount FROM campaign_quotas WHERE shard_id = 0 AND campaign_id = $1`,
		ingestion.ToUUID(campaignID)).Scan(&reservedAfter))
	require.Equal(t, int64(0), reservedAfter, "dead shard recon must release stuck reservations")

	logChaosProof(t, "quota_dead_shard_release", map[string]string{
		"subsystem":      "management_quota_recon",
		"shard":          "0",
		"released_micro": strconv.FormatInt(stuckReserved, 10),
		"reserved_after": "0",
		"quorum_ms":      strconv.FormatInt(quorumDur.Milliseconds(), 10),
		"baseline_ok":    "true",
		"fault_verify":   "redis_container_stopped",
	})
}
