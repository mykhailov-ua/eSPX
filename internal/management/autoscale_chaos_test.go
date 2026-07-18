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
	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_AutoscaleNoDoubleFreeze guards concurrent autoscale ticks apply at most one transfer per stats fingerprint.
func TestChaos_AutoscaleNoDoubleFreeze(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel:       "test:autoscale-double-freeze",
		AutoscaleHighCTRThreshold:   0.015,
		AutoscaleMinImpressions:     100,
		AutoscaleLowCTRThreshold:    0.005,
		AutoscaleMinRemainingBudget: 20_000_000,
		AutoscaleShiftAmount:        10_000_000,
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Autoscale Freeze", 1_000_000_000, "USD"))

	lowCTR, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "Low CTR", 100_000_000, "low-freeze"))
	require.NoError(t, err)
	highCTR, err := svc.CreateCampaign(ctx, testCampaignSpec(customerID, "High CTR", 100_000_000, "high-freeze"))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count) VALUES
		($1, CURRENT_DATE, 1000, 2, 0),
		($2, CURRENT_DATE, 500, 15, 0)`,
		ingestion.ToUUID(lowCTR), ingestion.ToUUID(highCTR))
	require.NoError(t, err)

	queries := db.New(pool)
	syncWorker := ingestion.NewSyncWorker(rdb, ingestion.NewCampaignRepo(queries), ingestion.NewCustomerRepo(queries), 100*time.Millisecond, nil, 0)
	_, err = pool.Exec(ctx, `DELETE FROM outbox_events`)
	require.NoError(t, err)

	var balanceBefore int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM customers WHERE id = $1`, ingestion.ToUUID(customerID)).Scan(&balanceBefore))

	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	var firstErr error
	var errMu sync.Mutex
	for range workers {
		go func() {
			defer wg.Done()
			if err := svc.AutoscaleBudgets(ctx, []*ingestion.SyncWorker{syncWorker}); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.NoError(t, firstErr)

	var limitLow, limitHigh int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT budget_limit FROM campaigns WHERE id = $1`, ingestion.ToUUID(lowCTR)).Scan(&limitLow))
	require.NoError(t, pool.QueryRow(ctx, `SELECT budget_limit FROM campaigns WHERE id = $1`, ingestion.ToUUID(highCTR)).Scan(&limitHigh))
	assert.Equal(t, int64(90_000_000), limitLow)
	assert.Equal(t, int64(110_000_000), limitHigh)

	var autoscaleFreeze, autoscaleRelease int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balance_ledger
		WHERE type = 'FREEZE' AND idempotency_hash LIKE 'autoscale-transfer:%'`).Scan(&autoscaleFreeze))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM balance_ledger
		WHERE type = 'RELEASE' AND idempotency_hash LIKE 'autoscale-transfer:%'`).Scan(&autoscaleRelease))
	assert.Equal(t, 1, autoscaleFreeze, "exactly one autoscale FREEZE per stats fingerprint")
	assert.Equal(t, 1, autoscaleRelease, "exactly one autoscale RELEASE per stats fingerprint")

	var balanceAfter int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT balance FROM customers WHERE id = $1`, ingestion.ToUUID(customerID)).Scan(&balanceAfter))
	assert.Equal(t, balanceBefore, balanceAfter, "autoscale transfer must not change customer balance")

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'CREATE_CAMPAIGN'`).Scan(&outboxCount))
	assert.Equal(t, 2, outboxCount)

	logChaosProof(t, "autoscale_no_double_freeze", map[string]string{
		"subsystem":         "management_autoscale",
		"workers":           strconv.Itoa(workers),
		"autoscale_freeze":  strconv.Itoa(autoscaleFreeze),
		"autoscale_release": strconv.Itoa(autoscaleRelease),
		"limit_low":         strconv.FormatInt(limitLow, 10),
		"limit_high":        strconv.FormatInt(limitHigh, 10),
		"baseline_ok":       "true",
		"fault_type":        "concurrency_stress",
	})
}
