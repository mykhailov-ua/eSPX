package ads

import (
	"context"
	"sync"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnifiedFilter_migrationFenceRejectsDebit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)
	require.NoError(t, rdb.Set(ctx, MigrationFenceRedisKey(campID), 1, 0).Err())

	evt := &domain.Event{
		Type:       "click",
		IP:         "203.0.113.50",
		UserID:     "fence-u1",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	err := f.Check(checkCtx, evt)
	require.ErrorIs(t, err, ErrMigrationFenced)

	remaining, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(9_000_000_000_000_000), remaining)
}

func TestUnifiedFilter_budgetFrozenRejectsDebit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)
	require.NoError(t, SetBudgetFrozen(ctx, rdb, campID))

	evt := &domain.Event{
		Type:       "click",
		IP:         "203.0.113.51",
		UserID:     "freeze-u1",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	err := f.Check(checkCtx, evt)
	require.ErrorIs(t, err, ErrMigrationFenced)

	remaining, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(9_000_000_000_000_000), remaining)
}

func TestBumpMigrationFences_setsRedisAndPG(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	campID := uuid.New()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'fence', 0, 'USD')`,
		ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'fence-camp', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ToUUID(campID), ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, BumpMigrationFences(ctx, pool, rdb, []uuid.UUID{campID}))

	var gen int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT migration_gen FROM campaigns WHERE id = $1`, ToUUID(campID)).Scan(&gen))
	require.Equal(t, int64(1), gen)

	val, err := rdb.Get(ctx, MigrationFenceRedisKey(campID)).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), val)
}

func TestChaos_MigrationFenceConcurrentDebit(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)
	require.NoError(t, rdb.Set(ctx, MigrationFenceRedisKey(campID), 1, 0).Err())

	const workers = 32
	var wg sync.WaitGroup
	var fenced int64
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			evt := &domain.Event{
				Type:       "click",
				IP:         "203.0.113.52",
				UserID:     "fence-race",
				CampaignID: campID,
				ClickID:    uuid.NewString(),
			}
			checkCtx := attachFilterDeadline(ctx, time.Second)
			if err := f.Check(checkCtx, evt); err != nil {
				if err == ErrMigrationFenced {
					fenced++
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(workers), fenced)

	remaining, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(9_000_000_000_000_000), remaining)

	t.Logf("chaos_proof fault=slot_migration_fence subsystem=ads_lua workers=32 fenced=%d budget_unchanged=true", fenced)
}
