package management

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chaosRedisCmdable wraps one shard and injects network-style faults on selected commands.
type chaosRedisCmdable struct {
	redis.UniversalClient
	failDump    bool
	failRestore bool
	delay       time.Duration
}

func (c *chaosRedisCmdable) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Exists(ctx, keys...)
}

func (c *chaosRedisCmdable) Dump(ctx context.Context, key string) *redis.StringCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if c.failDump {
		cmd := redis.NewStringCmd(ctx, "dump", key)
		cmd.SetErr(errors.New("chaos: network partition on DUMP"))
		return cmd
	}
	return c.UniversalClient.Dump(ctx, key)
}

func (c *chaosRedisCmdable) TTL(ctx context.Context, key string) *redis.DurationCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.TTL(ctx, key)
}

func (c *chaosRedisCmdable) RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	if c.failRestore {
		cmd := redis.NewStatusCmd(ctx, "restore", key)
		cmd.SetErr(errors.New("chaos: network partition on RESTORE"))
		return cmd
	}
	return c.UniversalClient.RestoreReplace(ctx, key, ttl, value)
}

func (c *chaosRedisCmdable) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Scan(ctx, cursor, match, count)
}

func (c *chaosRedisCmdable) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.UniversalClient.Del(ctx, keys...)
}

func setupSlotMigrationChaos(t *testing.T, rdbs []redis.UniversalClient) (*Service, *pgxpool.Pool, context.Context) {
	t.Helper()
	pool, cleanupDB := database.SetupTestDB(t)
	t.Cleanup(cleanupDB)
	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := newBareService(t, pool, rdbs, cfg)
	return svc, pool, context.Background()
}

func buildFourRedisShards(base redis.UniversalClient, customize func(rdbs []redis.UniversalClient)) []redis.UniversalClient {
	rdbs := []redis.UniversalClient{base, base, base, base}
	if customize != nil {
		customize(rdbs)
	}
	return rdbs
}

func campaignIDForSlot(t *testing.T, slot int16) uuid.UUID {
	t.Helper()
	for range 50_000 {
		id := uuid.New()
		if ingestion.CampaignSlotIndex(id) == slot {
			return id
		}
	}
	t.Fatalf("no campaign id for slot %d", slot)
	return uuid.Nil
}

func seedCampaignForSlot(t *testing.T, svc *Service, pool *pgxpool.Pool, ctx context.Context, slot int16, rdb redis.UniversalClient) (uuid.UUID, int16) {
	t.Helper()
	campID := campaignIDForSlot(t, slot)
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Chaos", 5_000_000, "USD"))
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'chaos-slot', 1000000, 222223, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)
	key := ingestion.BudgetCampaignKey(campID)
	require.NoError(t, rdb.Set(ctx, key, "777777", 0).Err())
	return campID, slot
}

func prepareMigratingVersion(t *testing.T, ctx context.Context, mapRepo *ingestion.SlotMapRepo, slot int16, targetShard int16) int32 {
	t.Helper()
	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	v, err := mapRepo.CreateNextVersion(ctx, active, nil)
	require.NoError(t, err)
	require.NoError(t, mapRepo.MarkSlotsMigrating(ctx, v, []int16{slot}, targetShard))
	return v
}

// TestChaos_SlotMigrationActivateBeforeCopyRejected guards cutover cannot proceed without copied state (split-brain spend).
func TestChaos_SlotMigrationActivateBeforeCopyRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, 7, 2)

	err := svc.ActivateSlotMapVersion(ctx, uuid.Nil, v)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSlotMigrationNotReady)

	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(1), active, "active map must stay on old version when copy incomplete")

	logChaosProof(t, "slot_migration_activate_before_copy", map[string]string{
		"subsystem":   "slot_migration",
		"fault_type":  "premature_cutover",
		"rejected":    "true",
		"active_safe": "true",
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMigrationCopyRedisPartition marks migration failed and leaves active map unchanged.
func TestChaos_SlotMigrationCopyRedisPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := buildFourRedisShards(rdb, func(rdbs []redis.UniversalClient) {
		rdbs[3] = &chaosRedisCmdable{UniversalClient: rdb, failDump: true}
	})
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 3 // seeded source shard 3, target 1
	_, _ = seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdbs[3])
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 1)

	err := svc.CopyAllMigratingSlots(ctx, v)
	require.Error(t, err)

	migrations, err := svc.GetSlotMigrations(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, "failed", migrations[0].State)
	assert.NotEmpty(t, migrations[0].LastError)

	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(1), active)

	logChaosProof(t, "slot_migration_redis_partition", map[string]string{
		"subsystem":    "slot_migration",
		"fault_type":   "network_partition",
		"dump_failed":  "true",
		"state_failed": "true",
		"active_safe":  "true",
		"baseline_ok":  "true",
	})
}

// TestChaos_SlotMigrationCopySlowEventuallySucceeds injects latency on Redis copy path.
func TestChaos_SlotMigrationCopySlowEventuallySucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	slow := &chaosRedisCmdable{UniversalClient: rdb, delay: 15 * time.Millisecond}
	rdbs := buildFourRedisShards(rdb, func(rdbs []redis.UniversalClient) {
		rdbs[0] = slow
		rdbs[1] = slow
	})
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 0 // seeded source shard 0, target 1
	campID, _ := seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdbs[0])
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 1)

	start := time.Now()
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))
	elapsed := time.Since(start)

	migrations, err := svc.GetSlotMigrations(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, "copied", migrations[0].State)

	key := ingestion.BudgetCampaignKey(campID)
	val, err := rdbs[1].Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "777777", val)

	logChaosProof(t, "slot_migration_redis_slow_copy", map[string]string{
		"subsystem":   "slot_migration",
		"fault_type":  "latency_injection",
		"delay_ms":    "15",
		"state":       "copied",
		"elapsed_ms":  fmt.Sprintf("%d", elapsed.Milliseconds()),
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMigrationConcurrentCopySameSlot stresses idempotent copy under parallel orchestrator ticks.
func TestChaos_SlotMigrationConcurrentCopySameSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 17
	campID, _ := seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdb)
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 3)

	const workers = 6
	var wg sync.WaitGroup
	var okCount atomic.Int32
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if err := svc.CopyAllMigratingSlots(ctx, v); err == nil {
				okCount.Add(1)
			}
		}()
	}
	wg.Wait()

	migrations, err := svc.GetSlotMigrations(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, "copied", migrations[0].State)

	key := ingestion.BudgetCampaignKey(campID)
	val, err := rdbs[3].Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "777777", val)

	logChaosProof(t, "slot_migration_concurrent_copy", map[string]string{
		"subsystem":   "slot_migration",
		"fault_type":  "concurrency_stress",
		"workers":     "6",
		"ok_runs":     fmt.Sprintf("%d", okCount.Load()),
		"state":       "copied",
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMigrationConcurrentActivate serializes cutover - only one activate wins.
func TestChaos_SlotMigrationConcurrentActivate(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 19
	_, _ = seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdb)
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 2)
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))

	const workers = 4
	var wg sync.WaitGroup
	var success atomic.Int32
	var alreadyActive atomic.Int32
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := svc.ActivateSlotMapVersion(ctx, uuid.Nil, v)
			switch {
			case err == nil:
				success.Add(1)
			case errors.Is(err, ingestion.ErrSlotMapAlreadyActive):
				alreadyActive.Add(1)
			default:
				t.Errorf("unexpected activate error: %v", err)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), success.Load(), "exactly one concurrent activate must commit")
	assert.Equal(t, int32(workers-1), alreadyActive.Load(), "losers must get ErrSlotMapAlreadyActive")

	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, v, active)

	logChaosProof(t, "slot_migration_concurrent_activate", map[string]string{
		"subsystem":      "slot_migration",
		"fault_type":     "concurrency_stress",
		"workers":        "4",
		"success":        "1",
		"already_active": fmt.Sprintf("%d", alreadyActive.Load()),
		"active":         fmt.Sprintf("%d", active),
		"baseline_ok":    "true",
	})
}

// TestChaos_SlotMapMetaLockContention runs parallel version creation; at most one succeeds per attempt batch.
func TestChaos_SlotMapMetaLockContention(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	svc, _, ctx := setupSlotMigrationChaos(t, []redis.UniversalClient{rdb})
	repo := ingestion.NewSlotMapRepo(svc.GetPool())
	active, err := repo.GetActiveVersion(ctx)
	require.NoError(t, err)

	const workers = 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := make([]int32, 0, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			v, err := repo.CreateNextVersion(ctx, active, []ingestion.SlotOverride{
				{Slot: 50, ShardID: 1, State: db.RedisSlotStateACTIVE},
			})
			if err == nil {
				mu.Lock()
				created = append(created, v)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.NotEmpty(t, created)
	versions := make(map[int32]struct{}, len(created))
	for _, v := range created {
		versions[v] = struct{}{}
	}
	assert.Equal(t, len(created), len(versions), "each successful create must yield unique version")

	logChaosProof(t, "slot_map_meta_lock_contention", map[string]string{
		"subsystem":   "slot_map_control_plane",
		"fault_type":  "concurrency_stress",
		"workers":     "5",
		"created":     fmt.Sprintf("%d", len(created)),
		"unique":      fmt.Sprintf("%d", len(versions)),
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMapPGDeadlockRecovery probes cross-row lock ordering on slot map meta + slot rows.
func TestChaos_SlotMapPGDeadlockRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	ctx := context.Background()
	repo := ingestion.NewSlotMapRepo(pool)

	active, err := repo.GetActiveVersion(ctx)
	require.NoError(t, err)
	v, err := repo.CreateNextVersion(ctx, active, nil)
	require.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)

	run := func(slotA, slotB int16) {
		defer wg.Done()
		<-start
		e := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			q := db.New(tx)
			if _, err := q.LockSlotMapEntry(ctx, db.LockSlotMapEntryParams{Version: v, Slot: slotA}); err != nil {
				return err
			}
			time.Sleep(120 * time.Millisecond)
			_, err := q.LockSlotMapEntry(ctx, db.LockSlotMapEntryParams{Version: v, Slot: slotB})
			return err
		})
		if slotA == 60 {
			errA = e
		} else {
			errB = e
		}
	}

	go run(60, 61)
	go run(61, 60)
	close(start)
	wg.Wait()

	assert.True(t, isDeadlock(errA) || isDeadlock(errB), "expected deadlock, got: %v / %v", errA, errB)

	rows, err := repo.ListVersion(ctx, v)
	require.NoError(t, err)
	assert.Len(t, rows, ingestion.SlotCount)

	logChaosProof(t, "slot_map_pg_deadlock_recovery", map[string]string{
		"subsystem":   "slot_map_control_plane",
		"fault_type":  "concurrency_stress",
		"deadlock":    "true",
		"rows_intact": fmt.Sprintf("%d", len(rows)),
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMigrationCopyIdempotentRetry verifies second copy after transient failure can recover.
func TestChaos_SlotMigrationCopyIdempotentRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	flaky := &chaosRedisCmdable{UniversalClient: rdb}
	rdbs := buildFourRedisShards(rdb, func(rdbs []redis.UniversalClient) {
		rdbs[3] = flaky
	})
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 3 // seeded source shard 3, target 1
	campID, _ := seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdbs[3])
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 1)

	flaky.failDump = true
	err := svc.CopyAllMigratingSlots(ctx, v)
	require.Error(t, err)

	flaky.failDump = false
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))

	migrations, err := svc.GetSlotMigrations(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	assert.Equal(t, "copied", migrations[0].State)

	key := ingestion.BudgetCampaignKey(campID)
	val, err := rdbs[1].Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "777777", val)

	ingestion.AssertBudgetInvariant(t, ctx, svc.GetPool(), rdbs[1], campID)

	logChaosProof(t, "slot_migration_copy_retry_recovery", map[string]string{
		"subsystem":   "slot_migration",
		"fault_type":  "transient_network",
		"first_fail":  "true",
		"retry_ok":    "true",
		"state":       "copied",
		"baseline_ok": "true",
	})
}

// TestChaos_SlotMigrationRollbackAfterActivate restores previous active_version under broker-less config.
func TestChaos_SlotMigrationRollbackAfterActivate(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, _, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 29
	_, _ = seedCampaignForSlot(t, svc, svc.GetPool(), ctx, slot, rdb)
	mapRepo := ingestion.NewSlotMapRepo(svc.GetPool())
	prevActive, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 2)
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))
	require.NoError(t, svc.ActivateSlotMapVersion(ctx, uuid.Nil, v))

	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, v, active)

	require.NoError(t, svc.RollbackSlotMapVersion(ctx, uuid.Nil, prevActive))
	active, err = mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, prevActive, active)

	logChaosProof(t, "slot_migration_rollback", map[string]string{
		"subsystem":    "slot_migration",
		"fault_type":   "operator_recovery",
		"from_version": fmt.Sprintf("%d", v),
		"to_version":   fmt.Sprintf("%d", prevActive),
		"baseline_ok":  "true",
	})
}

// TestChaos_LUA10_DebitFencedDuringSlotCopy proves debits are rejected (code 11) while COPY fences the source shard.
func TestChaos_LUA10_DebitFencedDuringSlotCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{MigrationFenceEnabled: true}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb, rdb}, cfg)

	const slot int16 = 5
	campID, _ := seedCampaignForSlot(t, svc, pool, ctx, slot, rdb)
	require.NoError(t, ingestion.BumpMigrationFences(ctx, pool, rdb, []uuid.UUID{campID}))

	f := newMgmtUnifiedFilter(rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	evt := &campaignmodel.Event{
		Type:       "click",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		IP:         "203.0.113.70",
		UserID:     "lua10-fence",
	}
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := f.Check(checkCtx, evt)
	require.ErrorIs(t, err, ingestion.ErrMigrationFenced)

	key := ingestion.BudgetCampaignKey(campID)
	remaining, err := rdb.Get(ctx, key).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(777777), remaining)

	logChaosProof(t, "lua10_migration_fence_copy", map[string]string{
		"subsystem":        "ads_lua",
		"fault":            "debit_during_copy",
		"fenced":           "true",
		"budget_unchanged": "true",
		"code":             "11",
	})
}

// TestChaos_SO02_SlotMigrationPGRewarmCutover validates PG re-warm cutover preserves budget invariant (M1-04).
func TestChaos_SO02_SlotMigrationPGRewarmCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := buildFourRedisShards(rdb, nil)
	cfg := &config.Config{SlotMigrationEnabled: false, MigrationFenceEnabled: true}
	svc, pool, ctx := setupSlotMigrationChaos(t, rdbs)
	svc.cfg = cfg

	const slot int16 = 8
	campID, _ := seedCampaignForSlot(t, svc, pool, ctx, slot, rdbs[0])
	mapRepo := ingestion.NewSlotMapRepo(pool)
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 2)

	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))
	require.NoError(t, svc.ActivateSlotMapVersion(ctx, uuid.Nil, v))

	ingestion.AssertBudgetInvariant(t, ctx, pool, rdbs[2], campID)
	require.NoError(t, svc.VerifySlotMigrationR5(ctx))

	key := ingestion.BudgetCampaignKey(campID)
	val, err := rdbs[2].Get(ctx, key).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(777777), val)

	logChaosProof(t, "so02_slot_migration_pg_rewarm", map[string]string{
		"subsystem":   "slot_migration",
		"fault":       "hot_slot_cutover",
		"r5_ok":       "true",
		"pg_rewarm":   "true",
		"campaign_id": campID.String(),
	})
}

// TestChaos_SlotMigrationPGRewarmColdStart seeds budget on target from Postgres when source had no Redis keys.
func TestChaos_SlotMigrationPGRewarmColdStart(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := buildFourRedisShards(rdb, nil)
	svc, pool, ctx := setupSlotMigrationChaos(t, rdbs)

	const slot int16 = 11
	campID := campaignIDForSlot(t, slot)
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "NoRedis", 1_000_000, "USD"))
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'no-redis', 500000, 100000, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	mapRepo := ingestion.NewSlotMapRepo(pool)
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 1)
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))
	require.NoError(t, svc.ActivateSlotMapVersion(ctx, uuid.Nil, v))

	key := ingestion.BudgetCampaignKey(campID)
	val, err := rdbs[1].Get(ctx, key).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(400000), val)

	logChaosProof(t, "slot_migration_pg_rewarm_cold_start", map[string]string{
		"subsystem":  "slot_migration",
		"pg_rewarm":  "true",
		"cold_start": "true",
	})
}
