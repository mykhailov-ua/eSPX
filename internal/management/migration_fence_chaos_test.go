package management

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// mgmtTestRegistry is a minimal campaign registry for management integration tests.
type mgmtTestRegistry struct{}

func (mgmtTestRegistry) Exists(uuid.UUID) bool { return true }
func (mgmtTestRegistry) Add(uuid.UUID, uuid.UUID, *uuid.UUID, string, domain.PacingMode, int64, string, int32, int32, []string) {
}
func (mgmtTestRegistry) GetCustomerID(uuid.UUID) (uuid.UUID, bool) { return uuid.Nil, true }
func (mgmtTestRegistry) GetCampaign(id uuid.UUID) (*domain.Campaign, bool) {
	cp := &domain.Campaign{ID: id, CustomerID: uuid.New(), Location: time.UTC}
	cp.IDStr = id.String()
	cp.BudgetCampaignKey = "budget:campaign:" + cp.IDStr
	cp.CampaignSyncKey = "budget:sync:campaign:" + cp.IDStr
	cp.CustomerSyncKey = "budget:sync:customer:" + cp.CustomerID.String()
	return cp, true
}
func (mgmtTestRegistry) Sync(context.Context) (int, error)        { return 0, nil }
func (mgmtTestRegistry) StartSync(context.Context, time.Duration) {}
func (mgmtTestRegistry) Wait(context.Context) error               { return nil }

func newMgmtUnifiedFilter(rdb redis.UniversalClient) *ads.UnifiedFilter {
	return ads.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		ads.NewJumpHashSharder(1),
		mgmtTestRegistry{},
		nil,
		10_000, time.Minute, time.Hour, time.Hour,
		100_000, 10_000, "events", 10_000,
	)
}

// TestChaos_OutboxBudgetFreezePriority processes BUDGET_FREEZE before bulk pacing backlog.
func TestChaos_OutboxBudgetFreezePriority(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:update-freeze"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	const pacingBacklog = 200
	pacingPayload, err := json.Marshal(campaignPacingPayload{
		CampaignID: uuid.New().String(),
		PacingMode: "even",
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (event_type, payload)
		SELECT 'UPDATE_CAMPAIGN_PACING', $1::jsonb
		FROM generate_series(1, $2)`, pacingPayload, pacingBacklog)
	require.NoError(t, err)

	campID := uuid.New()
	freezePayload, err := json.Marshal(CampaignPayload{CampaignID: campID.String()})
	require.NoError(t, err)
	_, err = rdb.Set(ctx, "budget:campaign:"+campID.String(), 5_000_000, 0).Result()
	require.NoError(t, err)

	var freezeID int64
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO outbox_events (event_type, payload) VALUES ('BUDGET_FREEZE', $1) RETURNING id`,
		freezePayload).Scan(&freezeID))

	worker := NewOutboxWorker(svc)
	processed, err := worker.ProcessOutboxWithCount(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM outbox_events WHERE id = $1`, freezeID).Scan(&status))
	require.Equal(t, "PROCESSED", status)

	exists, err := rdb.Exists(ctx, ads.BudgetFrozenRedisKey(campID)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), exists)

	logChaosProof(t, "outbox_budget_freeze_priority", map[string]string{
		"subsystem":      "management_outbox",
		"pacing_backlog": strconv.Itoa(pacingBacklog),
		"freeze_first":   "true",
	})
}

// TestChaos_SlotMigrationFence proves concurrent debits are rejected while source shard is fenced.
func TestChaos_SlotMigrationFence(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	campID := uuid.New()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency) VALUES ($1, 'mig-fence', 0, 'USD')`,
		ads.ToUUID(customerID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'mig-fence', 10000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ads.ToUUID(campID), ads.ToUUID(customerID))
	require.NoError(t, err)

	require.NoError(t, ads.BumpMigrationFences(ctx, pool, rdb, []uuid.UUID{campID}))
	require.NoError(t, rdb.Set(ctx, "budget:campaign:"+campID.String(), 10_000_000, 0).Err())

	f := newMgmtUnifiedFilter(rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	const workers = 32
	var wg sync.WaitGroup
	var fenced, debited int64
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			evt := &domain.Event{
				Type:       "click",
				CampaignID: campID,
				ClickID:    uuid.NewString(),
				IP:         "203.0.113.60",
				UserID:     "mig-fence",
			}
			checkCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			err := f.Check(checkCtx, evt)
			if err != nil {
				if err == ads.ErrMigrationFenced {
					fenced++
				}
				return
			}
			debited++
		}()
	}
	wg.Wait()

	require.Equal(t, int64(workers), fenced)
	require.Equal(t, int64(0), debited)

	ads.AssertBudgetInvariant(t, ctx, pool, rdb, campID)

	logChaosProof(t, "slot_migration_fence", map[string]string{
		"subsystem":         "slot_migration",
		"workers":           strconv.Itoa(workers),
		"fenced":            strconv.FormatInt(fenced, 10),
		"debited":           "0",
		"budget_consistent": "true",
	})
}
