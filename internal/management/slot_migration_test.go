package management

import (
	"context"
	"testing"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSlotMigration_CopyAndActivate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb0, cleanup0 := database.SetupTestRedis(t)
	defer cleanup0()
	rdb1, cleanup1 := database.SetupTestRedis(t)
	defer cleanup1()

	cfg := &config.Config{SlotMigrationEnabled: false}
	svc := NewService(pool, []redis.UniversalClient{rdb0, rdb1}, ingestion.NewStaticSlotSharder(2), cfg)
	defer svc.Close()

	mapRepo := ingestion.NewSlotMapRepo(pool)
	active, err := mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)

	// Pick slot 0 from active map and a campaign that hashes to slot 0.
	var campID uuid.UUID
	var slot int16 = 0
	for {
		campID = uuid.New()
		if ingestion.CampaignSlotIndex(campID) == slot {
			break
		}
	}

	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Mig Customer", 1_000_000, "USD"))

	_, err = pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id, pacing_mode, timezone, freq_window)
		VALUES ($1, 'mig-test', 1000000, 0, 'ACTIVE', $2, 'ASAP', 'UTC', 86400)`,
		ingestion.ToUUID(campID), ingestion.ToUUID(customerID))
	require.NoError(t, err)

	key := "budget:campaign:" + campID.String()
	require.NoError(t, rdb0.Set(ctx, key, "900000", 0).Err())

	newVersion, err := mapRepo.CreateNextVersion(ctx, active, nil)
	require.NoError(t, err)
	require.NoError(t, mapRepo.MarkSlotsMigrating(ctx, newVersion, []int16{slot}, 1))

	require.NoError(t, svc.CopyAllMigratingSlots(ctx, newVersion))

	migrations, err := svc.GetSlotMigrations(ctx, newVersion)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	require.Equal(t, "copied", migrations[0].State)
	require.Equal(t, int32(1), migrations[0].CampaignsTotal)

	val, err := rdb1.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "900000", val)

	require.NoError(t, svc.ActivateSlotMapVersion(ctx, uuid.Nil, newVersion))

	active, err = mapRepo.GetActiveVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, newVersion, active)

	rows, err := mapRepo.ListVersion(ctx, newVersion)
	require.NoError(t, err)
	require.Equal(t, db.RedisSlotStateDRAINING, rows[slot].State)

	require.NoError(t, svc.DrainMigratingSlots(ctx, newVersion))
	require.Equal(t, int64(0), rdb0.Exists(ctx, key).Val())
}
