package management

import (
	"testing"

	"espx/internal/config"
	"espx/internal/database"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSlotMigration_DualWriteCopyAndActivate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, pool, ctx := setupSlotMigrationChaos(t, rdbs)
	svc.cfg = &config.Config{
		SlotMigrationEnabled:          false,
		MigrationFenceEnabled:         true,
		SlotMigrationDualWriteEnabled: true,
		SlotMigrationLagEpsilon:       0,
		SlotMigrationLagThreshold:     1000,
	}

	const slot int16 = 8
	campID, _ := seedCampaignForSlot(t, svc, pool, ctx, slot, rdbs[0])
	mapRepo := ingestion.NewSlotMapRepo(pool)
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 2)

	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))
	migrations, err := svc.GetSlotMigrations(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrations, 1)
	require.Equal(t, "dual_writing", migrations[0].State)

	flag, err := rdbs[0].Get(ctx, ingestion.SlotMigrationDualWriteFlagKey).Result()
	require.NoError(t, err)
	require.NotEmpty(t, flag)

	require.NoError(t, ingestion.PublishSlotMigrationDeltaTestHelper(ctx, rdbs[0], ingestion.SlotMigrationDelta{
		CampaignID: campID,
		Amount:     500,
		SpendKey:   ingestion.BudgetCampaignKey(campID),
	}))
	require.NoError(t, svc.CatchUpDualWriteSlots(ctx, v))

	require.NoError(t, svc.ActivateSlotMapVersion(ctx, uuid.Nil, v))
	ingestion.AssertBudgetInvariant(t, ctx, pool, rdbs[2], campID)
}

func TestSlotMigration_DualWriteActivateBlockedOnLag(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	rdb, cleanup := database.SetupTestRedis(t)
	defer cleanup()
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc, pool, ctx := setupSlotMigrationChaos(t, rdbs)
	svc.cfg = &config.Config{
		SlotMigrationDualWriteEnabled: true,
		SlotMigrationLagEpsilon:       0,
	}

	const slot int16 = 8
	campID, _ := seedCampaignForSlot(t, svc, pool, ctx, slot, rdbs[0])
	mapRepo := ingestion.NewSlotMapRepo(pool)
	v := prepareMigratingVersion(t, ctx, mapRepo, slot, 2)
	require.NoError(t, svc.CopyAllMigratingSlots(ctx, v))

	require.NoError(t, ingestion.PublishSlotMigrationDeltaTestHelper(ctx, rdbs[0], ingestion.SlotMigrationDelta{
		CampaignID: campID,
		Amount:     100,
		SpendKey:   ingestion.BudgetCampaignKey(campID),
	}))

	err := svc.ActivateSlotMapVersion(ctx, uuid.Nil, v)
	require.ErrorIs(t, err, ErrSlotMigrationLagNotCaughtUp)

	job, err := ingestion.NewSlotMigrationRepo(pool).Get(ctx, v, slot)
	require.NoError(t, err)
	require.Equal(t, db.RedisSlotMigrationStateDualWriting, job.State)
}
