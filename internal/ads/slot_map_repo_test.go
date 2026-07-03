package ads

import (
	"context"
	"sync"
	"testing"

	"espx/internal/ads/db"
	"espx/internal/database"

	"github.com/stretchr/testify/require"
)

func TestSlotMapRepo_CreateNextVersion_ACID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	repo := NewSlotMapRepo(pool)

	active, err := repo.GetActiveVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(1), active)

	rows, err := repo.ListVersion(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rows, SlotCount)

	newVersion, err := repo.CreateNextVersion(ctx, 1, []SlotOverride{
		{Slot: 0, ShardID: 2, State: db.RedisSlotStateMIGRATING},
		{Slot: 1, ShardID: 2, State: db.RedisSlotStateMIGRATING},
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), newVersion)

	// Active version unchanged until explicit activate.
	active, err = repo.GetActiveVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(1), active)

	v2, err := repo.ListVersion(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, db.RedisSlotStateMIGRATING, v2[0].State)
	require.Equal(t, int16(2), v2[0].ShardID)
	require.Equal(t, db.RedisSlotStateACTIVE, v2[2].State)
}

func TestSlotMapRepo_ActivateVersion_serializesMeta(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	repo := NewSlotMapRepo(pool)
	newVersion, err := repo.CreateNextVersion(ctx, 1, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- repo.ActivateVersion(ctx, newVersion)
		}()
	}
	wg.Wait()
	close(errs)

	var success int
	for err := range errs {
		if err == nil {
			success++
		}
	}
	require.Equal(t, 2, success)

	active, err := repo.GetActiveVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, newVersion, active)
}

func TestSlotMapRepo_MarkSlotsMigrating_rowLocks(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	repo := NewSlotMapRepo(pool)
	v, err := repo.CreateNextVersion(ctx, 1, nil)
	require.NoError(t, err)

	err = repo.MarkSlotsMigrating(ctx, v, []int16{10, 11, 12}, 3)
	require.NoError(t, err)

	migrating, err := repo.ListMigratingSlots(ctx, v)
	require.NoError(t, err)
	require.Len(t, migrating, 3)
	for _, row := range migrating {
		require.Equal(t, int16(3), row.ShardID)
		require.Equal(t, db.RedisSlotStateMIGRATING, row.State)
	}
}

func TestTableFromRows_matchesModulo(t *testing.T) {
	rows := make([]db.RedisSlotMap, SlotCount)
	for i := range rows {
		rows[i] = db.RedisSlotMap{
			Slot:    int16(i),
			ShardID: int16(i % 4),
			State:   db.RedisSlotStateACTIVE,
		}
	}
	table, err := TableFromRows(rows)
	require.NoError(t, err)

	sharder := NewStaticSlotSharder(4)
	for slot := 0; slot < SlotCount; slot++ {
		require.Equal(t, uint16(slot%4), table[slot])
	}
	_ = sharder
}

func TestSlotMapRepo_invalidOverrideRejected(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	repo := NewSlotMapRepo(pool)
	_, err := repo.CreateNextVersion(ctx, 1, []SlotOverride{
		{Slot: 2000, ShardID: 0, State: db.RedisSlotStateACTIVE},
	})
	require.ErrorIs(t, err, ErrSlotMapInvalidSlot)
}
