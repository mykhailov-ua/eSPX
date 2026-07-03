package sharding

import (
	"context"
	"testing"

	"espx/internal/ads/db"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLoadActiveSlotMap_fromPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	sharder := NewStaticSlotSharder(4)
	version, err := LoadActiveSlotMap(ctx, pool, sharder, 4)
	require.NoError(t, err)
	require.Equal(t, int32(1), version)
	require.Equal(t, int32(1), sharder.ActiveVersion())

	id := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	require.Equal(t, NewStaticSlotSharder(4).GetShard(id), sharder.GetShard(id))
}

func TestReloadStaticSlotMapIfChanged_skipsWhenCurrent(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	sharder := NewStaticSlotSharder(4)
	_, err := LoadActiveSlotMap(ctx, pool, sharder, 4)
	require.NoError(t, err)

	version, changed, err := ReloadStaticSlotMapIfChanged(ctx, pool, sharder, 4)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, int32(1), version)
}

func TestSlotMapReloadMessage_roundtrip(t *testing.T) {
	payload, err := EncodeSlotMapReloadMessage(3)
	require.NoError(t, err)
	msg, err := DecodeSlotMapReloadMessage(payload)
	require.NoError(t, err)
	require.Equal(t, int32(3), msg.Version)
}

func TestOpsSlotMapShardTable(t *testing.T) {
	rows := make([]db.RedisSlotMap, SlotCount)
	for i := range rows {
		rows[i] = db.RedisSlotMap{
			Slot:    int16(i),
			ShardID: int16(i % 4),
			State:   db.RedisSlotStateACTIVE,
		}
	}
	slots, err := SlotMapShardTable(rows)
	require.NoError(t, err)
	require.Len(t, slots, SlotCount)
	require.Equal(t, uint16(2), slots[2])
}

func TestEdgeShardPick_matchesGo(t *testing.T) {
	modulo := NewStaticSlotSharder(4)
	ids := []uuid.UUID{
		uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		uuid.New(),
		uuid.New(),
	}
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
	fromPG := NewStaticSlotSharder(4)
	fromPG.StoreSlotMap(table)

	for _, id := range ids {
		require.Equal(t, modulo.GetShard(id), fromPG.GetShard(id))
	}
}
