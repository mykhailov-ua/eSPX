package ingestion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"espx/internal/ingestion/sqlc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadActiveSlotMap reads active_version from Postgres and atomically swaps the sharder table.
// On missing schema or incomplete map, falls back to slot % fallbackBuckets and returns an error.
func LoadActiveSlotMap(
	ctx context.Context,
	pool *pgxpool.Pool,
	sharder *StaticSlotSharder,
	fallbackBuckets int,
) (int32, error) {
	if sharder == nil {
		return 0, fmt.Errorf("slot map loader: nil sharder")
	}
	if pool == nil {
		sharder.SwapSnapshot(0, buildSlotTable(fallbackBuckets), 0)
		return 0, fmt.Errorf("slot map loader: nil pool")
	}

	repo := NewSlotMapRepo(pool)
	version, err := repo.GetActiveVersion(ctx)
	if err != nil {
		sharder.SwapSnapshot(0, buildSlotTable(fallbackBuckets), 0)
		return 0, fmt.Errorf("slot map meta: %w", err)
	}

	rows, err := repo.ListVersion(ctx, version)
	if err != nil {
		sharder.SwapSnapshot(0, buildSlotTable(fallbackBuckets), 0)
		return version, err
	}

	table, err := TableFromRows(rows)
	if err != nil {
		sharder.SwapSnapshot(0, buildSlotTable(fallbackBuckets), 0)
		return version, err
	}

	st := slotTable(*table)
	sharder.SwapSnapshot(version, &st, 0)
	slog.Info("loaded active slot map from postgres", "version", version, "buckets", fallbackBuckets)
	return version, nil
}

// ReloadStaticSlotMapIfChanged reloads when Postgres active_version differs from sharder state.
func ReloadStaticSlotMapIfChanged(
	ctx context.Context,
	pool *pgxpool.Pool,
	sharder *StaticSlotSharder,
	fallbackBuckets int,
) (int32, bool, error) {
	if sharder == nil || pool == nil {
		return 0, false, errors.New("slot map reload: nil sharder or pool")
	}
	repo := NewSlotMapRepo(pool)
	version, err := repo.GetActiveVersion(ctx)
	if err != nil {
		return 0, false, err
	}
	if version == sharder.ActiveVersion() && sharder.ActiveVersion() != 0 {
		return version, false, nil
	}
	loaded, err := LoadActiveSlotMap(ctx, pool, sharder, fallbackBuckets)
	if err != nil {
		return loaded, true, err
	}
	return loaded, true, nil
}

// SlotMapShardTable builds the 1024-entry shard routing table for ops/edge export.
func SlotMapShardTable(rows []db.RedisSlotMap) ([]uint16, error) {
	table, err := TableFromRows(rows)
	if err != nil {
		return nil, err
	}
	out := make([]uint16, SlotCount)
	copy(out, table[:])
	return out, nil
}
