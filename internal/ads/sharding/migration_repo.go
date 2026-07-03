package sharding

import (
	"context"
	"fmt"

	"espx/internal/ads/db"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SlotMigrationRepo tracks per-slot copy/drain progress (Phase 2.3).
type SlotMigrationRepo struct {
	pool *pgxpool.Pool
}

// NewSlotMigrationRepo constructs a slot migration repository.
func NewSlotMigrationRepo(pool *pgxpool.Pool) *SlotMigrationRepo {
	return &SlotMigrationRepo{pool: pool}
}

// InsertIfAbsent registers a pending job without overwriting in-progress copy state.
func (r *SlotMigrationRepo) InsertIfAbsent(
	ctx context.Context,
	version int32,
	slot, sourceShard, targetShard int16,
) error {
	if r.pool == nil {
		return fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).InsertSlotMigrationIfAbsent(ctx, db.InsertSlotMigrationIfAbsentParams{
		Version:     version,
		Slot:        slot,
		SourceShard: sourceShard,
		TargetShard: targetShard,
	})
}

// Upsert registers or updates a migration job row.
func (r *SlotMigrationRepo) Upsert(
	ctx context.Context,
	version int32,
	slot, sourceShard, targetShard int16,
	state db.RedisSlotMigrationState,
	total, copied int32,
	lastErr string,
) error {
	if r.pool == nil {
		return fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).UpsertSlotMigration(ctx, db.UpsertSlotMigrationParams{
		Version:         version,
		Slot:            slot,
		SourceShard:     sourceShard,
		TargetShard:     targetShard,
		State:           state,
		CampaignsTotal:  total,
		CampaignsCopied: copied,
		LastError:       pgText(lastErr),
	})
}

// Get returns one migration job.
func (r *SlotMigrationRepo) Get(ctx context.Context, version int32, slot int16) (db.RedisSlotMigration, error) {
	if r.pool == nil {
		return db.RedisSlotMigration{}, fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).GetSlotMigration(ctx, db.GetSlotMigrationParams{
		Version: version,
		Slot:    slot,
	})
}

// ListByVersion returns all migration jobs for a map version.
func (r *SlotMigrationRepo) ListByVersion(ctx context.Context, version int32) ([]db.RedisSlotMigration, error) {
	if r.pool == nil {
		return nil, fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).ListSlotMigrationsByVersion(ctx, version)
}

// UpdateProgress updates copy counters and state.
func (r *SlotMigrationRepo) UpdateProgress(
	ctx context.Context,
	version int32,
	slot int16,
	total, copied int32,
	state db.RedisSlotMigrationState,
	lastErr string,
) error {
	if r.pool == nil {
		return fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).UpdateSlotMigrationProgress(ctx, db.UpdateSlotMigrationProgressParams{
		Version:         version,
		Slot:            slot,
		CampaignsTotal:  total,
		CampaignsCopied: copied,
		State:           state,
		LastError:       pgText(lastErr),
	})
}

// UpdateState sets migration state only.
func (r *SlotMigrationRepo) UpdateState(
	ctx context.Context,
	version int32,
	slot int16,
	state db.RedisSlotMigrationState,
	lastErr string,
) error {
	if r.pool == nil {
		return fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).UpdateSlotMigrationState(ctx, db.UpdateSlotMigrationStateParams{
		Version:   version,
		Slot:      slot,
		State:     state,
		LastError: pgText(lastErr),
	})
}

func pgText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

// ListDraining returns jobs awaiting old-shard key cleanup.
func (r *SlotMigrationRepo) ListDraining(ctx context.Context) ([]db.RedisSlotMigration, error) {
	if r.pool == nil {
		return nil, fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).ListDrainingSlotMigrations(ctx)
}

// ListByStates returns jobs in any of the given states.
func (r *SlotMigrationRepo) ListByStates(ctx context.Context, states []db.RedisSlotMigrationState) ([]db.RedisSlotMigration, error) {
	if r.pool == nil {
		return nil, fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).ListSlotMigrationsByState(ctx, states)
}

// GetMaxDraftVersionWithMigrating returns the highest draft version with MIGRATING slots.
func (r *SlotMigrationRepo) GetMaxDraftVersionWithMigrating(ctx context.Context) (int32, error) {
	if r.pool == nil {
		return 0, fmt.Errorf("slot migration repo: nil pool")
	}
	return db.New(r.pool).GetMaxDraftVersionWithMigrating(ctx)
}
