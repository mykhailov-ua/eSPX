package sharding

import (
	"context"
	"errors"
	"fmt"

	"espx/internal/ads/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// SlotCount is the fixed slot space for Fixed Slot Map (Phase 2).
	SlotCount = 1024
	// SlotMask is crc32(campaign_id) & SlotMask for slot index.
	SlotMask = SlotCount - 1
)

var (
	// ErrSlotMapIncomplete is returned when a version does not have exactly SlotCount rows.
	ErrSlotMapIncomplete = errors.New("slot map version must contain exactly 1024 slots")
	// ErrSlotMapVersionNotFound is returned when no rows exist for the requested version.
	ErrSlotMapVersionNotFound = errors.New("slot map version not found")
	// ErrSlotMapInvalidSlot is returned when slot is outside [0, 1023].
	ErrSlotMapInvalidSlot = errors.New("slot must be in [0, 1023]")
	// ErrSlotMapInvalidShard is returned when shard_id is negative.
	ErrSlotMapInvalidShard = errors.New("shard_id must be non-negative")
	// ErrSlotMapAlreadyActive is returned when activate targets the current active_version.
	ErrSlotMapAlreadyActive = errors.New("slot map version is already active")
)

// SlotOverride describes a single slot change when cloning a map version.
type SlotOverride struct {
	Slot    int16
	ShardID int16
	State   db.RedisSlotState
}

// SlotMapRepo manages Fixed Slot Map versions in Postgres (Phase 2.1 control plane).
type SlotMapRepo struct {
	pool *pgxpool.Pool
}

// NewSlotMapRepo constructs a slot map repository backed by a pgx pool.
func NewSlotMapRepo(pool *pgxpool.Pool) *SlotMapRepo {
	return &SlotMapRepo{pool: pool}
}

// GetActiveVersion returns the version trackers must load at startup.
func (r *SlotMapRepo) GetActiveVersion(ctx context.Context) (int32, error) {
	if r.pool == nil {
		return 0, fmt.Errorf("slot map repo: nil pool")
	}
	meta, err := db.New(r.pool).GetSlotMapMeta(ctx)
	if err != nil {
		return 0, err
	}
	return meta.ActiveVersion, nil
}

// ListVersion returns all slot rows for a map version ordered by slot index.
func (r *SlotMapRepo) ListVersion(ctx context.Context, version int32) ([]db.RedisSlotMap, error) {
	if r.pool == nil {
		return nil, fmt.Errorf("slot map repo: nil pool")
	}
	q := db.New(r.pool)
	count, err := q.CountSlotMapRowsForVersion(ctx, version)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrSlotMapVersionNotFound
	}
	return q.ListSlotMapByVersion(ctx, version)
}

// ListMigratingSlots returns slots in MIGRATING state for orchestrator batching (Phase 2.3).
func (r *SlotMapRepo) ListMigratingSlots(ctx context.Context, version int32) ([]db.RedisSlotMap, error) {
	if r.pool == nil {
		return nil, fmt.Errorf("slot map repo: nil pool")
	}
	return db.New(r.pool).ListMigratingSlotsByVersion(ctx, version)
}

// CreateNextVersion clones baseVersion into max(version)+1 and applies overrides atomically.
func (r *SlotMapRepo) CreateNextVersion(
	ctx context.Context,
	baseVersion int32,
	overrides []SlotOverride,
) (int32, error) {
	if r.pool == nil {
		return 0, fmt.Errorf("slot map repo: nil pool")
	}
	for _, o := range overrides {
		if err := validateSlotOverride(o); err != nil {
			return 0, err
		}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)

	if _, err := q.LockSlotMapMeta(ctx); err != nil {
		return 0, err
	}

	baseCount, err := q.CountSlotMapRowsForVersion(ctx, baseVersion)
	if err != nil {
		return 0, err
	}
	if baseCount == 0 {
		return 0, ErrSlotMapVersionNotFound
	}
	if baseCount != SlotCount {
		return 0, ErrSlotMapIncomplete
	}

	maxVersion, err := q.GetMaxSlotMapVersion(ctx)
	if err != nil {
		return 0, err
	}
	newVersion := maxVersion + 1

	if err := q.CopySlotMapVersion(ctx, db.CopySlotMapVersionParams{
		Version:   baseVersion,
		Version_2: newVersion,
	}); err != nil {
		return 0, err
	}

	for _, o := range overrides {
		if err := q.UpdateSlotMapEntry(ctx, db.UpdateSlotMapEntryParams{
			Version: newVersion,
			Slot:    o.Slot,
			ShardID: o.ShardID,
			State:   o.State,
		}); err != nil {
			return 0, err
		}
	}

	newCount, err := q.CountSlotMapRowsForVersion(ctx, newVersion)
	if err != nil {
		return 0, err
	}
	if newCount != SlotCount {
		return 0, ErrSlotMapIncomplete
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newVersion, nil
}

// MarkSlotsMigrating sets slots to MIGRATING with a target shard_id inside one transaction.
func (r *SlotMapRepo) MarkSlotsMigrating(
	ctx context.Context,
	version int32,
	slots []int16,
	targetShard int16,
) error {
	if r.pool == nil {
		return fmt.Errorf("slot map repo: nil pool")
	}
	if targetShard < 0 {
		return ErrSlotMapInvalidShard
	}
	if len(slots) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)

	versionCount, err := q.CountSlotMapRowsForVersion(ctx, version)
	if err != nil {
		return err
	}
	if versionCount == 0 {
		return ErrSlotMapVersionNotFound
	}

	for _, slot := range slots {
		if slot < 0 || slot > SlotMask {
			return ErrSlotMapInvalidSlot
		}
		if _, err := q.LockSlotMapEntry(ctx, db.LockSlotMapEntryParams{
			Version: version,
			Slot:    slot,
		}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("slot %d missing in version %d", slot, version)
			}
			return err
		}
		if err := q.UpdateSlotMapEntry(ctx, db.UpdateSlotMapEntryParams{
			Version: version,
			Slot:    slot,
			ShardID: targetShard,
			State:   db.RedisSlotStateMIGRATING,
		}); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// ActivateVersion switches the active map pointer after validating completeness.
func (r *SlotMapRepo) ActivateVersion(ctx context.Context, version int32) error {
	if r.pool == nil {
		return fmt.Errorf("slot map repo: nil pool")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)

	if _, err := q.LockSlotMapMeta(ctx); err != nil {
		return err
	}

	count, err := q.CountSlotMapRowsForVersion(ctx, version)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrSlotMapVersionNotFound
	}
	if count != SlotCount {
		return ErrSlotMapIncomplete
	}

	if err := q.SetSlotMapActiveVersion(ctx, version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// TableFromRows builds a [1024]uint16 shard table from Postgres rows (cold path only).
func TableFromRows(rows []db.RedisSlotMap) (*[SlotCount]uint16, error) {
	if len(rows) != SlotCount {
		return nil, ErrSlotMapIncomplete
	}
	var table [SlotCount]uint16
	for _, row := range rows {
		if row.Slot < 0 || row.Slot > SlotMask {
			return nil, fmt.Errorf("invalid slot %d in map row", row.Slot)
		}
		table[row.Slot] = uint16(row.ShardID)
	}
	return &table, nil
}

func validateSlotOverride(o SlotOverride) error {
	if o.Slot < 0 || o.Slot > SlotMask {
		return ErrSlotMapInvalidSlot
	}
	if o.ShardID < 0 {
		return ErrSlotMapInvalidShard
	}
	switch o.State {
	case db.RedisSlotStateACTIVE, db.RedisSlotStateMIGRATING, db.RedisSlotStateDRAINING:
		return nil
	default:
		return fmt.Errorf("invalid slot state %q", o.State)
	}
}
