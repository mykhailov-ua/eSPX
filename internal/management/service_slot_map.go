package management

import (
	"context"
	"fmt"
	"log/slog"

	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
)

// SlotMapDTO is the admin API view of one slot row.
type SlotMapDTO struct {
	Slot    int16  `json:"slot"`
	ShardID int16  `json:"shard_id"`
	State   string `json:"state"`
}

// SlotMapVersionDTO summarizes a slot map version for admin responses.
type SlotMapVersionDTO struct {
	Version        int32        `json:"version"`
	ActiveVersion  int32        `json:"active_version"`
	SlotCount      int32        `json:"slot_count"`
	MigratingCount int          `json:"migrating_count"`
	Slots          []SlotMapDTO `json:"slots,omitempty"`
}

// GetSlotMap returns the active or requested slot map version.
func (s *Service) GetSlotMap(ctx context.Context, version *int32, includeSlots bool) (SlotMapVersionDTO, error) {
	repo := ingestion.NewSlotMapRepo(s.GetPool())
	active, err := repo.GetActiveVersion(ctx)
	if err != nil {
		return SlotMapVersionDTO{}, err
	}

	target := active
	if version != nil {
		target = *version
	}

	rows, err := repo.ListVersion(ctx, target)
	if err != nil {
		return SlotMapVersionDTO{}, err
	}

	dto := SlotMapVersionDTO{
		Version:       target,
		ActiveVersion: active,
		SlotCount:     int32(len(rows)),
	}
	migrating, err := repo.ListMigratingSlots(ctx, target)
	if err != nil {
		return SlotMapVersionDTO{}, err
	}
	dto.MigratingCount = len(migrating)

	if includeSlots {
		dto.Slots = make([]SlotMapDTO, 0, len(rows))
		for _, row := range rows {
			dto.Slots = append(dto.Slots, slotRowToDTO(row))
		}
	}
	return dto, nil
}

// CreateSlotMapVersion clones base (or active) into max(version)+1 with overrides; audit in same tx.
func (s *Service) CreateSlotMapVersion(ctx context.Context, adminID uuid.UUID, baseVersion *int32, overrides []ingestion.SlotOverride) (int32, error) {
	base := int32(0)
	if baseVersion != nil {
		base = *baseVersion
	} else {
		active, err := ingestion.NewSlotMapRepo(s.GetPool()).GetActiveVersion(ctx)
		if err != nil {
			return 0, err
		}
		base = active
	}

	for _, o := range overrides {
		if o.Slot < 0 || o.Slot > ingestion.SlotMask || o.ShardID < 0 {
			return 0, fmt.Errorf("invalid slot override: slot=%d shard=%d", o.Slot, o.ShardID)
		}
	}

	tx, err := s.GetPool().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)
	if _, err := q.LockSlotMapMeta(ctx); err != nil {
		return 0, err
	}

	baseCount, err := q.CountSlotMapRowsForVersion(ctx, base)
	if err != nil {
		return 0, err
	}
	if baseCount == 0 {
		return 0, ingestion.ErrSlotMapVersionNotFound
	}
	if baseCount != ingestion.SlotCount {
		return 0, ingestion.ErrSlotMapIncomplete
	}

	maxVersion, err := q.GetMaxSlotMapVersion(ctx)
	if err != nil {
		return 0, err
	}
	newVersion := maxVersion + 1

	if err := q.CopySlotMapVersion(ctx, db.CopySlotMapVersionParams{
		Version:   base,
		Version_2: newVersion,
	}); err != nil {
		return 0, err
	}
	for _, o := range overrides {
		state := o.State
		if state == "" {
			state = db.RedisSlotStateACTIVE
		}
		if err := q.UpdateSlotMapEntry(ctx, db.UpdateSlotMapEntryParams{
			Version: newVersion,
			Slot:    o.Slot,
			ShardID: o.ShardID,
			State:   state,
		}); err != nil {
			return 0, err
		}
	}
	newCount, err := q.CountSlotMapRowsForVersion(ctx, newVersion)
	if err != nil {
		return 0, err
	}
	if newCount != ingestion.SlotCount {
		return 0, ingestion.ErrSlotMapIncomplete
	}

	s.AuditLog(ctx, q, adminID, "SLOT_MAP_VERSION_CREATED", "redis_slot_map", nil, map[string]any{
		"base_version": base,
		"new_version":  newVersion,
		"overrides":    overrides,
	}, nil)

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newVersion, nil
}

// MarkSlotMapMigrating marks slots MIGRATING on a draft version with audit log in the same tx.
func (s *Service) MarkSlotMapMigrating(ctx context.Context, adminID uuid.UUID, version int32, slots []int16, targetShard int16) error {
	if targetShard < 0 {
		return ingestion.ErrSlotMapInvalidShard
	}

	tx, err := s.GetPool().Begin(ctx)
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
		return ingestion.ErrSlotMapVersionNotFound
	}

	for _, slot := range slots {
		if slot < 0 || slot > ingestion.SlotMask {
			return ingestion.ErrSlotMapInvalidSlot
		}
		if _, err := q.LockSlotMapEntry(ctx, db.LockSlotMapEntryParams{
			Version: version,
			Slot:    slot,
		}); err != nil {
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

	s.AuditLog(ctx, q, adminID, "SLOT_MAP_MARK_MIGRATING", "redis_slot_map", nil, map[string]any{
		"version":      version,
		"slots":        slots,
		"target_shard": targetShard,
	}, nil)

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.alerter != nil {
		s.alerter.AlertSlotMapMigrating(version, slots, targetShard)
	}
	return nil
}

// ActivateSlotMapVersion switches active_version after validation with audit log in the same tx.
// When the version has MIGRATING slots, copy must be complete before cutover (Phase 2.3).
func (s *Service) ActivateSlotMapVersion(ctx context.Context, adminID uuid.UUID, version int32) error {
	return s.ActivateSlotMapVersionWithMigration(ctx, adminID, version)
}

func (s *Service) afterSlotMapActivated(ctx context.Context, version int32) {
	routingEpoch := int64(0)
	if row, err := ingestion.NewCampaignRoutingRepo(s.GetPool()).BumpGlobalRoutingEpoch(ctx); err == nil {
		routingEpoch = row.RoutingEpoch
		version = row.ActiveVersion
	}
	if ss, ok := s.sharder.(*ingestion.StaticSlotSharder); ok {
		if _, err := ingestion.LoadActiveSlotMap(ctx, s.GetPool(), ss, len(s.rdbs)); err != nil {
			slog.Warn("management slot map reload after activate failed", "error", err)
		}
	}
	s.publishRoutingCutover(ctx, routingEpoch, version)
}

func slotRowToDTO(row db.RedisSlotMap) SlotMapDTO {
	return SlotMapDTO{
		Slot:    row.Slot,
		ShardID: row.ShardID,
		State:   string(row.State),
	}
}
