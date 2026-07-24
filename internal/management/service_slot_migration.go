package management

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrSlotMigrationNotReady is returned when activate is attempted before copy completes.
	ErrSlotMigrationNotReady = errors.New("slot migration copy not complete for all MIGRATING slots")
	// ErrSlotMigrationNoDraft is returned when no draft version with MIGRATING slots exists.
	ErrSlotMigrationNoDraft = errors.New("no draft slot map version with MIGRATING slots")
	// ErrSlotMigrationKeysMissing is returned when required keys are absent on the target shard after PG re-warm.
	ErrSlotMigrationKeysMissing = errors.New("slot migration target shard missing required keys")
	// ErrSlotMigrationLagNotCaughtUp is returned when dual-write replication lag exceeds epsilon at cutover.
	ErrSlotMigrationLagNotCaughtUp = errors.New("slot migration dual-write lag above epsilon")
)

// SlotMigrationDTO is the admin view of one slot migration job.
type SlotMigrationDTO struct {
	Version         int32  `json:"version"`
	Slot            int16  `json:"slot"`
	SourceShard     int16  `json:"source_shard"`
	TargetShard     int16  `json:"target_shard"`
	State           string `json:"state"`
	CampaignsTotal  int32  `json:"campaigns_total"`
	CampaignsCopied int32  `json:"campaigns_copied"`
	LastError       string `json:"last_error,omitempty"`
}

// GetSlotMigrations returns migration progress for a map version.
func (s *Service) GetSlotMigrations(ctx context.Context, version int32) ([]SlotMigrationDTO, error) {
	repo := ingestion.NewSlotMigrationRepo(s.GetPool())
	rows, err := repo.ListByVersion(ctx, version)
	if err != nil {
		return nil, err
	}
	out := make([]SlotMigrationDTO, 0, len(rows))
	for _, row := range rows {
		out = append(out, slotMigrationToDTO(row))
	}
	return out, nil
}

// EnsureSlotMigrationJobs registers pending jobs for MIGRATING slots in a draft version.
func (s *Service) EnsureSlotMigrationJobs(ctx context.Context, draftVersion int32) error {
	mapRepo := ingestion.NewSlotMapRepo(s.GetPool())
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())

	active, err := mapRepo.GetActiveVersion(ctx)
	if err != nil {
		return err
	}
	if draftVersion <= active {
		return fmt.Errorf("draft version %d must be greater than active %d", draftVersion, active)
	}

	activeRows, err := mapRepo.ListVersion(ctx, active)
	if err != nil {
		return err
	}
	sourceBySlot := make(map[int16]int16, len(activeRows))
	for _, row := range activeRows {
		sourceBySlot[row.Slot] = row.ShardID
	}

	migrating, err := mapRepo.ListMigratingSlots(ctx, draftVersion)
	if err != nil {
		return err
	}
	for _, row := range migrating {
		source, ok := sourceBySlot[row.Slot]
		if !ok {
			return fmt.Errorf("slot %d missing in active map", row.Slot)
		}
		if source == row.ShardID {
			return fmt.Errorf("slot %d source and target shard are both %d", row.Slot, source)
		}
		if err := migRepo.InsertIfAbsent(ctx, draftVersion, row.Slot, source, row.ShardID); err != nil {
			return err
		}
	}
	return nil
}

// CopySlotMigrationData copies Redis keys for one MIGRATING slot (idempotent).
func (s *Service) CopySlotMigrationData(ctx context.Context, version int32, slot int16) error {
	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis shards configured")
	}
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())
	job, err := migRepo.Get(ctx, version, slot)
	if err != nil {
		return err
	}
	if job.State == db.RedisSlotMigrationStateCopied ||
		job.State == db.RedisSlotMigrationStateDualWriting ||
		job.State == db.RedisSlotMigrationStateDraining ||
		job.State == db.RedisSlotMigrationStateDone {
		return nil
	}
	if job.SourceShard < 0 || int(job.SourceShard) >= len(s.rdbs) ||
		job.TargetShard < 0 || int(job.TargetShard) >= len(s.rdbs) {
		return fmt.Errorf("invalid shard indices source=%d target=%d", job.SourceShard, job.TargetShard)
	}

	campaignIDs, err := s.listActiveCampaignUUIDs(ctx)
	if err != nil {
		return err
	}
	slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, slot)
	total := int32(len(slotCampaigns))

	if err := migRepo.UpdateProgress(ctx, version, slot, total, job.CampaignsCopied,
		db.RedisSlotMigrationStateCopying, ""); err != nil {
		return err
	}

	src := s.rdbs[job.SourceShard]
	dst := s.rdbs[job.TargetShard]

	if s.cfg != nil && s.cfg.MigrationFenceEnabled && !s.slotMigrationDualWriteEnabled() && len(slotCampaigns) > 0 {
		if err := ingestion.BumpMigrationFences(ctx, s.GetPool(), src, slotCampaigns); err != nil {
			_ = migRepo.UpdateProgress(ctx, version, slot, total, job.CampaignsCopied,
				db.RedisSlotMigrationStateFailed, err.Error())
			return fmt.Errorf("migration fence: %w", err)
		}
	}

	migrator := &ingestion.CampaignKeyMigrator{}
	var copied int32
	for _, id := range slotCampaigns {
		if _, err := migrator.MigrateCampaignKeys(ctx, src, dst, id); err != nil {
			_ = migRepo.UpdateProgress(ctx, version, slot, total, copied,
				db.RedisSlotMigrationStateFailed, err.Error())
			return fmt.Errorf("copy campaign %s: %w", id, err)
		}
		copied++
		if copied%10 == 0 || copied == total {
			if err := migRepo.UpdateProgress(ctx, version, slot, total, copied,
				db.RedisSlotMigrationStateCopying, ""); err != nil {
				return err
			}
		}
	}

	catalog := ingestion.DefaultCampaignRedisKeyCatalog
	for _, id := range slotCampaigns {
		for _, key := range catalog.ActivationRequiredKeys(id) {
			srcExists, err := src.Exists(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("post-copy exists src %q: %w", key, err)
			}
			if srcExists == 0 {
				continue
			}
			dstExists, err := dst.Exists(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("post-copy exists dst %q: %w", key, err)
			}
			if dstExists == 0 {
				_ = migRepo.UpdateProgress(ctx, version, slot, total, copied,
					db.RedisSlotMigrationStateFailed, "missing key on target: "+key)
				return fmt.Errorf("post-copy verify: %q missing on target shard", key)
			}
		}
	}

	finalState := db.RedisSlotMigrationStateCopied
	if s.slotMigrationDualWriteEnabled() {
		if err := ingestion.EnableSlotMigrationDualWrite(ctx, src, version, slot, job.TargetShard); err != nil {
			_ = migRepo.UpdateProgress(ctx, version, slot, total, copied,
				db.RedisSlotMigrationStateFailed, err.Error())
			return fmt.Errorf("enable dual-write slot %d: %w", slot, err)
		}
		finalState = db.RedisSlotMigrationStateDualWriting
	}

	return migRepo.UpdateProgress(ctx, version, slot, total, copied,
		finalState, "")
}

// CopyAllMigratingSlots copies data for every pending/copying slot in a draft version.
func (s *Service) CopyAllMigratingSlots(ctx context.Context, draftVersion int32) error {
	if err := s.EnsureSlotMigrationJobs(ctx, draftVersion); err != nil {
		return err
	}
	mapRepo := ingestion.NewSlotMapRepo(s.GetPool())
	migrating, err := mapRepo.ListMigratingSlots(ctx, draftVersion)
	if err != nil {
		return err
	}
	for _, row := range migrating {
		if err := s.CopySlotMigrationData(ctx, draftVersion, row.Slot); err != nil {
			slog.Error("slot migration copy failed", "version", draftVersion, "slot", row.Slot, "error", err)
			return err
		}
	}
	return nil
}

// ActivateSlotMapVersionWithMigration validates copy completion, cutovers MIGRATING slots, activates, and starts drain.
func (s *Service) ActivateSlotMapVersionWithMigration(ctx context.Context, adminID uuid.UUID, version int32) error {
	mapRepo := ingestion.NewSlotMapRepo(s.GetPool())
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())

	migrating, err := mapRepo.ListMigratingSlots(ctx, version)
	if err != nil {
		return err
	}
	campaignIDs, err := s.listActiveCampaignUUIDs(ctx)
	if err != nil {
		return err
	}
	catalog := ingestion.DefaultCampaignRedisKeyCatalog

	if len(migrating) > 0 {
		if err := s.EnsureSlotMigrationJobs(ctx, version); err != nil {
			return err
		}
		for _, row := range migrating {
			job, err := migRepo.Get(ctx, version, row.Slot)
			if err != nil {
				return err
			}
			skipRewarm := false
			if job.State == db.RedisSlotMigrationStateDualWriting {
				if err := s.finalizeDualWriteSlot(ctx, version, job, campaignIDs); err != nil {
					return err
				}
				skipRewarm = true
				job, err = migRepo.Get(ctx, version, row.Slot)
				if err != nil {
					return err
				}
			}
			if job.State != db.RedisSlotMigrationStateCopied {
				return ErrSlotMigrationNotReady
			}
			if job.TargetShard < 0 || int(job.TargetShard) >= len(s.rdbs) {
				return fmt.Errorf("invalid target shard %d for slot %d", job.TargetShard, row.Slot)
			}
			slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, row.Slot)
			dst := s.rdbs[job.TargetShard]
			if !skipRewarm {
				if err := ingestion.RewarmCampaignBudgetKeys(ctx, s.GetPool(), dst, slotCampaigns); err != nil {
					return fmt.Errorf("pg re-warm slot %d: %w", row.Slot, err)
				}
			}
			if err := catalog.VerifySlotCampaignKeysExist(ctx, dst, slotCampaigns); err != nil {
				metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("missing_keys").Inc()
				return fmt.Errorf("%w: %v", ErrSlotMigrationKeysMissing, err)
			}
		}
	}

	tx, err := s.GetPool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)
	meta, err := q.LockSlotMapMeta(ctx)
	if err != nil {
		return err
	}
	if meta.ActiveVersion == version {
		return ingestion.ErrSlotMapAlreadyActive
	}
	if meta.ActiveVersion > version {
		return fmt.Errorf("slot map version %d is older than active %d", version, meta.ActiveVersion)
	}
	count, err := q.CountSlotMapRowsForVersion(ctx, version)
	if err != nil {
		return err
	}
	if count == 0 {
		return ingestion.ErrSlotMapVersionNotFound
	}
	if count != ingestion.SlotCount {
		return ingestion.ErrSlotMapIncomplete
	}

	for _, row := range migrating {
		if err := q.UpdateSlotMapEntry(ctx, db.UpdateSlotMapEntryParams{
			Version: version,
			Slot:    row.Slot,
			ShardID: row.ShardID,
			State:   db.RedisSlotStateDRAINING,
		}); err != nil {
			return err
		}
		if err := q.UpdateSlotMigrationState(ctx, db.UpdateSlotMigrationStateParams{
			Version: version,
			Slot:    row.Slot,
			State:   db.RedisSlotMigrationStateDraining,
		}); err != nil {
			return err
		}
	}

	if err := q.SetSlotMapActiveVersion(ctx, version); err != nil {
		return err
	}

	s.AuditLog(ctx, q, adminID, "SLOT_MAP_ACTIVATED", "redis_slot_map", nil, map[string]any{
		"version":           version,
		"migrated_slots":    len(migrating),
		"migration_cutover": true,
	}, nil)

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.afterSlotMapActivated(ctx, version)
	return nil
}

// DrainMigratingSlots deletes stale keys on source shards for DRAINING slots on the active map.
func (s *Service) DrainMigratingSlots(ctx context.Context, version int32) error {
	if len(s.rdbs) == 0 {
		return fmt.Errorf("no redis shards configured")
	}
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())
	jobs, err := migRepo.ListDraining(ctx)
	if err != nil {
		return err
	}
	mapRepo := ingestion.NewSlotMapRepo(s.GetPool())
	active, err := mapRepo.GetActiveVersion(ctx)
	if err != nil {
		return err
	}
	if version != 0 && version != active {
		return fmt.Errorf("drain requested for version %d but active is %d", version, active)
	}

	campaignIDs, err := s.listActiveCampaignUUIDs(ctx)
	if err != nil {
		return err
	}
	migrator := &ingestion.CampaignKeyMigrator{}

	for _, job := range jobs {
		if job.Version != active {
			continue
		}
		if job.SourceShard < 0 || int(job.SourceShard) >= len(s.rdbs) {
			continue
		}
		src := s.rdbs[job.SourceShard]
		slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, job.Slot)
		for _, id := range slotCampaigns {
			if _, err := migrator.DrainCampaignKeys(ctx, src, id); err != nil {
				_ = migRepo.UpdateState(ctx, job.Version, job.Slot,
					db.RedisSlotMigrationStateFailed, err.Error())
				return fmt.Errorf("drain campaign %s slot %d: %w", id, job.Slot, err)
			}
		}
		if err := migRepo.UpdateState(ctx, job.Version, job.Slot,
			db.RedisSlotMigrationStateDone, ""); err != nil {
			return err
		}
		if err := mapRepoUpdateSlotState(ctx, s.GetPool(), job.Version, job.Slot,
			job.TargetShard, db.RedisSlotStateACTIVE); err != nil {
			return err
		}
	}
	return nil
}

// RollbackSlotMapVersion reverts active_version to a previous map and broadcasts reload (Phase 2.3.5).
func (s *Service) RollbackSlotMapVersion(ctx context.Context, adminID uuid.UUID, previousVersion int32) error {
	tx, err := s.GetPool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := db.New(tx)
	meta, err := q.LockSlotMapMeta(ctx)
	if err != nil {
		return err
	}
	if previousVersion >= meta.ActiveVersion {
		return fmt.Errorf("rollback target %d must be less than active %d", previousVersion, meta.ActiveVersion)
	}
	count, err := q.CountSlotMapRowsForVersion(ctx, previousVersion)
	if err != nil {
		return err
	}
	if count != ingestion.SlotCount {
		return ingestion.ErrSlotMapIncomplete
	}
	if err := q.SetSlotMapActiveVersion(ctx, previousVersion); err != nil {
		return err
	}
	s.AuditLog(ctx, q, adminID, "SLOT_MAP_ROLLBACK", "redis_slot_map", nil, map[string]any{
		"from_version": meta.ActiveVersion,
		"to_version":   previousVersion,
	}, nil)
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.afterSlotMapActivated(ctx, previousVersion)
	return nil
}

// CatchUpDualWriteSlots drains replication streams for slots in dual_writing state.
func (s *Service) CatchUpDualWriteSlots(ctx context.Context, draftVersion int32) error {
	if !s.slotMigrationDualWriteEnabled() || len(s.rdbs) == 0 {
		return nil
	}
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())
	jobs, err := migRepo.ListByVersion(ctx, draftVersion)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.State != db.RedisSlotMigrationStateDualWriting {
			continue
		}
		if job.SourceShard < 0 || int(job.SourceShard) >= len(s.rdbs) ||
			job.TargetShard < 0 || int(job.TargetShard) >= len(s.rdbs) {
			continue
		}
		src := s.rdbs[job.SourceShard]
		dst := s.rdbs[job.TargetShard]
		_, lag, err := ingestion.CatchUpSlotMigrationDeltas(ctx, src, dst, job.Version, job.Slot)
		if err != nil {
			return fmt.Errorf("catch-up slot %d: %w", job.Slot, err)
		}
		cfg := s.dualWriteConfig()
		if lag > cfg.LagThreshold {
			slotCampaigns, listErr := s.listActiveCampaignUUIDs(ctx)
			if listErr != nil {
				return listErr
			}
			slotCampaigns = ingestion.FilterCampaignIDsBySlot(slotCampaigns, job.Slot)
			if s.cfg != nil && s.cfg.MigrationFenceEnabled && len(slotCampaigns) > 0 {
				if fenceErr := ingestion.BumpMigrationFences(ctx, s.GetPool(), src, slotCampaigns); fenceErr != nil {
					return fmt.Errorf("dual-write fence fallback slot %d: %w", job.Slot, fenceErr)
				}
			}
			_ = ingestion.DisableSlotMigrationDualWrite(ctx, src)
			metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("lag_threshold").Inc()
			slog.Warn("slot migration dual-write lag exceeded threshold; fence fallback",
				"version", job.Version, "slot", job.Slot, "lag", lag, "threshold", cfg.LagThreshold)
		}
	}
	return nil
}

func (s *Service) finalizeDualWriteSlot(
	ctx context.Context,
	version int32,
	job db.RedisSlotMigration,
	campaignIDs []uuid.UUID,
) error {
	if job.SourceShard < 0 || int(job.SourceShard) >= len(s.rdbs) ||
		job.TargetShard < 0 || int(job.TargetShard) >= len(s.rdbs) {
		return fmt.Errorf("invalid shard indices source=%d target=%d", job.SourceShard, job.TargetShard)
	}
	src := s.rdbs[job.SourceShard]
	dst := s.rdbs[job.TargetShard]
	cfg := s.dualWriteConfig()

	lag, err := ingestion.SlotMigrationReplicationLag(ctx, src)
	if err != nil {
		return err
	}
	if lag > cfg.LagThreshold {
		slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, job.Slot)
		if s.cfg != nil && s.cfg.MigrationFenceEnabled && len(slotCampaigns) > 0 {
			if fenceErr := ingestion.BumpMigrationFences(ctx, s.GetPool(), src, slotCampaigns); fenceErr != nil {
				return fmt.Errorf("dual-write fence fallback slot %d: %w", job.Slot, fenceErr)
			}
		}
		_ = ingestion.DisableSlotMigrationDualWrite(ctx, src)
		metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("lag_threshold").Inc()
		return fmt.Errorf("dual-write lag %d exceeds threshold %d for slot %d", lag, cfg.LagThreshold, job.Slot)
	}
	if lag > cfg.LagEpsilon {
		metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("lag_epsilon").Inc()
		return fmt.Errorf("%w: slot %d lag %d epsilon %d", ErrSlotMigrationLagNotCaughtUp, job.Slot, lag, cfg.LagEpsilon)
	}

	_, lag, err = ingestion.CatchUpSlotMigrationDeltas(ctx, src, dst, version, job.Slot)
	if err != nil {
		return err
	}
	if lag > cfg.LagEpsilon {
		metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("lag_epsilon").Inc()
		return fmt.Errorf("%w: slot %d lag %d epsilon %d", ErrSlotMigrationLagNotCaughtUp, job.Slot, lag, cfg.LagEpsilon)
	}
	slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, job.Slot)
	if len(slotCampaigns) > 0 {
		if err := ingestion.VerifyBudgetInvariant(ctx, s.GetPool(), dst, slotCampaigns[0]); err != nil {
			metrics.SlotMigrationCutoverBlockedTotal.WithLabelValues("invariant").Inc()
			return err
		}
	}
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())
	if err := migRepo.UpdateState(ctx, version, job.Slot, db.RedisSlotMigrationStateCopied, ""); err != nil {
		return err
	}
	return ingestion.DisableSlotMigrationDualWrite(ctx, src)
}

func (s *Service) slotMigrationDualWriteEnabled() bool {
	return s.cfg != nil && s.cfg.SlotMigrationDualWriteEnabled
}

func (s *Service) dualWriteConfig() ingestion.SlotMigrationDualWriteConfig {
	cfg := ingestion.SlotMigrationDualWriteConfig{
		Enabled:      s.slotMigrationDualWriteEnabled(),
		LagEpsilon:   0,
		LagThreshold: 1000,
	}
	if s.cfg == nil {
		return cfg
	}
	cfg.LagEpsilon = s.cfg.SlotMigrationLagEpsilon
	cfg.LagThreshold = s.cfg.SlotMigrationLagThreshold
	if cfg.LagThreshold <= 0 {
		cfg.LagThreshold = 1000
	}
	return cfg
}

// BumpFencesForPendingMigrations bumps migration_gen and Redis fence keys for campaigns in active copy jobs.
func (s *Service) BumpFencesForPendingMigrations(ctx context.Context) error {
	if s.cfg == nil || !s.cfg.MigrationFenceEnabled || len(s.rdbs) == 0 {
		return nil
	}
	migRepo := ingestion.NewSlotMigrationRepo(s.GetPool())
	draft, err := migRepo.GetMaxDraftVersionWithMigrating(ctx)
	if err != nil || draft <= 0 {
		return err
	}
	jobs, err := migRepo.ListByVersion(ctx, draft)
	if err != nil {
		return err
	}
	campaignIDs, err := s.listActiveCampaignUUIDs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.State == db.RedisSlotMigrationStateCopied ||
			job.State == db.RedisSlotMigrationStateDualWriting ||
			job.State == db.RedisSlotMigrationStateDraining ||
			job.State == db.RedisSlotMigrationStateDone {
			continue
		}
		if job.SourceShard < 0 || int(job.SourceShard) >= len(s.rdbs) {
			continue
		}
		slotCampaigns := ingestion.FilterCampaignIDsBySlot(campaignIDs, job.Slot)
		if len(slotCampaigns) == 0 {
			continue
		}
		src := s.rdbs[job.SourceShard]
		if err := ingestion.BumpMigrationFences(ctx, s.GetPool(), src, slotCampaigns); err != nil {
			return fmt.Errorf("bump fences slot %d: %w", job.Slot, err)
		}
	}
	return nil
}

func (s *Service) listActiveCampaignUUIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := db.New(s.GetPool()).ListCampaignIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]uuid.UUID, 0, len(rows))
	for _, row := range rows {
		if !row.Valid {
			continue
		}
		out = append(out, uuid.UUID(row.Bytes))
	}
	return out, nil
}

func slotMigrationToDTO(row db.RedisSlotMigration) SlotMigrationDTO {
	dto := SlotMigrationDTO{
		Version:         row.Version,
		Slot:            row.Slot,
		SourceShard:     row.SourceShard,
		TargetShard:     row.TargetShard,
		State:           string(row.State),
		CampaignsTotal:  row.CampaignsTotal,
		CampaignsCopied: row.CampaignsCopied,
	}
	if row.LastError.Valid {
		dto.LastError = row.LastError.String
	}
	return dto
}

func mapRepoUpdateSlotState(ctx context.Context, pool *pgxpool.Pool, version int32, slot, shard int16, state db.RedisSlotState) error {
	if pool == nil {
		return fmt.Errorf("nil pool")
	}
	return db.New(pool).UpdateSlotMapEntry(ctx, db.UpdateSlotMapEntryParams{
		Version: version,
		Slot:    slot,
		ShardID: shard,
		State:   state,
	})
}
