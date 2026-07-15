package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ingestion"
)

// SlotMigrationOrchestrator copies MIGRATING slot data and drains old keys after cutover (Phase 2.3).
type SlotMigrationOrchestrator struct {
	svc      *Service
	interval time.Duration
}

// NewSlotMigrationOrchestrator constructs the slot migration background worker.
func NewSlotMigrationOrchestrator(svc *Service, interval time.Duration) *SlotMigrationOrchestrator {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &SlotMigrationOrchestrator{svc: svc, interval: interval}
}

// Start runs copy and drain ticks until ctx is cancelled.
func (o *SlotMigrationOrchestrator) Start(ctx context.Context) {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.tick(ctx)
		}
	}
}

func (o *SlotMigrationOrchestrator) tick(ctx context.Context) {
	migRepo := ingestion.NewSlotMigrationRepo(o.svc.GetPool())
	draft, err := migRepo.GetMaxDraftVersionWithMigrating(ctx)
	if err != nil {
		slog.Error("slot migration: draft version lookup failed", "error", err)
		if o.svc.alerter != nil {
			o.svc.alerter.AlertSlotMigrationError("draft_lookup", err)
		}
		return
	}
	if draft > 0 {
		if err := o.svc.CopyAllMigratingSlots(ctx, draft); err != nil {
			slog.Warn("slot migration copy tick", "version", draft, "error", err)
			if o.svc.alerter != nil {
				o.svc.alerter.AlertSlotMigrationError("copy", err)
			}
		}
	}

	mapRepo := ingestion.NewSlotMapRepo(o.svc.GetPool())
	active, err := mapRepo.GetActiveVersion(ctx)
	if err != nil {
		slog.Error("slot migration: active version lookup failed", "error", err)
		if o.svc.alerter != nil {
			o.svc.alerter.AlertSlotMigrationError("active_lookup", err)
		}
		return
	}
	if err := o.svc.DrainMigratingSlots(ctx, active); err != nil {
		slog.Warn("slot migration drain tick", "version", active, "error", err)
		if o.svc.alerter != nil {
			o.svc.alerter.AlertSlotMigrationError("drain", err)
		}
	} else {
		pending, pendErr := o.svc.HasPendingSlotDrain(ctx)
		if pendErr == nil && !pending {
			if r5Err := o.svc.VerifySlotMigrationR5(ctx); r5Err != nil && o.svc.alerter != nil {
				o.svc.alerter.AlertSlotMigrationError("r5_verify", r5Err)
			}
		}
	}
	o.svc.CheckStuckDrainJobs(ctx)
}
