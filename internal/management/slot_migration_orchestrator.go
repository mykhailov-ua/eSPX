package management

import (
	"context"
	"espx/internal/ads/sharding"
	"log/slog"
	"time"
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
	migRepo := sharding.NewSlotMigrationRepo(o.svc.GetPool())
	draft, err := migRepo.GetMaxDraftVersionWithMigrating(ctx)
	if err != nil {
		slog.Error("slot migration: draft version lookup failed", "error", err)
		return
	}
	if draft > 0 {
		if err := o.svc.CopyAllMigratingSlots(ctx, draft); err != nil {
			slog.Warn("slot migration copy tick", "version", draft, "error", err)
		}
	}

	mapRepo := sharding.NewSlotMapRepo(o.svc.GetPool())
	active, err := mapRepo.GetActiveVersion(ctx)
	if err != nil {
		slog.Error("slot migration: active version lookup failed", "error", err)
		return
	}
	if err := o.svc.DrainMigratingSlots(ctx, active); err != nil {
		slog.Warn("slot migration drain tick", "version", active, "error", err)
	}
}
