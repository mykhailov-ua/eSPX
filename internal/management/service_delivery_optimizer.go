package management

import (
	"context"
	"fmt"
	"log/slog"

	"espx/internal/ingestion"
	"espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	outboxPriSyncBrandCreatives = 1
	outboxPriCreateCampaign     = 2
	outboxPriPacing             = 3
	outboxPriPause              = 4
)

type deliveryOutboxEntry struct {
	priority  int
	eventType string
	payload   []byte
}

// deliveryOutboxMerge deduplicates outbox side effects to at most one event per campaign per optimizer tick (M5.0).
type deliveryOutboxMerge map[uuid.UUID]deliveryOutboxEntry

func (m deliveryOutboxMerge) upsert(campaignID uuid.UUID, priority int, eventType string, payload []byte) {
	if m == nil {
		return
	}
	if existing, ok := m[campaignID]; ok && existing.priority >= priority {
		return
	}
	m[campaignID] = deliveryOutboxEntry{
		priority:  priority,
		eventType: eventType,
		payload:   payload,
	}
}

func (m deliveryOutboxMerge) flush(ctx context.Context, pool pgx.Tx) error {
	if len(m) == 0 {
		return nil
	}
	q := db.New(pool)
	for _, entry := range m {
		if _, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: entry.eventType,
			Payload:   entry.payload,
		}); err != nil {
			return fmt.Errorf("flush delivery optimizer outbox %s: %w", entry.eventType, err)
		}
	}
	return nil
}

// RunDeliveryOptimizerTick is the unified M5.0 delivery pass: sync, pacing, autoscale, MAB, bid floors.
func (s *Service) RunDeliveryOptimizerTick(ctx context.Context, syncWorkers []*ingestion.SyncWorker, runMAB bool) error {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	for _, sw := range syncWorkers {
		if sw != nil {
			sw.SyncAll(opCtx)
		}
	}

	merge := make(deliveryOutboxMerge)
	var mabBrands []uuid.UUID

	err := pgx.BeginFunc(opCtx, s.GetPool(), func(tx pgx.Tx) error {
		if err := s.closedLoopPacingControllerTx(opCtx, tx, merge); err != nil {
			return err
		}
		if err := s.autoscaleBudgetsTx(opCtx, tx, merge); err != nil {
			return err
		}
		if runMAB {
			brands, err := s.optimizeBrandCreativeMABTx(opCtx, tx)
			if err != nil {
				return err
			}
			mabBrands = brands
		}
		if err := merge.flush(opCtx, tx); err != nil {
			return err
		}
		for _, brandID := range mabBrands {
			if err := s.emitBrandCreativesOutbox(opCtx, db.New(tx), brandID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if _, err := s.OptimizeBidFloors(opCtx); err != nil {
		slog.Error("bid floor optimizer failed", "error", err)
	}
	return nil
}
