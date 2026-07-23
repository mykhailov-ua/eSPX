package management

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/database"
	"espx/internal/dedup"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"
	"espx/pkg/dedupkey"

	"github.com/jackc/pgx/v5"
)

// RegionOutboxRelay applies global outbox events to the local regional Redis cell.
type RegionOutboxRelay struct {
	svc        *Service
	regionCode uint8
	outbox     *OutboxWorker
}

// NewRegionOutboxRelay binds regional delivery polling to the management service.
func NewRegionOutboxRelay(svc *Service) *RegionOutboxRelay {
	code := uint8(0)
	if svc != nil && svc.cfg != nil {
		code = svc.cfg.RegionCode
	}
	return &RegionOutboxRelay{
		svc:        svc,
		regionCode: code,
		outbox:     NewOutboxWorker(svc),
	}
}

// Start polls pending regional deliveries until the context is cancelled.
func (r *RegionOutboxRelay) Start(ctx context.Context, interval time.Duration) {
	if r == nil || r.svc == nil || r.regionCode == 0 {
		return
	}
	if err := r.ProcessPending(ctx); err != nil {
		slog.Error("region outbox relay startup sync failed", "region", r.regionCode, "error", err)
	}
	slog.Info("region outbox relay starting", "region", r.regionCode, "interval", interval)

	pollBackoff := newOutboxPollBackoff()
	pollTimer := time.NewTimer(interval)
	defer pollTimer.Stop()

	recoveryTicker := time.NewTicker(interval * 5)
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-recoveryTicker.C:
			r.reclaimStaleProcessing(ctx)
		case <-pollTimer.C:
			var processed int
			var err error
			if r.svc != nil {
				err = r.svc.withPgHigh(ctx, func(runCtx context.Context) error {
					var innerErr error
					processed, innerErr = r.ProcessPendingWithCount(runCtx, 500)
					return innerErr
				})
			} else {
				processed, err = r.ProcessPendingWithCount(ctx, 500)
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if database.IsShutdownError(err) {
					return
				}
				slog.Error("region outbox relay iteration failed", "region", r.regionCode, "error", err)
				pollTimer.Reset(2 * time.Second)
				continue
			}
			pollTimer.Reset(pollBackoff.next(processed))
		}
	}
}

type regionDeliveryRow struct {
	deliveryID    int64
	outboxEventID int64
	eventType     string
	payload       []byte
	createdAt     time.Time
}

func (r *RegionOutboxRelay) reclaimStaleProcessing(ctx context.Context) {
	_, err := r.svc.GetPool().Exec(ctx, `
		UPDATE outbox_region_delivery
		SET status = 'PENDING', processing_started_at = NULL
		WHERE region_code = $1
		  AND status = 'PROCESSING'
		  AND processing_started_at IS NOT NULL
		  AND processing_started_at < NOW() - INTERVAL '1 minute'`, r.regionCode)
	if err != nil && ctx.Err() == nil && !database.IsShutdownError(err) {
		slog.Error("failed to reclaim stale region outbox deliveries", "region", r.regionCode, "error", err)
	}
}

// ProcessPending drains pending regional deliveries up to the default batch size.
func (r *RegionOutboxRelay) ProcessPending(ctx context.Context) error {
	_, err := r.ProcessPendingWithCount(ctx, 500)
	return err
}

// ProcessPendingWithCount claims, applies, and marks a batch of regional deliveries.
func (r *RegionOutboxRelay) ProcessPendingWithCount(ctx context.Context, limit int32) (int, error) {
	if r == nil || r.svc == nil || r.regionCode == 0 {
		return 0, nil
	}

	opCtx, cancel := workerContext(ctx, workerOutboxTimeout)
	defer cancel()

	var rows []regionDeliveryRow
	err := pgx.BeginFunc(opCtx, r.svc.GetPool(), func(tx pgx.Tx) error {
		qrows, err := tx.Query(opCtx, `
			SELECT d.outbox_event_id, e.event_type, e.payload, e.created_at
			FROM outbox_region_delivery d
			JOIN outbox_events e ON e.id = d.outbox_event_id
			WHERE d.region_code = $1
			  AND d.status = 'PENDING'
			ORDER BY e.created_at ASC
			LIMIT $2
			FOR UPDATE OF d SKIP LOCKED`, r.regionCode, limit)
		if err != nil {
			return err
		}
		defer qrows.Close()

		var ids []int64
		for qrows.Next() {
			var row regionDeliveryRow
			if err := qrows.Scan(&row.outboxEventID, &row.eventType, &row.payload, &row.createdAt); err != nil {
				return err
			}
			row.deliveryID = row.outboxEventID
			rows = append(rows, row)
			ids = append(ids, row.outboxEventID)
		}
		if err := qrows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		_, err = tx.Exec(opCtx, `
			UPDATE outbox_region_delivery
			SET status = 'PROCESSING', processing_started_at = NOW()
			WHERE region_code = $1 AND outbox_event_id = ANY($2)`, r.regionCode, ids)
		return err
	})
	if err != nil || len(rows) == 0 {
		return 0, err
	}

	delivered := 0
	var batchErrs []error
	for _, row := range rows {
		if err := r.applyDelivery(opCtx, ctx, row); err != nil {
			slog.Warn("region outbox apply failed", "region", r.regionCode, "event_id", row.outboxEventID, "error", err)
			_, revertErr := r.svc.GetPool().Exec(opCtx, `
				UPDATE outbox_region_delivery
				SET status = 'PENDING', processing_started_at = NULL
				WHERE region_code = $1 AND outbox_event_id = $2`, r.regionCode, row.outboxEventID)
			if revertErr != nil {
				batchErrs = append(batchErrs, revertErr)
			}
			batchErrs = append(batchErrs, fmt.Errorf("region delivery %d: %w", row.outboxEventID, err))
			continue
		}
		delivered++
	}

	if len(batchErrs) > 0 {
		return delivered, errors.Join(batchErrs...)
	}
	return delivered, nil
}

func (r *RegionOutboxRelay) applyDelivery(opCtx, ctx context.Context, row regionDeliveryRow) error {
	adapter := r.svc.dedupAdapter()
	var claim dedup.ClaimResult
	if adapter != nil {
		scope := adapter.RegionScope(dedupkey.RelaySourceID(r.regionCode), row.outboxEventID, row.outboxEventID)
		factorU := dedupkey.FactorU(dedupkey.CanonicalRelayPayload(row.outboxEventID, row.eventType, row.payload))
		var err error
		claim, err = adapter.ClaimConfirm(opCtx, scope, factorU)
		if err != nil {
			return err
		}
		if guardErr := dedup.GuardOutcome(claim); guardErr != nil {
			return guardErr
		}
		if claim.Outcome == dedup.OutcomeAlreadyConfirmed {
			already, idemErr := r.regionAlreadyApplied(opCtx, row.outboxEventID)
			if idemErr != nil {
				return idemErr
			}
			if already {
				return r.markDelivered(opCtx, row)
			}
		}
		if r.svc != nil && len(r.svc.rdbs) > 0 && claim.DedupKey != "" {
			redisKey := dedupkey.RedisKey(claim.DedupKey)
			ok, nxErr := r.svc.rdbs[0].SetNX(opCtx, redisKey, "1", 48*time.Hour).Result()
			if nxErr == nil && !ok && claim.Outcome == dedup.OutcomeConfirmed {
				already, idemErr := r.regionAlreadyApplied(opCtx, row.outboxEventID)
				if idemErr != nil {
					return idemErr
				}
				if already {
					return r.markDelivered(opCtx, row)
				}
			}
		}
	} else {
		already, err := r.regionAlreadyApplied(opCtx, row.outboxEventID)
		if err != nil {
			return err
		}
		if already {
			return r.markDelivered(opCtx, row)
		}
	}

	ev := db.OutboxEvent{
		ID:        row.outboxEventID,
		EventType: row.eventType,
		Payload:   row.payload,
	}
	if err := r.outbox.handleOutboxEvent(opCtx, ctx, ev); err != nil {
		return err
	}

	err := pgx.BeginFunc(opCtx, r.svc.GetPool(), func(tx pgx.Tx) error {
		tag, err := tx.Exec(opCtx, `
			INSERT INTO region_apply_idempotency (region_code, outbox_event_id)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, r.regionCode, row.outboxEventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		_, err = tx.Exec(opCtx, `
			UPDATE outbox_region_delivery
			SET status = 'DELIVERED', delivered_at = NOW(), processing_started_at = NULL
			WHERE region_code = $1 AND outbox_event_id = $2`, r.regionCode, row.outboxEventID)
		return err
	})
	if err != nil {
		return err
	}

	if !row.createdAt.IsZero() {
		lag := time.Since(row.createdAt).Seconds()
		if lag >= 0 {
			metrics.RegionOutboxDeliveryLag.Observe(lag)
		}
	}
	metrics.RegionOutboxDeliveredTotal.Inc()
	return nil
}

func (r *RegionOutboxRelay) regionAlreadyApplied(ctx context.Context, outboxEventID int64) (bool, error) {
	var already bool
	err := r.svc.GetPool().QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM region_apply_idempotency
			WHERE region_code = $1 AND outbox_event_id = $2
		)`, r.regionCode, outboxEventID).Scan(&already)
	return already, err
}

func (r *RegionOutboxRelay) markDelivered(ctx context.Context, row regionDeliveryRow) error {
	_, err := r.svc.GetPool().Exec(ctx, `
		UPDATE outbox_region_delivery
		SET status = 'DELIVERED', delivered_at = NOW(), processing_started_at = NULL
		WHERE region_code = $1 AND outbox_event_id = $2`, r.regionCode, row.outboxEventID)
	return err
}
