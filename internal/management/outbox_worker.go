package management

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// OutboxWorker implements a high-performance Hybrid CDC-like Transactional Outbox pattern.
// It leverages PostgreSQL LISTEN/NOTIFY for real-time push events to completely eliminate SQL polling database overhead,
// combined with a Decoupled Transaction Pattern that executes external Redis I/O outside of PostgreSQL transactions
// to prevent connection pool starvation and database row lock contention.
type OutboxWorker struct {
	svc *Service
}

func NewOutboxWorker(svc *Service) *OutboxWorker {
	return &OutboxWorker{svc: svc}
}

type CampaignPayload struct {
	CampaignID  string `json:"campaign_id"`
	BudgetLimit string `json:"budget_limit,omitempty"`
}

type SettingsPayload struct {
	Settings map[string]string `json:"settings"`
}

func (w *OutboxWorker) Start(ctx context.Context, interval time.Duration) {
	// 1. Cold Sync on startup: Drain any pending events created while the worker was offline
	if err := w.ProcessOutbox(ctx); err != nil {
		slog.Error("outbox startup cold sync failed", "error", err)
	}

	// 2. Persistent LISTEN/NOTIFY background worker (Real-time CDC Push Path)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Acquire a dedicated connection for LISTEN
			conn, err := w.svc.pool.Acquire(ctx)
			if err != nil {
				slog.Error("failed to acquire connection for outbox listen, retrying in 2s", "error", err)
				time.Sleep(2 * time.Second)
				continue
			}

			_, err = conn.Exec(ctx, "LISTEN outbox_channel")
			if err != nil {
				conn.Release()
				slog.Error("failed to execute LISTEN on outbox channel, retrying in 2s", "error", err)
				time.Sleep(2 * time.Second)
				continue
			}

			slog.Info("outbox worker listening for real-time events via pg_notify")

			for {
				select {
				case <-ctx.Done():
					conn.Release()
					return
				default:
				}

				// WaitForNotification blocks until a notification is received or context is canceled
				_, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					conn.Release()
					if ctx.Err() != nil {
						return
					}
					slog.Error("outbox listen connection lost, reconnecting in 2s", "error", err)
					time.Sleep(2 * time.Second)
					break // Break inner loop to trigger reconnect
				}

				// Real-time edge-triggered signal: drain the queue!
				if err := w.ProcessOutbox(ctx); err != nil {
					slog.Error("failed to process outbox after notification", "error", err)
				}
			}
		}
	}()

	// 3. Fallback Interval Janitor: Resets stuck 'PROCESSING' states and drains missed signals
	ticker := time.NewTicker(interval * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Self-healing: Reset events stuck in 'PROCESSING' state for > 5 minutes back to 'PENDING'
			_, _ = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PENDING' WHERE status = 'PROCESSING' AND created_at < NOW() - INTERVAL '5 minutes'")

			// Trigger safety drain
			if err := w.ProcessOutbox(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") {
					return
				}
				slog.Error("failed to run safety outbox fallback drain", "error", err)
			}
		}
	}
}

func (w *OutboxWorker) ProcessOutbox(ctx context.Context) error {
	var events []db.OutboxEvent

	// Acquire pending outbox events and transition them to PROCESSING inside a localized transaction.
	// This immediately commits the status update, releasing row locks and returning the DB connection to the pool before executing external I/O.
	err := pgx.BeginFunc(ctx, w.svc.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		var err error
		events, err = q.GetPendingOutboxEventsForUpdate(ctx, 100)
		if err != nil || len(events) == 0 {
			return err
		}

		for _, ev := range events {
			_, err = tx.Exec(ctx, "UPDATE outbox_events SET status = 'PROCESSING' WHERE id = $1", ev.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil || len(events) == 0 {
		return err
	}

	processedIDs := make([]int64, 0, len(events))
	revertIDs := make([]int64, 0, len(events))

	// Execute Redis network I/O completely outside of the PostgreSQL database transaction.
	// This decouples the database transaction hold time from external network transport latencies to prevent database connection pool exhaustion.
	for _, ev := range events {
		var rdbErr error
		switch ev.EventType {
		case "CREATE_CAMPAIGN":
			var p CampaignPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				campUUID, _ := uuid.Parse(p.CampaignID)
				rdb := w.svc.getRDB(campUUID)
				if rdb != nil {
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						budgetVal := int64(0)
						if dec, err := decimal.NewFromString(p.BudgetLimit); err == nil {
							budgetVal = ads.DecimalToMicro(dec)
						}
						pipe.Set(ctx, fmt.Sprintf("budget:campaign:%s", p.CampaignID), budgetVal, 24*time.Hour)
						channel := w.svc.cfg.CampaignUpdateChannel
						if channel == "" {
							channel = "campaigns:update"
						}
						pipe.Publish(ctx, channel, p.CampaignID)
						return nil
					})
				}
			}
		case "CANCEL_CAMPAIGN":
			var p CampaignPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				campUUID, _ := uuid.Parse(p.CampaignID)
				rdb := w.svc.getRDB(campUUID)
				if rdb != nil {
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Del(ctx, fmt.Sprintf("budget:campaign:%s", p.CampaignID))
						channel := w.svc.cfg.CampaignUpdateChannel
						if channel == "" {
							channel = "campaigns:update"
						}
						pipe.Publish(ctx, channel, p.CampaignID)
						return nil
					})
				}
			}
		case "UPDATE_SETTINGS":
			var p SettingsPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				if len(w.svc.rdbs) > 0 && w.svc.rdbs[0] != nil {
					rdb := w.svc.rdbs[0]
					_, rdbErr = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
						if len(p.Settings) > 0 {
							pipe.HSet(ctx, "config:values", p.Settings)
						}
						pipe.Incr(ctx, "config:version")
						return nil
					})
				}
			}
		}

		if rdbErr == nil {
			processedIDs = append(processedIDs, ev.ID)
		} else {
			slog.Warn("redis outbox processing failed for event, marking for revert", "id", ev.ID, "error", rdbErr)
			revertIDs = append(revertIDs, ev.ID)
		}
	}

	// Batch transition the outbox event statuses in the database to finalize the transaction outbox cycle.
	// Executing this as a batch query minimizes database roundtrips and lock holding times.
	if len(processedIDs) > 0 {
		_, err = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PROCESSED' WHERE id = ANY($1)", processedIDs)
		if err != nil {
			slog.Error("failed to mark outbox events as processed", "error", err)
		}
	}

	if len(revertIDs) > 0 {
		_, err = w.svc.pool.Exec(ctx, "UPDATE outbox_events SET status = 'PENDING' WHERE id = ANY($1)", revertIDs)
		if err != nil {
			slog.Error("failed to revert failed outbox events", "error", err)
		}
	}

	return nil
}
