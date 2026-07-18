package licensing

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/google/uuid"
)

var ErrEntitlementBufferFull = errors.New("entitlement sync buffer full")

// EntitlementSyncHandler applies a recovered entitlement update for one customer.
type EntitlementSyncHandler func(ctx context.Context, customerID uuid.UUID) error

// EntitlementSyncBuffer is a bounded cold-path queue protecting management from OOM during entitlement storms.
type EntitlementSyncBuffer struct {
	capacity int
	ch       chan uuid.UUID
	handler  EntitlementSyncHandler
	wg       sync.WaitGroup
}

// NewEntitlementSyncBuffer creates a buffer with explicit capacity. capacity must be > 0.
func NewEntitlementSyncBuffer(capacity int, handler EntitlementSyncHandler) *EntitlementSyncBuffer {
	if capacity <= 0 {
		capacity = 256
	}
	return &EntitlementSyncBuffer{
		capacity: capacity,
		ch:       make(chan uuid.UUID, capacity),
		handler:  handler,
	}
}

// Start launches the drain goroutine. Call Stop to shut down cleanly.
func (b *EntitlementSyncBuffer) Start(ctx context.Context) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-ctx.Done():
				slog.Info("entitlement sync buffer stopping", "reason", ctx.Err())
				return
			case customerID := <-b.ch:
				if err := b.handler(ctx, customerID); err != nil {
					slog.Error("entitlement sync handler failed",
						"customer_id", customerID,
						"error", err,
					)
				}
			}
		}
	}()
}

// Enqueue schedules a customer entitlement refresh. Returns ErrEntitlementBufferFull when saturated.
func (b *EntitlementSyncBuffer) Enqueue(customerID uuid.UUID) error {
	select {
	case b.ch <- customerID:
		return nil
	default:
		slog.Warn("entitlement sync buffer saturated, rejecting enqueue",
			"customer_id", customerID,
			"capacity", b.capacity,
		)
		return ErrEntitlementBufferFull
	}
}

// Recover replays pending customer IDs after process restart or dependency recovery.
func (b *EntitlementSyncBuffer) Recover(ctx context.Context, customerIDs []uuid.UUID) {
	for _, id := range customerIDs {
		if err := b.Enqueue(id); err != nil {
			slog.Warn("entitlement recovery enqueue dropped",
				"customer_id", id,
				"error", err,
			)
			if err := b.handler(ctx, id); err != nil {
				slog.Error("entitlement recovery direct apply failed",
					"customer_id", id,
					"error", err,
				)
			}
		}
	}
}

// PendingLen returns the number of queued updates (for metrics/tests).
func (b *EntitlementSyncBuffer) PendingLen() int {
	return len(b.ch)
}

// Stop waits for the worker goroutine to exit after context cancellation.
func (b *EntitlementSyncBuffer) Stop() {
	b.wg.Wait()
}
