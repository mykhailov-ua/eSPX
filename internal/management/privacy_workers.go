package management

import (
	"context"
	"log/slog"
	"time"
)

// ErasureWorker processes privacy erasure state transitions (M6.4).
type ErasureWorker struct {
	svc *Service
}

// NewErasureWorker binds the erasure processor to the management service.
func NewErasureWorker(svc *Service) *ErasureWorker {
	return &ErasureWorker{svc: svc}
}

// Start runs erasure ticks until the context is cancelled.
func (w *ErasureWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.ProcessPrivacyErasureTick(ctx); err != nil {
				slog.Error("privacy erasure tick failed", "error", err)
			}
		}
	}
}

// ConsentRetentionWorker deletes consent_events older than the configured retention window (M6.1).
type ConsentRetentionWorker struct {
	svc *Service
}

// NewConsentRetentionWorker binds consent retention cleanup to the service.
func NewConsentRetentionWorker(svc *Service) *ConsentRetentionWorker {
	return &ConsentRetentionWorker{svc: svc}
}

// Start runs daily consent retention cleanup.
func (w *ConsentRetentionWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.svc.CleanupConsentEvents(ctx); err != nil {
				slog.Error("consent retention cleanup failed", "error", err)
			}
		}
	}
}
