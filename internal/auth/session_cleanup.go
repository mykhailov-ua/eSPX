package auth

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// SessionCleanupWorker reclaims stale session rows so refresh-token storage does not grow without bound.
type SessionCleanupWorker struct {
	svc *Service
}

// NewSessionCleanupWorker attaches retention policy to the auth store without blocking request paths.
func NewSessionCleanupWorker(svc *Service) *SessionCleanupWorker {
	return &SessionCleanupWorker{svc: svc}
}

// Start runs cleanup on a ticker so expired refresh rows do not accumulate without bound.
func (w *SessionCleanupWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Cleanup(ctx); err != nil {
				if strings.Contains(err.Error(), "closed pool") || strings.Contains(err.Error(), "client is closed") {
					return
				}
				slog.Error("failed to cleanup expired or blocked sessions", "error", err)
			}
		}
	}
}

// Cleanup reclaims blocked and expired sessions in one pass per tick to cap table growth.
func (w *SessionCleanupWorker) Cleanup(ctx context.Context) error {
	rows, err := w.svc.repo.DeleteExpiredOrBlockedSessions(ctx)
	if err != nil {
		return err
	}
	if rows > 0 {
		slog.Info("cleaned up expired or blocked sessions", "count", rows)
	}
	return nil
}
