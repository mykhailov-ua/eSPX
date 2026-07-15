package management

import (
	"context"
	"log/slog"
	"time"

	"espx/internal/ingestion/sqlc"
)

const blacklistJanitorBatchSize = 200

// BlacklistJanitor evicts temporary blacklist rows from Postgres and propagates unblocks via outbox.
type BlacklistJanitor struct {
	svc      *Service
	interval time.Duration
}

// NewBlacklistJanitor constructs a janitor with the given scan interval.
func NewBlacklistJanitor(svc *Service, interval time.Duration) *BlacklistJanitor {
	if interval <= 0 {
		interval = time.Minute
	}
	return &BlacklistJanitor{svc: svc, interval: interval}
}

// Start runs periodic expiry scans until the context is cancelled.
func (j *BlacklistJanitor) Start(ctx context.Context) {
	if j == nil || j.svc == nil {
		return
	}

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	j.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

func (j *BlacklistJanitor) runOnce(ctx context.Context) {
	opCtx, cancel := workerContext(ctx, workerBatchTimeout)
	defer cancel()

	rows, err := db.New(j.svc.GetPool()).ListExpiredBlacklistIPs(opCtx, blacklistJanitorBatchSize)
	if err != nil {
		slog.Error("blacklist janitor scan failed", "error", err)
		if j.svc.alerter != nil {
			j.svc.alerter.AlertBlacklistJanitorFailed(err)
		}
		return
	}
	if len(rows) == 0 {
		return
	}

	var removed int
	for _, row := range rows {
		if err := j.svc.UnblockIP(opCtx, row.Ip, row.Reason); err != nil {
			slog.Warn("blacklist janitor unblock failed",
				"ip", row.Ip,
				"reason", row.Reason,
				"error", err,
			)
			continue
		}
		removed++
	}

	slog.Info("blacklist janitor cycle complete",
		"expired_found", len(rows),
		"removed", removed,
	)
}
