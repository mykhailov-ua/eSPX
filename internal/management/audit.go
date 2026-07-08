package management

import (
	"context"
	"espx/internal/ads/db"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// AuditLog persists an admin action for compliance review without failing the primary transaction.
func (s *Service) AuditLog(ctx context.Context, q db.Querier, adminID uuid.UUID, action string, targetType string, targetID *uuid.UUID, changes any, metadata any) {
	changesJSON, err := cold.MarshalJSON(changes)
	if err != nil {
		slog.Error("audit marshal changes failed", "error", err, "admin_id", adminID, "action", action)
		changesJSON = []byte("{}")
	}
	metadataJSON, err := cold.MarshalJSON(metadata)
	if err != nil {
		slog.Error("audit marshal metadata failed", "error", err, "admin_id", adminID, "action", action)
		metadataJSON = []byte("{}")
	}

	var tid pgtype.UUID
	if targetID != nil {
		tid = ads.ToUUID(*targetID)
	}

	if q == nil {
		q = db.New(s.GetPool())
	}

	_, err = q.CreateAuditLog(ctx, db.CreateAuditLogParams{
		AdminID:    ads.ToUUID(adminID),
		Action:     action,
		TargetType: targetType,
		TargetID:   tid,
		Changes:    changesJSON,
		Metadata:   metadataJSON,
	})

	if err != nil {
		slog.Error("failed to write audit log", "error", err, "admin_id", adminID, "action", action)
	}
}

// RunAuditCleaner periodically deletes audit rows older than the configured retention window.
func (s *Service) RunAuditCleaner(ctx context.Context, retention Days) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanOldLogs(ctx, retention)
		}
	}
}

// Days expresses audit retention duration in whole days for the cleaner worker.
type Days int

// cleanOldLogs removes audit entries older than the retention threshold to bound table growth.
func (s *Service) cleanOldLogs(ctx context.Context, retention Days) {
	threshold := time.Now().AddDate(0, 0, -int(retention))
	err := db.New(s.GetPool()).CleanupAuditLogs(ctx, pgtype.Timestamptz{Time: threshold, Valid: true})
	if err != nil {
		slog.Error("failed to cleanup audit logs", "error", err)
	} else {
		slog.Info("audit logs cleaned up", "older_than", threshold.Format(time.RFC3339))
	}
}
