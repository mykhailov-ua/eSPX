package management

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"espx/internal/ads/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const auditExportBatchSize = 1000

// AuditExportWorker writes daily audit_log CSV snapshots and prunes old export files.
type AuditExportWorker struct {
	svc           *Service
	exportPath    string
	retentionDays int
}

// NewAuditExportWorker configures the worker with the export directory and file retention window.
func NewAuditExportWorker(svc *Service, exportPath string, retentionDays int) *AuditExportWorker {
	if retentionDays <= 0 {
		retentionDays = 90
	}
	return &AuditExportWorker{
		svc:           svc,
		exportPath:    exportPath,
		retentionDays: retentionDays,
	}
}

// Start periodically exports audit logs until the context is cancelled.
func (w *AuditExportWorker) Start(ctx context.Context, interval time.Duration) {
	if err := w.ExportDaily(ctx, time.Now().UTC()); err != nil {
		slog.Error("audit export failed", "error", err)
	}
	if err := w.cleanupOldExports(time.Now().UTC()); err != nil {
		slog.Error("audit export retention cleanup failed", "error", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if err := w.ExportDaily(ctx, now); err != nil {
				slog.Error("audit export failed", "error", err)
			}
			if err := w.cleanupOldExports(now); err != nil {
				slog.Error("audit export retention cleanup failed", "error", err)
			}
		}
	}
}

// ExportDaily writes a CSV snapshot for the UTC calendar day containing now.
func (w *AuditExportWorker) ExportDaily(ctx context.Context, now time.Time) error {
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return w.exportDay(ctx, day)
}

func (w *AuditExportWorker) exportDay(ctx context.Context, day time.Time) error {
	start := day
	end := day.Add(24 * time.Hour)
	filename := day.Format("2006-01-02") + ".csv"

	if err := os.MkdirAll(w.exportPath, 0755); err != nil {
		return fmt.Errorf("create audit export dir: %w", err)
	}

	path := filepath.Join(w.exportPath, filename)
	tmpPath := path + ".tmp"

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open audit export temp file: %w", err)
	}

	var rowsWritten int
	writeErr := func() error {
		cw := csv.NewWriter(tmpFile)
		if err := cw.Write([]string{"id", "admin_id", "action", "target_type", "target_id", "changes", "metadata", "created_at"}); err != nil {
			return fmt.Errorf("write audit export header: %w", err)
		}

		q := db.New(w.svc.GetPool())
		var offset int32
		for {
			batch, err := q.ListAuditLogsInRange(ctx, db.ListAuditLogsInRangeParams{
				CreatedAt:   pgtype.Timestamptz{Time: start, Valid: true},
				CreatedAt_2: pgtype.Timestamptz{Time: end, Valid: true},
				Limit:       auditExportBatchSize,
				Offset:      offset,
			})
			if err != nil {
				return fmt.Errorf("list audit logs for export: %w", err)
			}
			if len(batch) == 0 {
				break
			}

			for _, row := range batch {
				if err := cw.Write(auditLogCSVRecord(row)); err != nil {
					return fmt.Errorf("write audit export row: %w", err)
				}
				rowsWritten++
			}

			if len(batch) < auditExportBatchSize {
				break
			}
			offset += auditExportBatchSize
		}

		cw.Flush()
		if err := cw.Error(); err != nil {
			return fmt.Errorf("flush audit export csv: %w", err)
		}
		if err := tmpFile.Sync(); err != nil {
			return fmt.Errorf("sync audit export file: %w", err)
		}
		return nil
	}()

	if writeErr != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return writeErr
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close audit export temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace audit export file: %w", err)
	}

	slog.Info("audit log exported", "path", path, "day", filename, "rows", rowsWritten)
	return nil
}

func auditLogCSVRecord(row db.AdminAuditLog) []string {
	adminID := ""
	if row.AdminID.Valid {
		adminID = uuid.UUID(row.AdminID.Bytes).String()
	}
	targetID := ""
	if row.TargetID.Valid {
		targetID = uuid.UUID(row.TargetID.Bytes).String()
	}
	createdAt := ""
	if row.CreatedAt.Valid {
		createdAt = row.CreatedAt.Time.UTC().Format(time.RFC3339)
	}
	return []string{
		fmt.Sprintf("%d", row.ID),
		adminID,
		row.Action,
		row.TargetType,
		targetID,
		string(row.Changes),
		string(row.Metadata),
		createdAt,
	}
}

func (w *AuditExportWorker) cleanupOldExports(now time.Time) error {
	cutoff := now.AddDate(0, 0, -w.retentionDays)

	entries, err := os.ReadDir(w.exportPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read audit export dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		dayStr := strings.TrimSuffix(entry.Name(), ".csv")
		day, err := time.ParseInLocation("2006-01-02", dayStr, time.UTC)
		if err != nil {
			continue
		}
		if day.Before(cutoff) {
			if err := os.Remove(filepath.Join(w.exportPath, entry.Name())); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove old audit export %s: %w", entry.Name(), err)
			}
			slog.Info("removed expired audit export", "file", entry.Name(), "retention_days", w.retentionDays)
		}
	}
	return nil
}
