package management

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditExportWorker_exportDay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	svc := NewService(pool, nil, nil, nil)
	defer svc.Close()

	exportPath := t.TempDir()
	worker := NewAuditExportWorker(svc, exportPath, 90)

	ctx := context.Background()
	day := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	adminID := uuid.New()
	campaignID := uuid.New()

	svc.AuditLog(ctx, nil, adminID, "EXPORT_TEST", "campaign", &campaignID, map[string]string{"field": "value"}, map[string]string{"ip": "127.0.0.1"})

	_, err := pool.Exec(ctx, `
		UPDATE admin_audit_log
		SET created_at = $1
		WHERE action = 'EXPORT_TEST'`, day.Add(12*time.Hour))
	require.NoError(t, err)

	require.NoError(t, worker.exportDay(ctx, day))

	csvPath := filepath.Join(exportPath, "2026-07-07.csv")
	content, err := os.ReadFile(csvPath)
	require.NoError(t, err)

	records, err := csv.NewReader(strings.NewReader(string(content))).ReadAll()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 2)
	assert.Equal(t, []string{"id", "admin_id", "action", "target_type", "target_id", "changes", "metadata", "created_at"}, records[0])
	assert.Equal(t, "EXPORT_TEST", records[1][2])
	assert.Equal(t, campaignID.String(), records[1][4])
	assert.Contains(t, records[1][5], "field")
}

func TestAuditExportWorker_retentionCleanup(t *testing.T) {
	exportPath := t.TempDir()
	worker := NewAuditExportWorker(nil, exportPath, 30)

	oldDay := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	recentDay := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, os.WriteFile(filepath.Join(exportPath, oldDay.Format("2006-01-02")+".csv"), []byte("old"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(exportPath, recentDay.Format("2006-01-02")+".csv"), []byte("recent"), 0644))

	now := time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC)
	require.NoError(t, worker.cleanupOldExports(now))

	_, err := os.Stat(filepath.Join(exportPath, oldDay.Format("2006-01-02")+".csv"))
	assert.True(t, os.IsNotExist(err))

	_, err = os.Stat(filepath.Join(exportPath, recentDay.Format("2006-01-02")+".csv"))
	require.NoError(t, err)
}
