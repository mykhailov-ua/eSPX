package management

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mykhailov-ua/ad-event-processor/internal/database"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNginxConfigWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, nil)
	defer svc.Close()

	exportPath := t.TempDir()
	worker := NewNginxConfigWorker(svc, exportPath)

	ctx := context.Background()

	t.Run("ExportBlacklists", func(t *testing.T) {
		err := svc.BlockIP(ctx, "1.2.3.4", "manual")
		require.NoError(t, err)
		err = svc.BlockIP(ctx, "5.6.7.8", "auto")
		require.NoError(t, err)

		err = worker.ExportAndReload(ctx)
		require.NoError(t, err)

		manualContent, err := os.ReadFile(filepath.Join(exportPath, "manual.conf"))
		require.NoError(t, err)
		assert.Contains(t, string(manualContent), "deny 1.2.3.4;")

		autoContent, err := os.ReadFile(filepath.Join(exportPath, "auto.conf"))
		require.NoError(t, err)
		assert.Contains(t, string(autoContent), "deny 5.6.7.8;")

		flagPath := filepath.Join(exportPath, "reload_required.flg")
		flagInfo, err := os.Stat(flagPath)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0644), flagInfo.Mode().Perm())

		flagContent, err := os.ReadFile(flagPath)
		require.NoError(t, err)
		assert.Equal(t, "1\n", string(flagContent))
	})
}
