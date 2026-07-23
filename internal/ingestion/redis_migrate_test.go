package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"espx/internal/database"
)

func TestCampaignKeyMigrator_MigrateAndDrain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	src, cleanupSrc := database.SetupTestRedis(t)
	defer cleanupSrc()
	dst, cleanupDst := database.SetupTestRedis(t)
	defer cleanupDst()

	ctx := context.Background()
	id := uuid.New()
	key := BudgetCampaignKey(id)
	quotaKey := campaignHashTag(id) + "budget:quota:" + id.String()
	fcapKey := fcapKeyPrefix(id, "") + "abc"
	dupKey := campaignHashTag(id) + "dup:click:" + uuid.NewString()
	require.NoError(t, src.Set(ctx, key, "5000000", 0).Err())
	require.NoError(t, src.Set(ctx, quotaKey, "1000000", 0).Err())
	require.NoError(t, src.Set(ctx, fcapKey, "3", 0).Err())
	require.NoError(t, src.Set(ctx, dupKey, "1", time.Hour).Err())

	m := &CampaignKeyMigrator{}
	n, err := m.MigrateCampaignKeys(ctx, src, dst, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 4)

	val, err := dst.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "5000000", val)
	require.Equal(t, int64(1), dst.Exists(ctx, dupKey).Val())

	n2, err := m.MigrateCampaignKeys(ctx, src, dst, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n2, 4)

	deleted, err := m.DrainCampaignKeys(ctx, src, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, 4)
	require.Equal(t, int64(0), src.Exists(ctx, key).Val())
	require.Equal(t, "5000000", dst.Get(ctx, key).Val())
}
