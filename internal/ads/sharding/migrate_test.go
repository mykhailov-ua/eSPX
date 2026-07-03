package sharding

import (
	"context"
	"testing"

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
	key := "budget:campaign:" + id.String()
	require.NoError(t, src.Set(ctx, key, "5000000", 0).Err())
	require.NoError(t, src.Set(ctx, "budget:quota:"+id.String(), "1000000", 0).Err())
	require.NoError(t, src.Set(ctx, "fcap:c:"+id.String()+":u:abc", "3", 0).Err())

	m := &CampaignKeyMigrator{}
	n, err := m.MigrateCampaignKeys(ctx, src, dst, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 3)

	val, err := dst.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "5000000", val)

	n2, err := m.MigrateCampaignKeys(ctx, src, dst, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n2, 3)

	deleted, err := m.DrainCampaignKeys(ctx, src, id)
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, 3)
	require.Equal(t, int64(0), src.Exists(ctx, key).Val())
	require.Equal(t, "5000000", dst.Get(ctx, key).Val())
}
