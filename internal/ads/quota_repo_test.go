package ads

import (
	"context"
	"testing"

	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCampaignShardID_oneCampaignOneShard(t *testing.T) {
	t.Parallel()
	sharder := NewStaticSlotSharder(4)
	id := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	s1 := CampaignShardID(sharder, id)
	s2 := CampaignShardID(sharder, id)
	require.Equal(t, s1, s2)
	require.GreaterOrEqual(t, s1, 0)
	require.Less(t, s1, 4)
}

func TestQuotaRepo_ReserveChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	sharder := NewStaticSlotSharder(4)
	repo := NewQuotaRepo(pool)

	customerID := uuid.New()
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO customers (id, name, balance) VALUES ($1, $2, $3)`, customerID, "Quota Customer", 500_000_000)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO campaigns (id, name, budget_limit, current_spend, status, customer_id) VALUES ($1, $2, $3, $4, $5, $6)`,
		campaignID, "Quota Campaign", int64(100_000_000), int64(50_000_000), "ACTIVE", customerID,
	)
	require.NoError(t, err)

	res, err := repo.ReserveChunk(ctx, sharder, campaignID, 10_000_000, "refill-1")
	require.NoError(t, err)
	require.False(t, res.AlreadyApplied)
	require.Equal(t, int16(QuotaShardForCampaign(sharder, campaignID)), res.ShardID)
	require.Equal(t, int64(10_000_000), res.ReservedAmount)
	require.Equal(t, int64(10_000_000), res.ChunkSize)

	res2, err := repo.ReserveChunk(ctx, sharder, campaignID, 10_000_000, "refill-2")
	require.NoError(t, err)
	require.False(t, res2.AlreadyApplied)
	require.Equal(t, int64(20_000_000), res2.ReservedAmount)

	_, err = repo.ReserveChunk(ctx, sharder, campaignID, 35_000_000, "refill-3")
	require.ErrorIs(t, err, ErrQuotaBudgetExceeded)

	dup, err := repo.ReserveChunk(ctx, sharder, campaignID, 10_000_000, "refill-2")
	require.NoError(t, err)
	require.True(t, dup.AlreadyApplied)
	require.Equal(t, int64(20_000_000), dup.ReservedAmount)

	row, err := repo.GetQuota(ctx, sharder, campaignID)
	require.NoError(t, err)
	require.Equal(t, int64(20_000_000), row.ReservedAmount)
}

func TestQuotaRepo_ReserveChunk_invalidChunk(t *testing.T) {
	t.Parallel()
	repo := NewQuotaRepo(nil)
	_, err := repo.ReserveChunk(context.Background(), NewStaticSlotSharder(4), uuid.New(), 0, "k")
	require.ErrorIs(t, err, ErrQuotaInvalidChunk)
}
