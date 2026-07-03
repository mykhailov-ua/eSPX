package filter

import (
	"context"
	"testing"
	"time"

	"espx/internal/ads/sharding"
	"espx/internal/ads/testutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// NewTestUnifiedFilter builds UnifiedFilter against real Redis for cross-package integration tests.
func NewTestUnifiedFilter(t testing.TB, rdb redis.UniversalClient) *UnifiedFilter {
	t.Helper()
	return NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharding.NewJumpHashSharder(1),
		&testutil.MockRegistry{},
		nil,
		10_000,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
}

// SeedTestCampaignBudget seeds Redis budget key so integration tests start with known balance.
func SeedTestCampaignBudget(t testing.TB, ctx context.Context, rdb redis.UniversalClient, campID uuid.UUID) {
	t.Helper()
	reg := &testutil.MockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 9_000_000_000_000_000, 0).Err())
}
