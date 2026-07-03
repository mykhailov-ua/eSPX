package filter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/ads/sharding"
	adstest "espx/internal/ads/testutil"
	"espx/internal/domain"
	"espx/internal/metrics"

	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type budgetMissOnceRedis struct {
	adstest.MockRedisClient
	calls atomic.Int32
}

func (m *budgetMissOnceRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if m.calls.Add(1) == 1 {
		cmd.SetVal(int64(-1))
		return cmd
	}
	cmd.SetVal(int64(0))
	return cmd
}

func (m *budgetMissOnceRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return m.EvalSha(ctx, "", keys, args...)
}

type panicCampaignRepo struct{}

func (panicCampaignRepo) GetByID(context.Context, uuid.UUID) (*domain.Campaign, error) {
	panic("PG must not be called when registry has budget snapshot")
}

func (panicCampaignRepo) UpdateStatus(context.Context, uuid.UUID, domain.CampaignStatus) error {
	return nil
}

func (panicCampaignRepo) UpdateSpend(context.Context, uuid.UUID, int64, string) error {
	return nil
}

func (panicCampaignRepo) ListActive(context.Context) ([]*domain.Campaign, error) {
	return nil, nil
}

func TestUnifiedFilter_budgetMiss_recoversFromRegistryWithoutPG(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	adstest.CachedMockCamp.Store(&domain.Campaign{
		ID:           campID,
		CustomerID:   custID,
		BudgetLimit:  10_000_000,
		CurrentSpend: 0,
	})
	t.Cleanup(func() { adstest.CachedMockCamp.Store(nil) })

	reg := &adstest.MockRegistry{}
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissOnceRedis{}},
		sharding.NewJumpHashSharder(1),
		reg,
		panicCampaignRepo{},
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000,
		10_000,
		"events-budget-warm",
		10000,
	)

	beforePG := promtest.ToFloat64(metrics.BudgetCacheMissPGTotal)
	beforeRecover := promtest.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal)

	err := f.Check(context.Background(), &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.NoError(t, err)
	assert.Equal(t, beforePG, promtest.ToFloat64(metrics.BudgetCacheMissPGTotal))
	assert.Equal(t, beforeRecover+1, promtest.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal))
}

// TestVerify_budgetMissRegistryBeforePG documents hot path invariant:
// Lua -1 -> registry SET NX -> retry Lua; PostgreSQL only when registry lacks campaign budget snapshot.
func TestVerify_budgetMissRegistryBeforePG(t *testing.T) {
	TestUnifiedFilter_budgetMiss_recoversFromRegistryWithoutPG(t)
}
