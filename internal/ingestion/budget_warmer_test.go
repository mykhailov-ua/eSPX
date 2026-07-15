package ingestion

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/ingestion/sqlc"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type budgetMissOnceRedis struct {
	mockRedisClient
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

func (m *budgetMissOnceRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	if m.calls.Add(1) == 1 {
		setProcessLuaInt64(cmd, -1)
	} else {
		setProcessLuaInt64(cmd, 0)
	}
	return nil
}

type panicCampaignRepo struct{}

func (panicCampaignRepo) GetByID(context.Context, uuid.UUID) (*campaignmodel.Campaign, error) {
	panic("PG must not be called when registry has budget snapshot")
}

func (panicCampaignRepo) UpdateStatus(context.Context, uuid.UUID, campaignmodel.CampaignStatus) error {
	return nil
}

func (panicCampaignRepo) UpdateSpend(context.Context, uuid.UUID, int64, string) error {
	return nil
}

func (panicCampaignRepo) ListActive(context.Context) ([]*campaignmodel.Campaign, error) {
	return nil, nil
}

func TestRemainingBudgetMicro(t *testing.T) {
	assert.Equal(t, int64(0), RemainingBudgetMicro(nil))
	assert.Equal(t, int64(700), RemainingBudgetMicro(&campaignmodel.Campaign{BudgetLimit: 1000, CurrentSpend: 300}))
	assert.Equal(t, int64(0), RemainingBudgetMicro(&campaignmodel.Campaign{BudgetLimit: 100, CurrentSpend: 500}))
}

func TestBudgetCacheWarmer_SetNXDoesNotOverwrite(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	camp.BudgetLimit = 1_000_000
	camp.CurrentSpend = 200_000
	cachedMockCamp.Store(camp)

	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 42, 0).Err())

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	warmed, err := w.Warm(ctx, []*campaignmodel.Campaign{camp})
	require.NoError(t, err)
	assert.Equal(t, 0, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(42), val)
}

func TestBudgetCacheWarmer_insertsMissingKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	camp.BudgetLimit = 5_000_000
	camp.CurrentSpend = 1_000_000
	cachedMockCamp.Store(camp)

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	warmed, err := w.Warm(ctx, []*campaignmodel.Campaign{camp})
	require.NoError(t, err)
	assert.Equal(t, 1, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(4_000_000), val)
}

func TestUnifiedFilter_budgetMiss_recoversFromRegistryWithoutPG(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	cachedMockCamp.Store(&campaignmodel.Campaign{
		ID:           campID,
		CustomerID:   custID,
		BudgetLimit:  10_000_000,
		CurrentSpend: 0,
	})
	t.Cleanup(func() { cachedMockCamp.Store(nil) })

	reg := &mockRegistry{}
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissOnceRedis{}},
		NewJumpHashSharder(1),
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

	beforePG := testutil.ToFloat64(metrics.BudgetCacheMissPGTotal)
	beforeRecover := testutil.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal)

	err := f.Check(context.Background(), &campaignmodel.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.NoError(t, err)
	assert.Equal(t, beforePG, testutil.ToFloat64(metrics.BudgetCacheMissPGTotal))
	assert.Equal(t, beforeRecover+1, testutil.ToFloat64(metrics.BudgetCacheRegistryRecoverTotal))
}

// TestVerify_budgetMissRegistryBeforePG documents hot path invariant:
// Lua -1 -> registry SET NX -> retry Lua; PostgreSQL only when registry lacks campaign budget snapshot.
func TestVerify_budgetMissRegistryBeforePG(t *testing.T) {
	TestUnifiedFilter_budgetMiss_recoversFromRegistryWithoutPG(t)
}

func TestBudgetCacheWarmer_WarmOne_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	camp := &campaignmodel.Campaign{
		ID:                campID,
		BudgetLimit:       2_000_000,
		CurrentSpend:      500_000,
		BudgetCampaignKey: "budget:campaign:" + campID.String(),
	}

	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))

	// First warm should succeed (SET NX)
	warmed, err := w.WarmOne(ctx, camp)
	require.NoError(t, err)
	assert.True(t, warmed)

	val, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(1_500_000), val)

	// Second warm should return false since key already exists
	warmed2, err := w.WarmOne(ctx, camp)
	require.NoError(t, err)
	assert.False(t, warmed2)
}

func TestCampaignRegistry_UpdateAndWarmCampaign_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	custID := uuid.New()

	// Настраиваем MockRepo
	mock := &MockRepo{
		budgets: map[uuid.UUID]db.GetCampaignBudgetRow{
			campID: {
				ID:           pgtype.UUID{Bytes: campID, Valid: true},
				CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
				BudgetLimit:  3_000_000,
				CurrentSpend: 1_000_000,
				Status:       db.CampaignStatusTypeACTIVE,
			},
		},
	}

	r := newTestRegistry(t, mock)
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	r.SetBudgetWarmer(w)

	// Добавляем кампанию в реестр с изначальными значениями
	r.Add(campID, custID, nil, "", campaignmodel.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	// Проверяем, что изначальные значения в реестре верны
	campBefore, ok := r.GetCampaign(campID)
	require.True(t, ok)
	assert.Equal(t, int64(1000), campBefore.DailyBudget)

	// Вызываем инкрементальный прогрев/обновление
	err := r.UpdateAndWarmCampaign(ctx, campID)
	require.NoError(t, err)

	// Проверяем, что в реестре обновились значения бюджета
	campAfter, ok := r.GetCampaign(campID)
	require.True(t, ok)
	assert.Equal(t, int64(3_000_000), campAfter.BudgetLimit)
	assert.Equal(t, int64(1_000_000), campAfter.CurrentSpend)

	// Проверяем, что в Redis записался правильный оставшийся бюджет
	val, err := rdb.Get(ctx, campAfter.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(2_000_000), val)
}

func TestCampaignRegistry_StartWatch_IncrementalWarm(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	campID := uuid.New()
	custID := uuid.New()

	mock := &MockRepo{
		budgets: map[uuid.UUID]db.GetCampaignBudgetRow{
			campID: {
				ID:           pgtype.UUID{Bytes: campID, Valid: true},
				CustomerID:   pgtype.UUID{Bytes: custID, Valid: true},
				BudgetLimit:  5_000_000,
				CurrentSpend: 1_000_000,
				Status:       db.CampaignStatusTypeACTIVE,
			},
		},
	}

	r := newTestRegistry(t, mock)
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	r.SetBudgetWarmer(w)

	// Добавляем кампанию в реестр с изначальными значениями
	r.Add(campID, custID, nil, "", campaignmodel.PacingModeAsap, 1000, "UTC", 0, 0, nil)

	channel := "test:campaign:updates:incremental"
	r.StartWatch(ctx, rdb, channel)

	// Даем время подписке установиться
	time.Sleep(200 * time.Millisecond)

	// Публикуем сообщение в pubsub с ID кампании
	err := rdb.Publish(ctx, channel, campID.String()).Err()
	require.NoError(t, err)

	// Проверяем, что в реестре обновились значения бюджета
	assert.Eventually(t, func() bool {
		camp, ok := r.GetCampaign(campID)
		return ok && camp.BudgetLimit == 5_000_000 && camp.CurrentSpend == 1_000_000
	}, 2*time.Second, 50*time.Millisecond)

	// Проверяем, что в Redis записался правильный оставшийся бюджет
	val, err := rdb.Get(ctx, "budget:campaign:"+campID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(4_000_000), val)
}

type benchmarkRedisClient struct {
	redis.UniversalClient
}

type benchmarkPipeliner struct {
	redis.Pipeliner
}

func (b *benchmarkPipeliner) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

func (b *benchmarkPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return nil, nil
}

func (r *benchmarkRedisClient) Pipeline() redis.Pipeliner {
	return &benchmarkPipeliner{}
}

func (r *benchmarkRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	cmd.SetVal(true)
	return cmd
}

func BenchmarkBudgetCacheWarmer_WarmOne(b *testing.B) {
	ctx := context.Background()
	rdb := &benchmarkRedisClient{}
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	campID := uuid.New()
	camp := &campaignmodel.Campaign{
		ID:                campID,
		BudgetLimit:       1000,
		CurrentSpend:      100,
		BudgetCampaignKey: "budget:campaign:" + campID.String(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.WarmOne(ctx, camp)
	}
}

func BenchmarkBudgetCacheWarmer_Warm(b *testing.B) {
	ctx := context.Background()
	rdb := &benchmarkRedisClient{}
	w := NewBudgetCacheWarmer([]redis.UniversalClient{rdb}, NewJumpHashSharder(1))
	campaigns := make([]*campaignmodel.Campaign, 10)
	for i := 0; i < 10; i++ {
		campID := uuid.New()
		campaigns[i] = &campaignmodel.Campaign{
			ID:                campID,
			BudgetLimit:       1000,
			CurrentSpend:      100,
			BudgetCampaignKey: "budget:campaign:" + campID.String(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = w.Warm(ctx, campaigns)
	}
}
