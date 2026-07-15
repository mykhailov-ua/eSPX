package ingestion

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"espx/internal/campaignmodel"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	testQuotaChunkMicro      int64 = 1_000_000
	testQuotaClickMicro      int64 = 100_000
	testQuotaRefillThreshold       = 20
)

func newQuotaUnifiedFilter(t testing.TB, rdb redis.UniversalClient) *UnifiedFilter {
	t.Helper()
	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetQuotaConfig("live", testQuotaChunkMicro, testQuotaRefillThreshold)
	return f
}

func quotaKey(campaignID uuid.UUID) string {
	return "budget:quota:" + campaignID.String()
}

func seedCampaignQuota(t testing.TB, ctx context.Context, rdb redis.UniversalClient, campID uuid.UUID, micro int64) {
	t.Helper()
	require.NoError(t, rdb.Set(ctx, quotaKey(campID), micro, 0).Err())
}

func TestUnifiedFilter_quotaDebit(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newQuotaUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignQuota(t, ctx, rdb, campID, 500_000)

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.10",
		UserID:     "quota-u1",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	require.NoError(t, f.Check(checkCtx, evt))

	remaining, err := rdb.Get(ctx, quotaKey(campID)).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(400_000), remaining)
}

func TestUnifiedFilter_quotaDualRead_legacyFallback(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newQuotaUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 300_000, 0).Err())

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.11",
		UserID:     "quota-u2",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	require.NoError(t, f.Check(checkCtx, evt))

	remaining, err := rdb.Get(ctx, camp.BudgetCampaignKey).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(200_000), remaining)
	exists, err := rdb.Exists(ctx, quotaKey(campID)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists)
}

func TestUnifiedFilter_quotaExhausted_returns3(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newQuotaUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignQuota(t, ctx, rdb, campID, 50_000)

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.12",
		UserID:     "quota-u3",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	require.ErrorIs(t, f.Check(checkCtx, evt), ErrBudgetExhausted)
}

func TestUnifiedFilter_quotaRefill_thunderingHerd(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newQuotaUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	// chunk=1M, threshold=200k; after 100k debit from 250k -> 150k < 200k triggers refill
	seedCampaignQuota(t, ctx, rdb, campID, 250_000)

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			evt := &campaignmodel.Event{
				Type:       "click",
				IP:         "203.0.113.13",
				UserID:     fmt.Sprintf("quota-herd-%d", i),
				CampaignID: campID,
				ClickID:    uuid.NewString(),
			}
			checkCtx := attachFilterDeadline(ctx, time.Second)
			_ = f.Check(checkCtx, evt)
		}(i)
	}
	wg.Wait()

	count, err := rdb.SCard(ctx, "budget:refill_needed").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "refill lock must collapse parallel enqueue to one task")
}

func TestUnifiedFilter_quotaOff_legacyPathUnchanged(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	f.SetQuotaConfig("off", testQuotaChunkMicro, testQuotaRefillThreshold)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.14",
		UserID:     "quota-off",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)
	require.NoError(t, f.Check(checkCtx, evt))

	count, err := rdb.SCard(ctx, "budget:refill_needed").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestUnifiedFilter_QuotaMode_LatencyProfile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newQuotaUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignQuota(t, ctx, rdb, campID, 50_000_000_000)

	const iterations = 300
	latencies := make([]time.Duration, 0, iterations)
	for i := range iterations {
		evt := &campaignmodel.Event{
			Type:       "click",
			IP:         "203.0.113.15",
			UserID:     fmt.Sprintf("quota-bench-%d", i),
			CampaignID: campID,
			ClickID:    uuid.NewString(),
		}
		checkCtx := attachFilterDeadline(ctx, time.Second)
		start := time.Now()
		require.NoError(t, f.Check(checkCtx, evt))
		latencies = append(latencies, time.Since(start))
	}

	sortLatencies := append([]time.Duration(nil), latencies...)
	sortDurations(sortLatencies)
	p50 := sortLatencies[len(sortLatencies)*50/100]
	p99 := sortLatencies[len(sortLatencies)*99/100]
	t.Logf("quota mode UnifiedFilter.Check n=%d p50=%v p99=%v", iterations, p50, p99)
	if p50 > 8*time.Millisecond {
		t.Fatalf("p50 %v exceeds 8ms local bound with quota keys", p50)
	}
}

func sortDurations(a []time.Duration) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func BenchmarkUnifiedFilter_Check_QuotaMode(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(b)
	defer cleanup()

	f := NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		&mockRegistry{},
		nil,
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"events",
		10_000,
	)
	f.SetQuotaConfig("live", testQuotaChunkMicro, testQuotaRefillThreshold)
	if err := f.PreloadScripts(ctx); err != nil {
		b.Fatal(err)
	}
	campID := uuid.New()
	seedCampaignQuota(b, ctx, rdb, campID, 900_000_000_000)

	evt := &campaignmodel.Event{
		Type:       "click",
		IP:         "203.0.113.16",
		UserID:     "quota-bench",
		CampaignID: campID,
	}
	setFilterDeadlineOnEvent(evt, time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := strconv.AppendInt(evt.ClickIDBuf[:0], int64(i), 10)
		evt.ClickID = unsafeString(n)
		if err := f.Check(ctx, evt); err != nil {
			b.Fatal(err)
		}
	}
}
