package ads

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"espx/internal/domain"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestChaos_LuaFastPathP99 runs a 10k impression burst on budget_fast.lua and checks R5 on Redis spend.
func TestChaos_LuaFastPathP99(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	registry := newChaosRegistry(t, infra.Queries)
	campaignID := seedChaosCampaign(t, infra, registry)

	f := NewUnifiedFilter(
		[]redis.UniversalClient{infra.Redis},
		NewJumpHashSharder(1),
		registry,
		NewCampaignRepo(infra.Queries),
		0,
		time.Minute,
		time.Hour,
		time.Hour,
		100_000,
		10_000,
		"ad:events:stream",
		100_000,
	)
	f.SetLuaFastPathEnabled(true)
	f.SetTTCMin(0)
	require.NoError(t, f.PreloadScripts(ctx))

	camp, ok := registry.GetCampaign(campaignID)
	require.True(t, ok)
	require.NoError(t, infra.Redis.Set(ctx, camp.BudgetCampaignKey, 100_000_000, 0).Err())

	const iterations = 10_000
	latencies := make([]time.Duration, 0, iterations)
	beforeFast := testutil.ToFloat64(metrics.RedisLuaFastPathTotal.WithLabelValues("0"))

	for i := 0; i < iterations; i++ {
		evt := &domain.Event{
			Type:       "impression",
			CampaignID: campaignID,
			ClickID:    fmt.Sprintf("fast-%d", i),
			IP:         "203.0.113.200",
			UserID:     "burst",
		}
		checkCtx := attachFilterDeadline(ctx, 2*time.Second)
		start := time.Now()
		require.NoError(t, f.Check(checkCtx, evt))
		latencies = append(latencies, time.Since(start))
	}

	afterFast := testutil.ToFloat64(metrics.RedisLuaFastPathTotal.WithLabelValues("0"))
	require.Greater(t, afterFast-beforeFast, float64(iterations*9/10), "most events should use fast path")

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[len(latencies)*99/100]
	t.Logf("lua fast path n=%d p99=%v", iterations, p99)

	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, campaignID)

	logChaosProof(t, "lua_fastpath_p99", map[string]string{
		"n":          fmt.Sprintf("%d", iterations),
		"p99_ms":     fmt.Sprintf("%.3f", float64(p99.Microseconds())/1000),
		"fast_total": fmt.Sprintf("%.0f", afterFast-beforeFast),
		"r5":         "ok",
	})
}

// BenchmarkUnifiedFilter_Check_FastPath_RealRedis compares Tier B against full Lua on real Redis.
func BenchmarkUnifiedFilter_Check_FastPath_RealRedis(b *testing.B) {
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
	f.SetLuaFastPathEnabled(true)
	f.SetTTCMin(0)
	if err := f.PreloadScripts(ctx); err != nil {
		b.Fatal(err)
	}
	campID := uuid.New()
	seedCampaignBudget(b, ctx, rdb, campID)

	evt := &domain.Event{
		Type:       "impression",
		IP:         "203.0.113.210",
		CampaignID: campID,
	}
	setFilterDeadlineOnEvent(evt, time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evt.ClickID = ""
		if err := f.Check(ctx, evt); err != nil {
			b.Fatal(err)
		}
	}
}
