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

func newRealRedisUnifiedFilter(t testing.TB, rdb redis.UniversalClient) *UnifiedFilter {
	t.Helper()
	return NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		NewJumpHashSharder(1),
		&mockRegistry{},
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

// Seeds Redis budget key so filter tests start with known balance.
func seedCampaignBudget(t testing.TB, ctx context.Context, rdb redis.UniversalClient, campID uuid.UUID) {
	t.Helper()
	reg := &mockRegistry{}
	camp, ok := reg.GetCampaign(campID)
	require.True(t, ok)
	require.NoError(t, rdb.Set(ctx, camp.BudgetCampaignKey, 9_000_000_000_000_000, 0).Err())
}

// Regression anchor: SCRIPT LOAD SHA must succeed with EVALSHA per Redis spec.
func TestVerify_1a_RedisSpec_EvalShaAfterScriptLoad(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	evt := &domain.Event{
		Type:       "click",
		IP:         "203.0.113.1",
		UserID:     "u1",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	before := testutil.ToFloat64(metrics.RedisLuaNoScriptTotal.WithLabelValues("0"))
	checkCtx := attachFilterDeadline(ctx, 500*time.Millisecond)
	err := f.Check(checkCtx, evt)
	require.NoError(t, err)
	after := testutil.ToFloat64(metrics.RedisLuaNoScriptTotal.WithLabelValues("0"))
	if after-before != 0 {
		t.Fatalf("expected zero NOSCRIPT fallbacks after preload, before=%v after=%v", before, after)
	}
}

func TestEvalScript_NOSCRIPTFallbackAfterScriptFlush(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))
	require.NoError(t, rdb.ScriptFlush(ctx).Err())

	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)
	evt := &domain.Event{
		Type:       "click",
		IP:         "203.0.113.2",
		UserID:     "u2",
		CampaignID: campID,
		ClickID:    uuid.NewString(),
	}
	before := testutil.ToFloat64(metrics.RedisLuaNoScriptTotal.WithLabelValues("0"))
	checkCtx := attachFilterDeadline(ctx, 500*time.Millisecond)
	require.NoError(t, f.Check(checkCtx, evt))
	after := testutil.ToFloat64(metrics.RedisLuaNoScriptTotal.WithLabelValues("0"))
	if after-before < 1 {
		t.Fatalf("expected NOSCRIPT fallback counter increment after SCRIPT FLUSH, delta=%v", after-before)
	}
}

func TestFilterRedisOptions_realClientRespectsReadTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	const filterMs = 30
	client, ok := rdb.(*redis.Client)
	require.True(t, ok, "expected single-node redis client")
	opts := FilterRedisOptions([]string{client.Options().Addr}, "", 4, filterMs)
	slow := redis.NewUniversalClient(opts)
	defer slow.Close()

	require.NoError(t, slow.Ping(ctx).Err())
	require.NoError(t, rdb.Do(ctx, "CLIENT", "PAUSE", 2000).Err())

	start := time.Now()
	err := slow.Ping(ctx).Err()
	elapsed := time.Since(start)

	require.Error(t, err, "ping during CLIENT PAUSE should exceed ReadTimeout")
	if elapsed < 25*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("expected ~%dms wall wait, got %v (err=%v)", filterMs, elapsed, err)
	}
}

// Regression anchor: real Redis UnifiedFilter.Check latency stays within SLA envelope.
func TestVerify_1d_RealRedisLatencyProfile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := context.Background()
	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	f := newRealRedisUnifiedFilter(t, rdb)
	require.NoError(t, f.PreloadScripts(ctx))

	const iterations = 500
	latencies := make([]time.Duration, 0, iterations)
	campID := uuid.New()
	seedCampaignBudget(t, ctx, rdb, campID)

	for i := 0; i < iterations; i++ {
		evt := &domain.Event{
			Type:       "click",
			IP:         "203.0.113.3",
			UserID:     fmt.Sprintf("u-%d", i),
			CampaignID: campID,
			ClickID:    uuid.NewString(),
		}
		checkCtx := attachFilterDeadline(ctx, 500*time.Millisecond)
		start := time.Now()
		require.NoError(t, f.Check(checkCtx, evt))
		latencies = append(latencies, time.Since(start))
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("real redis UnifiedFilter.Check n=%d p50=%v p99=%v (mock bench ~287ns/op)", iterations, p50, p99)

	if p50 > 5*time.Millisecond {
		t.Fatalf("p50 %v exceeds 5ms sanity bound for local testcontainer", p50)
	}
}

// Tracks UnifiedFilter.Check against real Redis for integration perf baselines.
func BenchmarkUnifiedFilter_Check_RealRedis(b *testing.B) {
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
	if err := f.PreloadScripts(ctx); err != nil {
		b.Fatal(err)
	}
	campID := uuid.New()
	seedCampaignBudget(b, ctx, rdb, campID)

	evt := &domain.Event{
		Type:       "click",
		IP:         "203.0.113.4",
		UserID:     "bench",
		CampaignID: campID,
	}
	checkCtx := attachFilterDeadline(ctx, time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		evt.ClickID = ""
		if err := f.Check(checkCtx, evt); err != nil {
			b.Fatal(err)
		}
	}
}
