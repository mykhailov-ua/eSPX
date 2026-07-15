package ingestion

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fraudScorerDownP99Limit = 80 * time.Millisecond

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func runAtModuleRoot(t *testing.T, name string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = moduleRoot(t)
	return cmd.CombinedOutput()
}

// TestChaos_MLWorkerDown verifies the hot path stays isolated when the ML worker is absent.
// R1: control cohort /track p99 must remain < 80 ms with no ML runtime in cmd/tracker.
func TestChaos_MLWorkerDown(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	out, err := runAtModuleRoot(t, "go", "list", "-deps", "./cmd/tracker")
	require.NoError(t, err, string(out))
	deps := string(out)
	assert.NotContains(t, deps, "espx/internal/fraudscoring")
	assert.NotContains(t, deps, "github.com/zhongdai/go-lgbm")

	ctx := context.Background()
	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stack := startAdsIngestStackOpts(t, infra, "ads-chaos-ml-worker-down", adsIngestStackOpts{
		filterTimeoutMs: 2000,
		maxWorkers:      8,
		rateLimit:       1_000_000,
	})
	defer stack.Close(t)

	const samples = 200
	latencies := measureChaosTrackLatencies(t, stack.Handler, stack.CampaignID, 4, samples)
	require.NotEmpty(t, latencies)

	p99 := percentileDuration(latencies, 99)
	t.Logf("ml worker down control cohort n=%d p99=%v", len(latencies), p99)
	require.Less(t, p99, fraudScorerDownP99Limit, "control cohort p99 must stay <80 ms when ML worker is down")

	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, stack.CampaignID)

	logChaosProof(t, "fraud_worker_down", map[string]string{
		"subsystem":      "fraud_scoring",
		"control_n":      fmt.Sprintf("%d", len(latencies)),
		"control_p99_ms": fmt.Sprintf("%.3f", float64(p99.Microseconds())/1000),
		"import_fence":   "true",
		"r1":             "ok",
	})
}

// TestChaos_MLHotPathZeroAlloc verifies ML boost lookup on the filter path does not allocate.
func TestChaos_MLHotPathZeroAlloc(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	engine, evt, ctx := setupFilterFraudBoostBench(t)
	for i := 0; i < 100; i++ {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	}
	avg := testing.AllocsPerRun(100, func() {
		resetFraudBenchEvent(evt)
		_ = engine.Check(ctx, evt)
	})
	require.Equal(t, float64(0), avg, "ML boost filter path must be 0 allocs/op")

	logChaosProof(t, "fraud_hotpath_zero_alloc", map[string]string{
		"subsystem": "fraud_scoring",
		"allocs_op": "0",
	})
}
