package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	shardLoadSpikeWorkers   = 32
	shardLoadSpikeP99Limit  = 80 * time.Millisecond
	shardLoadSpikeBaselineN = 200
	shardLoadSpikePerWorker = 200 // 32×200 = 6400 events ≈ 10× baseline burst
)

// TestChaos_ShardLoadSpike automates EDGE.md Part III §7.3 sharp load rise on a pinned control cohort.
// CI: 32 concurrent gnet workers × 200 impressions (R5). Sustained 30 s 10× ramp: scripts/load-test/k6_spike_traffic.js.
func TestChaos_ShardLoadSpike(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stack := startAdsIngestStackOpts(t, infra, "ads-chaos-shard-load-spike", adsIngestStackOpts{
		filterTimeoutMs: 2000,
		maxWorkers:      shardLoadSpikeWorkers,
		rateLimit:       10_000_000,
	})
	defer stack.Close(t)

	controlID := stack.CampaignID
	baseline := measureChaosTrackLatencies(t, stack.Handler, controlID, 1, shardLoadSpikeBaselineN)
	baselineP99 := percentileDuration(baseline, 99)
	t.Logf("baseline n=%d p99=%v", len(baseline), baselineP99)

	var (
		spikeLatencies []time.Duration
		spikeMu        sync.Mutex
		errCount       atomic.Uint64
		okCount        atomic.Uint64
	)

	var wg sync.WaitGroup
	wg.Add(shardLoadSpikeWorkers)
	for w := 0; w < shardLoadSpikeWorkers; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			userPrefix := fmt.Sprintf("spike-w%d-", workerID)
			for i := 0; i < shardLoadSpikePerWorker; i++ {
				clickID := uuid.NewString()
				userID := userPrefix + clickID[:8]
				start := time.Now()
				status := postChaosTrackWait(t, stack.Handler, controlID, "impression", userID, clickID)
				elapsed := time.Since(start)
				if status == http.StatusAccepted || status == http.StatusOK {
					okCount.Add(1)
					spikeMu.Lock()
					spikeLatencies = append(spikeLatencies, elapsed)
					spikeMu.Unlock()
				} else {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.NotEmpty(t, spikeLatencies, "spike phase must record latencies")
	total := okCount.Load() + errCount.Load()
	errPct := float64(errCount.Load()) / float64(total) * 100
	spikeP99 := percentileDuration(spikeLatencies, 99)
	t.Logf("spike workers=%d per_worker=%d n=%d p99=%v ok=%d err=%d err_pct=%.3f",
		shardLoadSpikeWorkers, shardLoadSpikePerWorker, len(spikeLatencies), spikeP99,
		okCount.Load(), errCount.Load(), errPct)

	require.GreaterOrEqual(t, okCount.Load(), uint64(shardLoadSpikeBaselineN*10),
		"spike must reach at least 10× baseline event count")
	require.Less(t, errPct, 0.1, "control cohort error rate must stay <0.1%% during spike")
	require.Less(t, spikeP99, shardLoadSpikeP99Limit,
		"control cohort p99 must stay <80 ms during sharp load rise")

	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, controlID)

	var budgetLimit, currentSpend int64
	err := infra.Pool.QueryRow(ctx,
		`SELECT budget_limit, current_spend FROM campaigns WHERE id = $1`, ToUUID(controlID),
	).Scan(&budgetLimit, &currentSpend)
	require.NoError(t, err)
	require.LessOrEqual(t, currentSpend, budgetLimit, "PG spend ceiling (R5)")

	logChaosProof(t, "shard_load_spike", map[string]string{
		"workers":         fmt.Sprintf("%d", shardLoadSpikeWorkers),
		"per_worker":      fmt.Sprintf("%d", shardLoadSpikePerWorker),
		"baseline_p99_ms": fmt.Sprintf("%.3f", float64(baselineP99.Microseconds())/1000),
		"spike_p99_ms":    fmt.Sprintf("%.3f", float64(spikeP99.Microseconds())/1000),
		"spike_n":         fmt.Sprintf("%d", len(spikeLatencies)),
		"err_pct":         fmt.Sprintf("%.3f", errPct),
		"r5":              "ok",
	})
}

func postChaosTrackWait(t *testing.T, h *AdsPacketHandler, campaignID uuid.UUID, evtType, userID, clickID string) int {
	t.Helper()
	payload := map[string]any{
		"campaign_id": campaignID,
		"type":        evtType,
		"click_id":    clickID,
		"user_id":     userID,
		"payload":     map[string]string{"chaos": "1"},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	status, _ := PostTrackGnetJSONWait(h, body, 5*time.Second)
	return status
}

func measureChaosTrackLatencies(t *testing.T, h *AdsPacketHandler, campaignID uuid.UUID, workers, total int) []time.Duration {
	t.Helper()
	if workers < 1 {
		workers = 1
	}
	perWorker := total / workers
	rem := total % workers

	var (
		mu        sync.Mutex
		latencies []time.Duration
		wg        sync.WaitGroup
	)

	wg.Add(workers)
	for w := 0; w < workers; w++ {
		n := perWorker
		if w < rem {
			n++
		}
		workerID := w
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, n)
			prefix := fmt.Sprintf("base-w%d-", workerID)
			for i := 0; i < n; i++ {
				clickID := uuid.NewString()
				payload := map[string]any{
					"campaign_id": campaignID,
					"type":        "impression",
					"click_id":    clickID,
					"user_id":     prefix + clickID[:8],
					"payload":     map[string]string{"chaos": "spike-baseline"},
				}
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				start := time.Now()
				status, _ := PostTrackGnetJSONWait(h, body, 5*time.Second)
				local = append(local, time.Since(start))
				require.True(t, status == http.StatusAccepted || status == http.StatusOK,
					"baseline track status=%d", status)
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return latencies
}

func percentileDuration(samples []time.Duration, pct int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if pct < 1 {
		pct = 1
	}
	if pct > 100 {
		pct = 100
	}
	cp := append([]time.Duration(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := len(cp)*pct/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}
