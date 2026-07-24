package ingestion

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/metrics"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	p1WorkerPoolWorkers   = 2
	p1WorkerPoolQueue     = 2
	p1WorkerPoolSpikeReqs = 64
	p1FraudBurstProducers = 16
	p1FraudBurstPerProd   = 256
	p1TrackSpikeWorkers   = 8
	p1TrackSpikePerWorker = 40
)

// TestChaos_PinnedWorkerPoolSaturationSpike verifies overload rejections when the pool is saturated.
func TestChaos_PinnedWorkerPoolSaturationSpike(t *testing.T) {
	pool := NewPinnedWorkerPool(p1WorkerPoolWorkers, p1WorkerPoolQueue)
	unblock := make(chan struct{})
	defer func() {
		close(unblock)
		pool.Shutdown()
	}()

	started := make(chan struct{}, p1WorkerPoolWorkers)
	for i := 0; i < p1WorkerPoolWorkers; i++ {
		ctx := &connContext{
			offloadOnEnter: func() { started <- struct{}{} },
			offloadBlock:   unblock,
		}
		require.True(t, pool.SubmitOffload(ctx))
	}
	for i := 0; i < p1WorkerPoolWorkers; i++ {
		<-started
	}
	for i := 0; i < p1WorkerPoolQueue; i++ {
		require.True(t, pool.SubmitOffload(&connContext{offloadBlock: unblock}))
	}

	before := testutil.ToFloat64(metrics.WorkerPoolRejectTotal)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.SetWorkerPool(pool)

	var overloads atomic.Int32
	var wg sync.WaitGroup
	wg.Add(p1WorkerPoolSpikeReqs)
	for i := 0; i < p1WorkerPoolSpikeReqs; i++ {
		go func() {
			defer wg.Done()
			body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
			_, conn := ServeGnetHarness(h, BuildGnetPostTrackJSON(body))
			if bytes.Contains(conn.Written(), []byte("server overloaded")) {
				overloads.Add(1)
			}
		}()
	}
	wg.Wait()

	after := testutil.ToFloat64(metrics.WorkerPoolRejectTotal)
	require.Greater(t, overloads.Load(), int32(p1WorkerPoolSpikeReqs/4))
	require.Greater(t, after, before)

	logChaosProof(t, "worker_pool_saturation_spike", map[string]string{
		"requests":  fmt.Sprintf("%d", p1WorkerPoolSpikeReqs),
		"overloads": fmt.Sprintf("%d", overloads.Load()),
		"rejects":   fmt.Sprintf("%.0f", after-before),
	})
}

// TestChaos_FraudStreamRingOverflowSpike fills the fraud ring and asserts lossy drop behavior.
func TestChaos_FraudStreamRingOverflowSpike(t *testing.T) {
	q := &FraudStreamWriter{
		stream: "fraud-stream-chaos",
		maxLen: 1000,
		stopCh: make(chan struct{}),
	}
	q.allocCursor = fraudRingUsable - 32
	q.writeCursor = q.allocCursor

	before := testutil.ToFloat64(metrics.FraudStreamDropTotal)
	evt := &campaignmodel.Event{
		ClickID:     "chaos-fraud",
		CampaignID:  uuid.New(),
		Type:        "click",
		FraudReason: "chaos",
	}

	var wg sync.WaitGroup
	wg.Add(p1FraudBurstProducers)
	for p := 0; p < p1FraudBurstProducers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < p1FraudBurstPerProd; i++ {
				enqueueFraudReject(q, 0, evt)
			}
		}()
	}
	wg.Wait()

	after := testutil.ToFloat64(metrics.FraudStreamDropTotal)
	require.Greater(t, after, before)

	logChaosProof(t, "fraud_stream_ring_overflow", map[string]string{
		"producers": fmt.Sprintf("%d", p1FraudBurstProducers),
		"per_prod":  fmt.Sprintf("%d", p1FraudBurstPerProd),
		"drops":     fmt.Sprintf("%.0f", after-before),
	})
}

// TestChaos_TrackUnderWorkerPoolSpike runs real ingest with a pinned pool under concurrent load.
func TestChaos_TrackUnderWorkerPoolSpike(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx := context.Background()
	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stack := startAdsIngestStackOpts(t, infra, "ads-chaos-worker-pool-spike", adsIngestStackOpts{
		filterTimeoutMs: 2000,
		maxWorkers:      8,
		rateLimit:       1_000_000,
		useStaticSlot:   true,
	})
	defer stack.Close(t)

	pool := NewPinnedWorkerPool(4, 16)
	stack.Handler.SetWorkerPool(pool)
	defer pool.Shutdown()

	var (
		okCount atomic.Int32
		wg      sync.WaitGroup
	)
	wg.Add(p1TrackSpikeWorkers)
	for w := 0; w < p1TrackSpikeWorkers; w++ {
		w := w
		go func() {
			defer wg.Done()
			prefix := fmt.Sprintf("wp-w%d-", w)
			for i := 0; i < p1TrackSpikePerWorker; i++ {
				status := postChaosImpression(t, stack.Handler, stack.CampaignID, prefix+fmt.Sprintf("%d", i))
				if status == http.StatusAccepted || status == http.StatusOK {
					okCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	total := p1TrackSpikeWorkers * p1TrackSpikePerWorker
	require.Greater(t, int(okCount.Load()), total*9/10, "majority of tracks must accept under worker pool spike")
	AssertBudgetInvariant(t, ctx, infra.Pool, infra.Redis, stack.CampaignID)

	rejects := testutil.ToFloat64(metrics.WorkerPoolRejectTotal)
	logChaosProof(t, "track_worker_pool_spike", map[string]string{
		"workers":      fmt.Sprintf("%d", p1TrackSpikeWorkers),
		"per_worker":   fmt.Sprintf("%d", p1TrackSpikePerWorker),
		"accepted":     fmt.Sprintf("%d", okCount.Load()),
		"pool_rejects": fmt.Sprintf("%.0f", rejects),
		"budget_ok":    "true",
	})
}
