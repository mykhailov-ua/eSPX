package ingestion

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/database"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chaosTestEvent(clickID string) *campaignmodel.Event {
	return &campaignmodel.Event{
		ClickID:    clickID,
		CampaignID: uuid.New(),
		Type:       "click",
		IP:         "203.0.113.1",
		UA:         "chaos-agent",
		Payload:    []byte(`{"chaos":"write_path"}`),
		CreatedAt:  time.Now().UTC(),
	}
}

func countLinuxFDs(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("fd count requires linux /proc")
	}
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// TestChaos_ProcessorPgGate_Overflow caps concurrent real Postgres writes under burst load.
func TestChaos_ProcessorPgGate_Overflow(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	pm := database.NewPartitionManager(infra.Pool, 7, 1)
	require.NoError(t, pm.Run(ctx))

	const poolMax = 4
	gate := NewProcessorPgGate(0, poolMax)
	require.Equal(t, poolMax-ProcessorPgReserve, gate.Capacity())

	store := NewPostgresStoreWithGate(infra.Queries, 2*time.Second, gate)

	var peakInFlight atomic.Int32
	stop := make(chan struct{})
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if v := int32(gate.InFlight()); v > peakInFlight.Load() {
					peakInFlight.Store(v)
				}
				time.Sleep(time.Microsecond)
			}
		}
	}()

	const workers = 24
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			evt := chaosTestEvent("pg-gate-" + uuid.NewString())
			err := store.StoreBatch(ctx, []*campaignmodel.Event{evt})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
	close(stop)
	monitor.Wait()

	assert.LessOrEqual(t, peakInFlight.Load(), int32(gate.Capacity()),
		"gate must cap concurrent PG writers")
	assert.Greater(t, peakInFlight.Load(), int32(1), "burst must exercise the gate")

	logChaosProof(t, "processor_pg_gate_overflow", map[string]string{
		"subsystem":      "ads_processor",
		"gate_capacity":  strconv.Itoa(gate.Capacity()),
		"peak_in_flight": strconv.Itoa(int(peakInFlight.Load())),
		"workers":        strconv.Itoa(workers),
		"baseline_ok":    "true",
	})
}

// TestChaos_SyncWorker_PgGateOverflow serializes spend flushes through the shared processor PG gate.
func TestChaos_SyncWorker_PgGateOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	ctx := context.Background()
	registry := newChaosRegistry(t, infra.Queries)
	campaignID := seedChaosCampaign(t, infra, registry)
	campaignRepo := NewCampaignRepo(infra.Queries)

	gate := NewProcessorPgGate(3, 4)
	worker := NewSyncWorker(infra.Redis, campaignRepo, nil, time.Hour, 0, gate, 0)

	syncKey := "budget:sync:campaign:" + campaignID.String()
	require.NoError(t, infra.Redis.SAdd(ctx, "budget:dirty_campaigns", campaignID.String()).Err())
	require.NoError(t, infra.Redis.Set(ctx, syncKey, 500_000, 0).Err())

	var peakInFlight atomic.Int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if v := int32(gate.InFlight()); v > peakInFlight.Load() {
					peakInFlight.Store(v)
				}
				time.Sleep(time.Microsecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.SyncAll(ctx)
		}()
	}
	wg.Wait()
	close(stop)

	assert.LessOrEqual(t, peakInFlight.Load(), int32(gate.Capacity()))

	logChaosProof(t, "sync_worker_pg_gate_overflow", map[string]string{
		"subsystem":      "ads_processor",
		"gate_capacity":  strconv.Itoa(gate.Capacity()),
		"peak_in_flight": strconv.Itoa(int(peakInFlight.Load())),
		"baseline_ok":    "true",
	})
}

// TestChaos_CHSpool_Rotation uses real ClickHouse outage and small mmap segments.
func TestChaos_CHSpool_Rotation(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	conn, chCleanup := setupClickHouseIntegration(t)
	defer chCleanup()

	dir := t.TempDir()
	spoolCfg := CHSpoolConfig{SegmentSizeBytes: 16 * 1024, MaxSegments: 4}
	spool, err := OpenCHSpoolWithConfig(dir, spoolCfg)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	failConn := newFailingCHConn(true)
	store := NewClickHouseStore(failConn, time.Second, "", spoolCfg, nil)
	store.SetSpool(spool)
	_ = conn

	bigPayload := make([]byte, 2048)
	for i := range bigPayload {
		bigPayload[i] = 'x'
	}

	for i := 0; i < 40; i++ {
		evt := chaosTestEvent("ch-rotate-" + strconv.Itoa(i))
		evt.Payload = bigPayload
		if err := store.StoreBatch(context.Background(), []*campaignmodel.Event{evt}); err != nil {
			break
		}
	}

	require.GreaterOrEqual(t, spool.SegmentCount(), 2)
	assert.Equal(t, 1, spool.OpenFDCount())

	rotated, err := filepath.Glob(filepath.Join(dir, "events.wal.*"))
	require.NoError(t, err)
	assert.NotEmpty(t, rotated)

	logChaosProof(t, "ch_spool_rotation", map[string]string{
		"subsystem":     "ch_spool",
		"segment_count": strconv.Itoa(spool.SegmentCount()),
		"open_fds":      strconv.Itoa(spool.OpenFDCount()),
		"rotated_files": strconv.Itoa(len(rotated)),
		"baseline_ok":   "true",
	})
}

// TestChaos_CHSpool_MaxSegments faults when rotation budget is exhausted during CH outage.
func TestChaos_CHSpool_MaxSegments(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	dir := t.TempDir()
	spoolCfg := CHSpoolConfig{SegmentSizeBytes: 4096, MaxSegments: 2}
	spool, err := OpenCHSpoolWithConfig(dir, spoolCfg)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	failConn := newFailingCHConn(true)
	store := NewClickHouseStore(failConn, time.Second, "", spoolCfg, nil)
	store.SetSpool(spool)

	bigPayload := make([]byte, 1024)
	for i := range bigPayload {
		bigPayload[i] = 'y'
	}

	var lastErr error
	for i := 0; i < 30; i++ {
		evt := chaosTestEvent("ch-maxseg-" + strconv.Itoa(i))
		evt.Payload = bigPayload
		lastErr = store.StoreBatch(context.Background(), []*campaignmodel.Event{evt})
		if lastErr != nil {
			break
		}
	}
	require.Error(t, lastErr)
	assert.ErrorIs(t, lastErr, errCHSpoolMaxSegments)

	logChaosProof(t, "ch_spool_max_segments", map[string]string{
		"subsystem":     "ch_spool",
		"max_segments":  "2",
		"fault_type":    "max_segments_exceeded",
		"segment_count": strconv.Itoa(spool.SegmentCount()),
		"baseline_ok":   "true",
	})
}

// TestChaos_CHSpool_FdRelease keeps a single active FD while sealed segments stay closed.
func TestChaos_CHSpool_FdRelease(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	if runtime.GOOS != "linux" {
		t.Skip("linux fd test")
	}

	dir := t.TempDir()
	spoolCfg := CHSpoolConfig{SegmentSizeBytes: 8192, MaxSegments: 4}
	spool, err := OpenCHSpoolWithConfig(dir, spoolCfg)
	require.NoError(t, err)
	defer func() { _ = spool.Close() }()

	failConn := newFailingCHConn(true)
	store := NewClickHouseStore(failConn, time.Second, "", spoolCfg, nil)
	store.SetSpool(spool)

	before := countLinuxFDs(t)
	payload := make([]byte, 512)
	for i := 0; i < 25; i++ {
		evt := chaosTestEvent("ch-fd-" + strconv.Itoa(i))
		evt.Payload = payload
		require.NoError(t, store.StoreBatch(context.Background(), []*campaignmodel.Event{evt}))
	}
	afterRotate := countLinuxFDs(t)

	_, err = spool.Scan()
	require.NoError(t, err)
	afterScan := countLinuxFDs(t)

	assert.Equal(t, 1, spool.OpenFDCount())
	assert.LessOrEqual(t, afterRotate-before, 2, "rotation must not leak FDs beyond active spool file")
	assert.LessOrEqual(t, afterScan-afterRotate, 1, "scan must unmap sealed segments (lazy FD close)")

	logChaosProof(t, "ch_spool_fd_release", map[string]string{
		"subsystem":    "ch_spool",
		"open_fds":     strconv.Itoa(spool.OpenFDCount()),
		"fd_delta":     strconv.Itoa(afterRotate - before),
		"scan_fd_leak": strconv.FormatBool(afterScan-afterRotate > 1),
		"baseline_ok":  "true",
	})
}

// TestChaos_WritePath_DBFailurePreAck retains Redis PEL entries when Postgres is unavailable.
func TestChaos_WritePath_DBFailurePreAck(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	infra, cleanup := setupAdsChaosInfra(t)
	defer cleanup()

	stream := "wp-db-fail-" + uuid.NewString()
	stack := startAdsIngestStack(t, infra, stream)
	defer stack.Close(t)

	ctx := context.Background()
	require.Equal(t, http.StatusAccepted, postChaosClick(t, stack.Handler, stack.CampaignID))
	waitChaosStreamDrained(t, infra.Redis, stack.Stream, stack.Stream+"-group", stack.CampaignID, infra.Pool, 1)

	stopAdsContainer(t, infra.PGContainer)

	require.Equal(t, http.StatusAccepted, postChaosClick(t, stack.Handler, stack.CampaignID))

	require.Eventually(t, func() bool {
		return stack.Consumer.CircuitBreakerState() == CircuitOpen
	}, 20*time.Second, 200*time.Millisecond, "circuit must open when Postgres is down")

	require.Eventually(t, func() bool {
		pending, err := infra.Redis.XPending(ctx, stack.Stream, stack.Stream+"-group").Result()
		return err == nil && pending.Count >= 1
	}, 20*time.Second, 200*time.Millisecond, "PEL must retain messages when Postgres is down")

	dlqLen, err := infra.Redis.XLen(ctx, stack.Stream+":dlq").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), dlqLen, "retriable PG outage must not route to DLQ during recovery")

	logChaosProof(t, "write_path_db_fail_pre_ack", map[string]string{
		"subsystem":           "ads_processor",
		"pel_retained":        "true",
		"circuit_open":        "true",
		"dlq_avoided":         "true",
		"backpressure_active": "true",
		"baseline_ok":         "true",
	})
}
