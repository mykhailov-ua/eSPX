package ivtdetector

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fraudBoostCandidate(ip string) SuspiciousIP {
	return SuspiciousIP{
		IP:         ip,
		Reason:     "lightgbm",
		Score:      45,
		CampaignID: uuid.New().String(),
		Action:     "boost",
		Boost:      45,
		TTLSeconds: 300,
	}
}

// TestChaos_FraudOutboxBackpressure guards ML boost enqueue pauses when outbox depth exceeds limit.
func TestChaos_FraudOutboxBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := pool.Exec(ctx, `
			INSERT INTO outbox_events (event_type, payload, status)
			VALUES ('ML_SCORE_BOOST', '{"action":"boost"}', 'PENDING')`)
		require.NoError(t, err)
	}

	mgmt := &countingManagement{}
	detector := NewDetector(
		stubFinder{ips: []SuspiciousIP{fraudBoostCandidate("203.0.113.50")}},
		NewIdempotencyStore(pool),
		mgmt,
		pool,
		DetectorConfig{OutboxPendingLimit: 3},
	)

	result, err := detector.Run(ctx)
	require.ErrorIs(t, err, ErrOutboxBackpressure)
	assert.True(t, result.Backlogged)
	assert.Equal(t, 0, mgmt.count("203.0.113.50"))

	logChaosProof(t, "fraud_outbox_backpressure", map[string]string{
		"subsystem":           "fraud_scoring",
		"backpressure_active": "true",
		"pending_limit":       "3",
	})
}

// TestChaos_FraudExactlyOnce guards concurrent ML boost cycles enqueue exactly once per IP.
func TestChaos_FraudExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	ip := "203.0.113.51"
	mgmt := &countingManagement{}
	detector := NewDetector(
		stubFinder{ips: []SuspiciousIP{fraudBoostCandidate(ip)}},
		NewIdempotencyStore(pool),
		mgmt,
		pool,
		DetectorConfig{OutboxPendingLimit: 0},
	)

	const goroutines = 20
	var wg sync.WaitGroup
	var success atomic.Int32
	start := make(chan struct{})

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			result, err := detector.Run(ctx)
			if err == nil && result.Enqueued > 0 {
				success.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 1, mgmt.count(ip))
	assert.Equal(t, int32(1), success.Load())

	logChaosProof(t, "fraud_exactly_once", map[string]string{
		"subsystem":     "fraud_scoring",
		"goroutines":    "20",
		"enqueue_calls": "1",
		"exactly_once":  "true",
	})
}

// TestChaos_FraudManagementRetry guards failed ML enqueue releases idempotency for retry.
func TestChaos_FraudManagementRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	ip := "203.0.113.52"
	mgmt := &countingManagement{}
	mgmt.fail.Store(1)

	detector := NewDetector(
		stubFinder{ips: []SuspiciousIP{fraudBoostCandidate(ip)}},
		NewIdempotencyStore(pool),
		mgmt,
		pool,
		DetectorConfig{OutboxPendingLimit: 0},
	)

	_, err := detector.Run(ctx)
	require.Error(t, err)
	assert.Equal(t, 0, mgmt.count(ip))

	claimed, err := detector.idem.TryClaimFraudEnforcement(ctx, ip, "lightgbm", "boost")
	require.NoError(t, err)
	assert.True(t, claimed, "idempotency claim must be released after management failure")
	require.NoError(t, detector.idem.ReleaseFraudEnforcement(ctx, ip, "lightgbm", "boost"))

	mgmt.fail.Store(0)

	result, err := detector.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Enqueued)
	assert.Equal(t, 1, mgmt.count(ip))

	logChaosProof(t, "fraud_management_retry", map[string]string{
		"subsystem":     "fraud_scoring",
		"retry_ok":      "true",
		"enqueue_calls": "1",
		"exactly_once":  "true",
	})
}
