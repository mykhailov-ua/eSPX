package licensing

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func logEntitlementChaosProof(t *testing.T, fault string, fields map[string]string) {
	t.Helper()
	var b []byte
	b = append(b, "chaos_proof fault="...)
	b = append(b, fault...)
	for k, v := range fields {
		b = append(b, ' ')
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, v...)
	}
	t.Log(string(b))
}

// TestChaos_EntitlementBufferOOMProtection proves bounded channel rejects overload without panic.
func TestChaos_EntitlementBufferOOMProtection(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())

	const capacity = 8
	var applied int
	var mu sync.Mutex
	buf := NewEntitlementSyncBuffer(capacity, func(ctx context.Context, customerID uuid.UUID) error {
		mu.Lock()
		applied++
		mu.Unlock()
		return nil
	})
	buf.Start(ctx)

	var rejected int
	for i := 0; i < 64; i++ {
		if err := buf.Enqueue(uuid.New()); err != nil {
			require.ErrorIs(t, err, ErrEntitlementBufferFull)
			rejected++
		}
	}
	require.Greater(t, rejected, 0, "buffer must reject overload")

	cancel()
	buf.Stop()

	logEntitlementChaosProof(t, "entitlement_buffer_oom_guard", map[string]string{
		"subsystem": "licensing",
		"capacity":  strconv.Itoa(capacity),
		"rejected":  strconv.Itoa(rejected),
		"applied":   strconv.Itoa(applied),
	})
}

// TestChaos_EntitlementBufferRecover replays pending IDs after simulated restart.
func TestChaos_EntitlementBufferRecover(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	ctx, cancel := context.WithCancel(context.Background())

	var seen []uuid.UUID
	var mu sync.Mutex
	buf := NewEntitlementSyncBuffer(4, func(ctx context.Context, customerID uuid.UUID) error {
		mu.Lock()
		seen = append(seen, customerID)
		mu.Unlock()
		return nil
	})
	buf.Start(ctx)

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	buf.Recover(ctx, ids)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= len(ids)
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
	buf.Stop()

	logEntitlementChaosProof(t, "entitlement_buffer_recovery", map[string]string{
		"subsystem": "licensing",
		"replayed":  strconv.Itoa(len(ids)),
	})
}
