package notifier

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"espx/internal/notifier/pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_notifierConcurrentDelivery verifies concurrent ProcessPending calls do not double-send.
// Hypothesis: exactly one provider Send per notification ID under 20+ concurrent worker polls.
func TestChaos_notifierConcurrentDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("notifier chaos integration test")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(100, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}
	svc := NewService(pool, providers)
	ctx := context.Background()

	const notifications = 5
	for i := range notifications {
		_, err := svc.SendNotification(ctx, &pb.SendNotificationRequest{
			Provider:  pb.Provider_PROVIDER_TELEGRAM,
			Recipient: fmt.Sprintf("chat-%d", i),
			Title:     "Concurrent test",
			Body:      fmt.Sprintf("body %d", i),
		})
		require.NoError(t, err)
	}

	var (
		wg          sync.WaitGroup
		processed   atomic.Int32
		errs        atomic.Int32
		workerCount = 24
	)

	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := svc.ProcessPending(ctx, workerBatchSize)
			if err != nil {
				errs.Add(1)
				return
			}
			processed.Add(int32(n))
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), errs.Load())
	assert.Equal(t, int32(notifications), processed.Load())
	assert.Len(t, mockProv.Sent, notifications)

	logChaosProof(t, "notifier_concurrent_delivery", map[string]string{
		"workers":       fmt.Sprintf("%d", workerCount),
		"notifications": fmt.Sprintf("%d", notifications),
		"sent_total":    fmt.Sprintf("%d", len(mockProv.Sent)),
		"double_send":   "false",
	})
}
