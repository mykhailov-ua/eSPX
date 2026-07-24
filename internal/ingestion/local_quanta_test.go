//go:build !race

package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLocalQuantaLedger_TrySpendLocal(t *testing.T) {
	ledger := NewLocalQuantaLedger()
	ledger.SetMode("live")
	id := uuid.New()
	ledger.Credit(id, 1_000_000, 1_000_000)

	require.True(t, ledger.TrySpendLocal(id, 100_000))
	require.Equal(t, int64(900_000), ledger.Remaining(id))
	require.False(t, ledger.TrySpendLocal(id, 2_000_000))
}

func TestLocalQuantaLedger_NeedsRefill(t *testing.T) {
	ledger := NewLocalQuantaLedger()
	id := uuid.New()
	ledger.Credit(id, 1_000_000, 1_000_000)
	require.False(t, ledger.NeedsRefill(id, 20))
	ledger.TrySpendLocal(id, 850_000)
	require.True(t, ledger.NeedsRefill(id, 20))
}

func TestLocalQuantaLedger_concurrentSpend(t *testing.T) {
	ledger := NewLocalQuantaLedger()
	id := uuid.New()
	const chunk = int64(10_000_000)
	const spend = int64(10_000)
	ledger.Credit(id, chunk, chunk)

	var wg sync.WaitGroup
	var okCount atomic.Int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if ledger.TrySpendLocal(id, spend) {
					okCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, chunk/spend, okCount.Load())
	require.Equal(t, int64(0), ledger.Remaining(id))
}

func TestLocalQuantaStrict_hysteresis(t *testing.T) {
	strict := NewLocalQuantaStrict(5_000_000, 8_000_000)
	id := uuid.New()
	strict.UpdateFromRedisRemaining(id, 4_000_000)
	require.True(t, strict.IsStrict(id))
	strict.UpdateFromRedisRemaining(id, 6_000_000)
	require.True(t, strict.IsStrict(id))
	strict.UpdateFromRedisRemaining(id, 9_000_000)
	require.False(t, strict.IsStrict(id))
}

func TestAdaptiveChunkSize_bounds(t *testing.T) {
	chunk := AdaptiveChunkSize(0, 500_000, 50_000_000, 5_000_000)
	require.Equal(t, int64(5_000_000), chunk)
	chunk = AdaptiveChunkSize(10_000, 500_000, 50_000_000, 5_000_000)
	require.Equal(t, int64(50_000_000), chunk)
	chunk = AdaptiveChunkSize(1, 500_000, 50_000_000, 5_000_000)
	require.Equal(t, int64(500_000), chunk)
}

func TestBudgetDeltaAggregator_pending(t *testing.T) {
	agg := NewBudgetDeltaAggregator()
	id := uuid.New()
	agg.Record(id, 100_000)
	pending, err := agg.PendingDeltaMicro(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, int64(100_000), pending)
	agg.MarkFlushed(id, 50_000)
	pending, err = agg.PendingDeltaMicro(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, int64(50_000), pending)
}

func TestQuotaRefillWorker_recoverFromDeltas(t *testing.T) {
	ledger := NewLocalQuantaLedger()
	w := &QuotaRefillWorker{ledger: ledger, baseChunk: 5_000_000}
	id := uuid.New()
	w.RecoverFromDeltas(map[uuid.UUID]int64{id: 2_000_000})
	require.Equal(t, int64(2_000_000), ledger.Remaining(id))
}
