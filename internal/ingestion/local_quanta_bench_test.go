//go:build !race

package ingestion

import (
	"testing"

	"github.com/google/uuid"
)

// BenchmarkLocalQuantaSpend measures hot-path local quanta debit (M8-01 DoD).
func BenchmarkLocalQuantaSpend(b *testing.B) {
	ledger := NewLocalQuantaLedger()
	ledger.SetMode("live")
	id := uuid.New()
	ledger.Credit(id, 1_000_000_000, 1_000_000_000)
	const amount = int64(10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !ledger.TrySpendLocal(id, amount) {
			ledger.Credit(id, 1_000_000_000, 1_000_000_000)
		}
	}
}

// BenchmarkLocalQuantaSpend_1M verifies 1M ops < 50ms target from M8 DoD.
func TestLocalQuantaSpend_1M_under50ms(t *testing.T) {
	ledger := NewLocalQuantaLedger()
	id := uuid.New()
	ledger.Credit(id, 100_000_000_000, 100_000_000_000)
	const (
		iters  = 1_000_000
		amount = int64(10_000)
	)
	start := monotonicNano()
	for i := 0; i < iters; i++ {
		if !ledger.TrySpendLocal(id, amount) {
			t.Fatal("unexpected exhaust")
		}
	}
	elapsed := monotonicNano() - start
	t.Logf("LocalQuantaSpend 1M ops: %v", elapsed)
	if elapsed > 50_000_000 {
		t.Fatalf("1M TrySpendLocal took %dns, want <50ms", elapsed)
	}
}

// BenchmarkLocalQuantaSpend_parallel measures campaign-global pool under workers (M8-08).
func BenchmarkLocalQuantaSpend_parallel(b *testing.B) {
	ledger := NewLocalQuantaLedger()
	id := uuid.New()
	ledger.Credit(id, int64(b.N)*10_000+1_000_000, 1_000_000_000)
	const amount = int64(10_000)
	b.SetParallelism(8)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ledger.TrySpendLocal(id, amount)
		}
	})
}
