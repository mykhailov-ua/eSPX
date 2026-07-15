//go:build !race

package ingestion

import (
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkIngressQuota_padded measures cache-line padded per-worker counters.
func BenchmarkIngressQuota_padded(b *testing.B) {
	const workers = 8
	var limits UDPControlLimits
	limits.NumShards = 1
	limits.Limits[0] = 1_000_000_000
	m := buildIngressQuotaMap(1, &limits, workers)
	b.SetParallelism(workers)
	b.RunParallel(func(pb *testing.PB) {
		worker := int(atomic.AddUint64(new(uint64), 1)-1) % workers
		for pb.Next() {
			_ = m.tryAcquire(0, worker)
		}
	})
}

// BenchmarkIngressQuota_unpadded measures false-sharing baseline (packed atomics).
func BenchmarkIngressQuota_unpadded(b *testing.B) {
	const workers = 8
	m := &unpaddedIngressCounters{max: 1_000_000_000}
	b.SetParallelism(workers)
	b.RunParallel(func(pb *testing.PB) {
		worker := int(atomic.AddUint64(new(uint64), 1)-1) % workers
		for pb.Next() {
			_ = m.tryAcquire(worker)
		}
	})
}

func TestIngressQuota_falseSharingRatio(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const workers = 8
	const iters = 500_000

	var limits UDPControlLimits
	limits.NumShards = 1
	limits.Limits[0] = 1_000_000_000
	padded := buildIngressQuotaMap(1, &limits, workers)
	unpadded := &unpaddedIngressCounters{max: 1_000_000_000}

	paddedNs := benchIngressWorkers(padded, unpadded, workers, iters, true)
	unpaddedNs := benchIngressWorkers(padded, unpadded, workers, iters, false)
	ratio := float64(unpaddedNs) / float64(paddedNs)
	t.Logf("ingress quota padded=%dns unpadded=%dns ratio=%.2fx", paddedNs, unpaddedNs, ratio)
	if ratio < 3.0 {
		t.Fatalf("expected padded >=3x throughput on %d workers, got %.2fx", workers, ratio)
	}
}

func benchIngressWorkers(padded *ingressQuotaMap, unpadded *unpaddedIngressCounters, workers, iters int, usePadded bool) int64 {
	var wg sync.WaitGroup
	start := monotonicNano()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if usePadded {
					_ = padded.tryAcquire(0, worker)
				} else {
					_ = unpadded.tryAcquire(worker)
				}
			}
		}(w)
	}
	wg.Wait()
	return monotonicNano() - start
}
