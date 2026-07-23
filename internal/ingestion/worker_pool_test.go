package ingestion

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPinnedWorkerPool(t *testing.T) {
	pool := NewPinnedWorkerPool(4, 1024)
	defer pool.Shutdown()

	var counter int64
	var wg sync.WaitGroup
	numTasks := 10000

	wg.Add(numTasks)
	for i := 0; i < numTasks; i++ {
		ctx := &connContext{
			offloadWG: &wg,
			offloadOnEnter: func() {
				atomic.AddInt64(&counter, 1)
			},
		}
		submitted := pool.SubmitOffload(ctx)
		if !submitted {
			wg.Done()
			t.Errorf("failed to submit task %d", i)
		}
	}

	wg.Wait()

	if atomic.LoadInt64(&counter) != int64(numTasks) {
		t.Errorf("expected counter to be %d, got %d", numTasks, counter)
	}
}

func TestPinnedWorkerPool_ZeroAlloc(t *testing.T) {
	pool := NewPinnedWorkerPool(4, 1024)
	defer pool.Shutdown()

	ctxs := make([]connContext, 1000)
	allocs := testing.AllocsPerRun(1, func() {
		for i := range ctxs {
			if !pool.SubmitOffload(&ctxs[i]) {
				t.Fatal("submit failed")
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("SubmitOffload allocs = %v, want 0", allocs)
	}
}

// Tracks pinned worker pool submit cost for gnet handler capacity planning.
func BenchmarkPinnedWorkerPool(b *testing.B) {
	benchmarks := []struct {
		name      string
		workers   int
		queueSize int
	}{
		{"Workers4_Queue1024", 4, 1024},
		{"Workers8_Queue4096", 8, 4096},
		{"Workers16_Queue8192", 16, 8192},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			pool := NewPinnedWorkerPool(bm.workers, bm.queueSize)
			defer pool.Shutdown()

			const ring = 4096
			ctxs := make([]connContext, ring)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx := &ctxs[i%ring]
				for !pool.SubmitOffload(ctx) {
					runtime.Gosched()
				}
			}
		})
	}
}
