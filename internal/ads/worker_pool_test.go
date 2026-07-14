package ads

import (
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
		submitted := pool.Submit(func(_ int) {
			atomic.AddInt64(&counter, 1)
			wg.Done()
		})
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

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.Submit(func(_ int) {})
			}
		})
	}
}
