package blocklist

import (
	"sync"
	"testing"
)

// TestStore_concurrentApplyDiff guards the deny snapshot against concurrent rewrites.
// edge-bpf-sync serializes runSync today; post-violation early sync plus scheduled sync
// would race on Store without the mutex.
func TestStore_concurrentApplyDiff(t *testing.T) {
	m := newLPMMap(t)
	store := NewStore()

	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			ip := []string{
				"198.51.100.1",
				"198.51.100.2",
				"203.0.113.10",
				"203.0.113.11",
			}[i%4]
			_, _, _ = store.ApplyDiff(m, []string{ip}, nil, nil)
		}()
	}
	wg.Wait()
	if store.Len() == 0 {
		t.Fatal("expected at least one deny entry after concurrent apply")
	}
}
