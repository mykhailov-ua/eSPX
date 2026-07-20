package ingestion

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStaticSlotSharder_SnapshotAtomic_concurrentStress(t *testing.T) {
	t.Parallel()
	s := NewStaticSlotSharder(4)
	id := uuid.New()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 10_000; n++ {
			select {
			case <-done:
				return
			default:
				var table slotTable
				for j := range table {
					table[j] = uint8((j + n) % 4)
				}
				s.SwapSnapshot(int32((n%100)+1), &table, int64(n))
			}
		}
	}()

	for range 100_000 {
		shard := s.GetShard(id)
		if shard < 0 || shard >= 4 {
			t.Fatalf("shard %d out of range", shard)
		}
	}
	close(done)
	wg.Wait()
}

func TestStaticSlotSharder_SnapshotAtomic_versionTableBundle(t *testing.T) {
	t.Parallel()
	s := NewStaticSlotSharder(4)
	id := uuid.New()

	var table slotTable
	for i := range table {
		table[i] = uint8((i + 2) % 4)
	}
	s.SwapSnapshot(42, &table, 7)

	snap := s.Snapshot()
	if snap.Version != 42 {
		t.Fatalf("version=%d want 42", snap.Version)
	}
	if snap.MigrationGen != 7 {
		t.Fatalf("migration_gen=%d want 7", snap.MigrationGen)
	}
	slot := crc32Castagnoli(&id) & 1023
	want := int(snap.Table[slot])
	if want != s.GetShard(id) {
		t.Fatalf("GetShard %d != snapshot table %d", s.GetShard(id), want)
	}
	if s.ActiveVersion() != 42 {
		t.Fatalf("ActiveVersion %d != 42", s.ActiveVersion())
	}
}

func TestLocalQuotaCache_concurrentRace(t *testing.T) {
	t.Parallel()
	cache := NewLocalQuotaCache()
	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(seed int) {
			defer wg.Done()
			id := uuid.New()
			for j := range 1000 {
				now := int64((seed*1000 + j + 1) * int(time.Millisecond))
				if j%2 == 0 {
					cache.Block(id, now)
				} else {
					_ = cache.IsBlocked(id, now)
				}
			}
		}(i)
	}
	wg.Wait()
}
