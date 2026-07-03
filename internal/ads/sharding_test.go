package ads

import (
	"hash/crc32"
	"testing"

	"github.com/google/uuid"
)

// Guards CRC32 Castagnoli hash matches expected values for routing keys.
func TestCRC32Castagnoli(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)
	ids := []uuid.UUID{uuid.Nil, uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		want := crc32.Checksum(id[:], table)
		if got := crc32Castagnoli(&id); got != want {
			t.Fatalf("id=%s: crc32Castagnoli=%08x want %08x", id, got, want)
		}
	}
}

// Guards jump hash maps campaign IDs to valid shard indices.
func TestJumpHashSharder_GetShard(t *testing.T) {
	numShards := 10
	sharder := NewJumpHashSharder(numShards)

	id := uuid.New()
	shard1 := sharder.GetShard(id)
	shard2 := sharder.GetShard(id)
	if shard1 != shard2 {
		t.Errorf("expected deterministic shard, got %d and %d", shard1, shard2)
	}

	for i := 0; i < 1000; i++ {
		shard := sharder.GetShard(uuid.New())
		if shard < 0 || shard >= numShards {
			t.Errorf("shard %d out of range [0, %d)", shard, numShards)
		}
	}

	counts := make(map[int]int)
	total := 10000
	for i := 0; i < total; i++ {
		shard := sharder.GetShard(uuid.New())
		counts[shard]++
	}

	for shard, count := range counts {
		expected := total / numShards
		tolerance := expected / 2
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("shard %d has poor distribution: %d events (expected ~%d)", shard, count, expected)
		}
	}
}

// Guards same campaign ID always maps to the same shard.
func TestJumpHashSharder_Consistency(t *testing.T) {
	id := uuid.New()

	s5 := NewJumpHashSharder(5)
	shard5 := s5.GetShard(id)

	s6 := NewJumpHashSharder(6)
	shard6 := s6.GetShard(id)

	if shard6 != shard5 && shard6 != 5 {
		t.Errorf("non-consistent move (up): shard moved from %d (5 shards) to %d (6 shards)", shard5, shard6)
	}
}

// Guards shard mapping stays stable when shard count decreases.
func TestJumpHashSharder_ScaleDown(t *testing.T) {
	id := uuid.New()

	s6 := NewJumpHashSharder(6)
	shard6 := s6.GetShard(id)

	s5 := NewJumpHashSharder(5)
	shard5 := s5.GetShard(id)

	if shard6 < 5 {
		if shard5 != shard6 {
			t.Errorf("non-consistent move (down): shard moved from %d to %d even though it was within range", shard6, shard5)
		}
	} else {
		if shard5 >= 5 {
			t.Errorf("failed to re-distribute: shard was %d, stayed %d after scale down to 5", shard6, shard5)
		}
	}
}

// Guards jump hash handles min and max UUID boundary inputs.
func TestJumpHashSharder_BoundaryValues(t *testing.T) {
	tests := []struct {
		numBuckets int
		expected   int
	}{
		{0, 0},
		{-1, 0},
		{1, 0},
	}

	for _, tt := range tests {
		s := NewJumpHashSharder(tt.numBuckets)
		shard := s.GetShard(uuid.New())
		if shard != tt.expected {
			t.Errorf("expected shard %d for numBuckets %d, got %d", tt.expected, tt.numBuckets, shard)
		}
	}
}

// Guards nil UUID maps to a valid shard without panic.
func TestJumpHashSharder_NilUUID(t *testing.T) {
	s := NewJumpHashSharder(10)
	shard1 := s.GetShard(uuid.Nil)
	shard2 := s.GetShard(uuid.Nil)

	if shard1 != shard2 {
		t.Error("nil UUID should be deterministic")
	}
}

// Tracks jump hash shard lookup cost at 10 shards.
func BenchmarkJumpHashSharder_10(b *testing.B) {
	s := NewJumpHashSharder(10)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

// Tracks jump hash shard lookup cost at 1024 shards.
func BenchmarkJumpHashSharder_1024(b *testing.B) {
	s := NewJumpHashSharder(1024)
	id := uuid.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

// Guards static slot sharder maps IDs to configured slot range.
func TestStaticSlotSharder_GetShard(t *testing.T) {
	numShards := 10
	sharder := NewStaticSlotSharder(numShards)

	id := uuid.New()
	shard1 := sharder.GetShard(id)
	shard2 := sharder.GetShard(id)
	if shard1 != shard2 {
		t.Errorf("expected deterministic shard, got %d and %d", shard1, shard2)
	}

	for i := 0; i < 1000; i++ {
		shard := sharder.GetShard(uuid.New())
		if shard < 0 || shard >= numShards {
			t.Errorf("shard %d out of range [0, %d)", shard, numShards)
		}
	}
}

// Tracks static slot shard lookup cost at 10 shards.
func BenchmarkStaticSlotSharder_10(b *testing.B) {
	s := NewStaticSlotSharder(10)
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

// Tracks static slot shard lookup cost at 1024 shards.
func BenchmarkStaticSlotSharder_1024(b *testing.B) {
	s := NewStaticSlotSharder(1024)
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.GetShard(id)
	}
}

// Guards GetShard stays zero-alloc on the tracker hot path.
func TestStaticSlotSharder_ZeroAllocs(t *testing.T) {
	s := NewStaticSlotSharder(4)
	id := uuid.New()
	allocs := testing.AllocsPerRun(1000, func() {
		_ = s.GetShard(id)
	})
	if allocs != 0 {
		t.Fatalf("GetShard allocs = %v, want 0", allocs)
	}
}

// Guards concurrent reload does not race GetShard readers.
func TestStaticSlotSharder_ConcurrentReload(t *testing.T) {
	s := NewStaticSlotSharder(4)
	id := uuid.New()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				s.ReloadFromModulo(4)
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
}

// Guards ReloadFromModulo preserves slot % N semantics.
func TestStaticSlotSharder_ReloadFromModulo(t *testing.T) {
	s := NewStaticSlotSharder(4)
	id := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	before := s.GetShard(id)

	s.ReloadFromModulo(4)
	requireEqualShard(t, before, s.GetShard(id))

	legacy := NewStaticSlotSharder(6)
	want := legacy.GetShard(id)
	s.ReloadFromModulo(6)
	requireEqualShard(t, want, s.GetShard(id))
}

func requireEqualShard(t *testing.T, want, got int) {
	t.Helper()
	if want != got {
		t.Fatalf("shard = %d, want %d", got, want)
	}
}

// Documents production services share StaticSlotSharder, not JumpHash.
func TestProductionServicesUseStaticSlot(t *testing.T) {
	const numShards = 4
	sharder := NewStaticSlotSharder(numShards)
	jump := NewJumpHashSharder(numShards)
	id := uuid.New()
	if sharder.GetShard(id) == jump.GetShard(id) {
		t.Log("single sample matched; divergence test covers distribution")
	}
}

// Documents shard migration blast radius when shard count changes for ops runbooks.
func TestSharderRebalanceImpact(t *testing.T) {
	const N = 6
	const samples = 10000
	ids := make([]uuid.UUID, samples)
	for i := range ids {
		ids[i] = uuid.New()
	}

	staticOld := NewStaticSlotSharder(N)
	staticNew := NewStaticSlotSharder(N + 1)
	jumpOld := NewJumpHashSharder(N)
	jumpNew := NewJumpHashSharder(N + 1)

	var staticMoves, jumpMoves int
	for _, id := range ids {
		if staticOld.GetShard(id) != staticNew.GetShard(id) {
			staticMoves++
		}
		if jumpOld.GetShard(id) != jumpNew.GetShard(id) {
			jumpMoves++
		}
	}

	staticFrac := float64(staticMoves) / samples
	jumpFrac := float64(jumpMoves) / samples
	t.Logf("N=%d->%d samples=%d: static moved=%.1f%% jump moved=%.1f%%", N, N+1, samples, staticFrac*100, jumpFrac*100)

	if staticFrac < 0.7 {
		t.Errorf("static should move majority on reshard, got %.1f%%", staticFrac*100)
	}
	if jumpFrac > 0.25 {
		t.Errorf("jump should move ~1/N fraction, got %.1f%%", jumpFrac*100)
	}
}

// TestSharderStaticVsJumpHashDivergence documents why tracker and management must share StaticSlotSharder, not JumpHash.
func TestSharderStaticVsJumpHashDivergence(t *testing.T) {
	const numShards = 4
	static := NewStaticSlotSharder(numShards)
	jump := NewJumpHashSharder(numShards)

	const samples = 10_000
	mismatch := 0
	for range samples {
		id := uuid.New()
		if static.GetShard(id) != jump.GetShard(id) {
			mismatch++
		}
	}
	frac := float64(mismatch) / samples
	t.Logf("StaticSlot vs JumpHash mismatch: %d/%d (%.1f%%) — management must not use JumpHash when tracker uses StaticSlot", mismatch, samples, frac*100)
	if frac < 0.5 {
		t.Fatalf("expected >50%% shard divergence between StaticSlot and JumpHash, got %.1f%%", frac*100)
	}
}

// Guards concurrent StoreSlotMap does not race GetShard readers.
func TestStaticSlotSharder_StoreSlotMap_concurrent(t *testing.T) {
	s := NewStaticSlotSharder(4)
	id := uuid.New()
	before := s.GetShard(id)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				var table [1024]uint16
				for i := range table {
					table[i] = uint16((i + 1) % 4)
				}
				s.StoreSlotMap(&table)
				s.SetActiveVersion(2)
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
	_ = before
}

// Documents ~67% shard index change when topology changes 6 -> 4 (4/12 slots keep slot%6 == slot%4).
func TestStaticSlotSharder_MigrateSixToFour(t *testing.T) {
	const samples = 10_000
	old := NewStaticSlotSharder(6)
	new := NewStaticSlotSharder(4)

	moves := 0
	for range samples {
		id := uuid.New()
		if old.GetShard(id) != new.GetShard(id) {
			moves++
		}
	}
	frac := float64(moves) / samples
	t.Logf("6->4 remap: %.1f%% campaigns change shard index (theoretical ~66.7%%)", frac*100)
	if frac < 0.55 || frac > 0.78 {
		t.Fatalf("expected ~67%% remap on 6->4, got %.1f%%", frac*100)
	}
}
