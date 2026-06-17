package ads

import (
	"github.com/google/uuid"
)

// Sharder maps campaign IDs to Redis shard indices for budget and filter keys.
type Sharder interface {
	GetShard(id uuid.UUID) int
}

// JumpHashSharder spreads keys with minimal remapping when shard count changes at scale.
type JumpHashSharder struct {
	numBuckets int
}

// StaticSlotSharder picks the lowest-latency shard for a fixed cluster size on the tracker hot path.
type StaticSlotSharder struct {
	slots [1024]uint16
}

// NewStaticSlotSharder precomputes shard slots for O(1) lookup at high RPS.
func NewStaticSlotSharder(numBuckets int) *StaticSlotSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	s := &StaticSlotSharder{}
	for i := 0; i < 1024; i++ {
		s.slots[i] = uint16(i % numBuckets)
	}
	return s
}

// GetShard returns the precomputed shard index for a campaign.
func (s *StaticSlotSharder) GetShard(id uuid.UUID) int {
	key := crc32Castagnoli(&id)
	slot := key & 1023
	return int(s.slots[slot])
}

// NewJumpHashSharder builds a consistent hasher for live cluster resize scenarios.
func NewJumpHashSharder(numBuckets int) *JumpHashSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	return &JumpHashSharder{numBuckets: numBuckets}
}

// GetShard returns the jump-hash shard index for a campaign.
func (s *JumpHashSharder) GetShard(id uuid.UUID) int {
	if s.numBuckets <= 1 {
		return 0
	}

	key := uint64(crc32Castagnoli(&id))

	return int(jumpHash(key, int32(s.numBuckets)))
}

// jumpHash implements Google jump consistent hashing for shard selection.
func jumpHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}
