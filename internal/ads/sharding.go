package ads

import ()

import (
	"hash/crc32"

	"github.com/google/uuid"
)

// Sharder defines the interface for key distribution across a variable number of execution shards.
// Chosen to decouple specific hashing algorithms from the high-level routing logic.
type Sharder interface {
	GetShard(id uuid.UUID) int
}

// JumpHashSharder implements a fast, consistent hashing algorithm.
// Chosen for its O(ln N) efficiency and minimal key redistribution during cluster resizing.
type JumpHashSharder struct {
	numBuckets int
}

// NewJumpHashSharder creates a sharder instance, defaulting to a single bucket if invalid input is provided.
func NewJumpHashSharder(numBuckets int) *JumpHashSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	return &JumpHashSharder{numBuckets: numBuckets}
}

// GetShard utilizes CRC32 to map UUIDs into a 64-bit space before applying consistent hashing.
// Provides uniform distribution and high performance for frequent sharding lookups.
func (s *JumpHashSharder) GetShard(id uuid.UUID) int {
	if s.numBuckets <= 1 {
		return 0
	}

	key := uint64(crc32.ChecksumIEEE(id[:]))

	return int(jumpHash(key, int32(s.numBuckets)))
}

// jumpHash implements the Jump Consistent Hash algorithm to provide deterministic bucket mapping with minimal rebalancing.
// Optimized to minimize movement when buckets are added to the set.
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
