package ads

import ()

import (
	"hash/crc32"

	"github.com/google/uuid"
)

type Sharder interface {
	GetShard(id uuid.UUID) int
}

type JumpHashSharder struct {
	numBuckets int
}

func NewJumpHashSharder(numBuckets int) *JumpHashSharder {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	return &JumpHashSharder{numBuckets: numBuckets}
}

func (s *JumpHashSharder) GetShard(id uuid.UUID) int {
	if s.numBuckets <= 1 {
		return 0
	}

	key := uint64(crc32.ChecksumIEEE(id[:]))

	return int(jumpHash(key, int32(s.numBuckets)))
}

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
