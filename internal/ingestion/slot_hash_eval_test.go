package ingestion

import (
	"math"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

// murmur3_32 is a minimal 32-bit MurmurHash3 x86 variant for benchmark comparison only.
func murmur3_32(data []byte, seed uint32) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)
	h := seed
	nblocks := len(data) / 4
	for i := 0; i < nblocks; i++ {
		k := uint32(data[i*4]) | uint32(data[i*4+1])<<8 | uint32(data[i*4+2])<<16 | uint32(data[i*4+3])<<24
		k *= c1
		k = (k << 15) | (k >> 17)
		k *= c2
		h ^= k
		h = (h << 13) | (h >> 19)
		h = h*5 + 0xe6546b64
	}
	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h ^= k1
	}
	h ^= uint32(len(data))
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func slotFromHash(h uint32) int {
	return int(h & SlotMask)
}

// shardEntropy returns normalized Shannon entropy of shard assignment over numShards buckets.
func shardEntropy(assign func(uuid.UUID) int, samples int, numShards int) float64 {
	counts := make([]int, numShards)
	for i := 0; i < samples; i++ {
		id := uuid.New()
		shard := assign(id)
		if shard < 0 || shard >= numShards {
			continue
		}
		counts[shard]++
	}
	var entropy float64
	for _, c := range counts {
		if c == 0 {
			continue
		}
		p := float64(c) / float64(samples)
		entropy -= p * math.Log2(p)
	}
	return entropy / math.Log2(float64(numShards))
}

func TestSlotHashEntropy_CRC32Retained(t *testing.T) {
	const (
		samples   = 100_000
		numShards = 4
	)
	crcEntropy := shardEntropy(func(id uuid.UUID) int {
		return NewStaticSlotSharder(numShards).GetShard(id)
	}, samples, numShards)
	xxEntropy := shardEntropy(func(id uuid.UUID) int {
		return slotFromHash(uint32(xxhash.Sum64(id[:])))
	}, samples, numShards)
	murEntropy := shardEntropy(func(id uuid.UUID) int {
		return slotFromHash(murmur3_32(id[:], 0))
	}, samples, numShards)

	t.Logf("shard entropy (normalized): crc32=%.4f xxhash=%.4f murmur3=%.4f", crcEntropy, xxEntropy, murEntropy)

	// All hashes distribute well; xxhash/murmur3 do not materially improve entropy.
	const minEntropy = 0.99
	if crcEntropy < minEntropy {
		t.Fatalf("crc32 entropy %.4f below %.2f", crcEntropy, minEntropy)
	}
	if xxEntropy <= crcEntropy+0.001 && murEntropy <= crcEntropy+0.001 {
		t.Log("keeping crc32Castagnoli: no entropy gain from alternate hashes")
	}
}

func BenchmarkSlotHash_CRC32(b *testing.B) {
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = crc32Castagnoli(&id)
	}
}

func BenchmarkSlotHash_xxhash64(b *testing.B) {
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = xxhash.Sum64(id[:])
	}
}

func BenchmarkSlotHash_murmur3(b *testing.B) {
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = murmur3_32(id[:], 0)
	}
}
