package sharding

import (
	"sync/atomic"

	"github.com/google/uuid"
)

// slotTable is an immutable 1024-entry shard map swapped via atomic.Value on reload.
type slotTable [1024]uint16

// Sharder maps campaign IDs to Redis shard indices for budget and filter keys.
type Sharder interface {
	GetShard(id uuid.UUID) int
}

// JumpHashSharder spreads keys with minimal remapping when shard count changes at scale.
type JumpHashSharder struct {
	numBuckets int
}

// StaticSlotSharder picks the lowest-latency shard for a fixed cluster size on the tracker hot path.
// Slot lookup uses atomic.Value — no mutex on GetShard; reload swaps the whole table on cold path.
type StaticSlotSharder struct {
	slots         atomic.Value // *slotTable
	activeVersion atomic.Int32 // Postgres active_version; 0 = modulo fallback only
}

// buildSlotTable precomputes slot % numBuckets routing for StaticSlotSharder.
func buildSlotTable(numBuckets int) *slotTable {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	var t slotTable
	for i := range t {
		t[i] = uint16(i % numBuckets)
	}
	return &t
}

// NewStaticSlotSharder precomputes shard slots for O(1) lookup at high RPS.
func NewStaticSlotSharder(numBuckets int) *StaticSlotSharder {
	s := &StaticSlotSharder{}
	s.slots.Store(buildSlotTable(numBuckets))
	return s
}

// GetShard returns the precomputed shard index for a campaign.
func (s *StaticSlotSharder) GetShard(id uuid.UUID) int {
	key := crc32Castagnoli(&id)
	slot := key & 1023
	table := s.slots.Load().(*slotTable)
	return int(table[slot])
}

// ReloadFromModulo atomically replaces the slot map for slot % N topology (cold path only).
func (s *StaticSlotSharder) ReloadFromModulo(numBuckets int) {
	s.slots.Store(buildSlotTable(numBuckets))
}

// StoreSlotMap atomically swaps a caller-built 1024-entry map (Phase 2 Fixed Slot Map).
func (s *StaticSlotSharder) StoreSlotMap(table *[1024]uint16) {
	if table == nil {
		return
	}
	var copy slotTable
	copy = slotTable(*table)
	s.slots.Store(&copy)
}

// SetActiveVersion records the Postgres map version loaded into this sharder (cold path).
func (s *StaticSlotSharder) SetActiveVersion(version int32) {
	s.activeVersion.Store(version)
}

// ActiveVersion returns the loaded Postgres map version; 0 if only modulo fallback was used.
func (s *StaticSlotSharder) ActiveVersion() int32 {
	return s.activeVersion.Load()
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

// jumpHash spreads campaigns across shards with minimal remapping when bucket count changes.
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

// JumpHash maps a 64-bit key to a bucket index for consistent shard selection.
func JumpHash(key uint64, numBuckets int32) int32 {
	return jumpHash(key, numBuckets)
}
