package filter

import (
	"sync/atomic"
	"time"

	"espx/internal/ads/sharding"

	"github.com/google/uuid"
)

const (
	localBlockCacheSize = 4096
	localBlockDuration  = 100 * time.Millisecond
)

type localBlockSlot struct {
	campaignID uuid.UUID
	blockedAt  int64 // monotonic nanoseconds
}

// LocalQuotaCache provides a lock-free, zero-allocation cache to block hot campaigns locally (Phase 1.6).
type LocalQuotaCache struct {
	slots [localBlockCacheSize]localBlockSlot
}

// NewLocalQuotaCache creates a lock-free LocalQuotaCache.
func NewLocalQuotaCache() *LocalQuotaCache {
	return &LocalQuotaCache{}
}

// IsBlocked checks if a campaign is currently blocked locally.
func (c *LocalQuotaCache) IsBlocked(id uuid.UUID, nowNano int64) bool {
	h := sharding.Crc32Castagnoli(&id)
	slotIdx := h % localBlockCacheSize

	slot := &c.slots[slotIdx]
	blockedAt := atomic.LoadInt64(&slot.blockedAt)
	if blockedAt == 0 {
		return false
	}

	if slot.campaignID == id {
		if nowNano-blockedAt < int64(localBlockDuration) {
			return true
		}
	}
	return false
}

// Block registers a campaign as locally blocked for localBlockDuration.
func (c *LocalQuotaCache) Block(id uuid.UUID, nowNano int64) {
	h := sharding.Crc32Castagnoli(&id)
	slotIdx := h % localBlockCacheSize

	slot := &c.slots[slotIdx]
	slot.campaignID = id
	atomic.StoreInt64(&slot.blockedAt, nowNano)
}
