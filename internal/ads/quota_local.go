package ads

import (
	"sync/atomic"
	"time"

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

// LocalQuotaCache blocks hot campaigns in-process after distributed quota exhaustion.
type LocalQuotaCache struct {
	slots [localBlockCacheSize]localBlockSlot
}

func NewLocalQuotaCache() *LocalQuotaCache {
	return &LocalQuotaCache{}
}

func (c *LocalQuotaCache) IsBlocked(id uuid.UUID, nowNano int64) bool {
	h := crc32Castagnoli(&id)
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
	h := crc32Castagnoli(&id)
	slotIdx := h % localBlockCacheSize

	slot := &c.slots[slotIdx]
	slot.campaignID = id
	atomic.StoreInt64(&slot.blockedAt, nowNano)
}
