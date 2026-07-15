package ingestion

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	localBlockCacheSize = 4096
	localBlockDuration  = 100 * time.Millisecond
)

// LocalQuotaCache blocks hot campaigns in-process after distributed quota exhaustion.
// Each slot is one atomic.Uint64: high 32 bits = campaign hash, low 32 = blocked mono ms.
type LocalQuotaCache struct {
	slots [localBlockCacheSize]atomic.Uint64
}

func NewLocalQuotaCache() *LocalQuotaCache {
	return &LocalQuotaCache{}
}

func packLocalBlock(campaignHash uint32, blockedMs uint32) uint64 {
	return uint64(campaignHash)<<32 | uint64(blockedMs)
}

func (c *LocalQuotaCache) IsBlocked(id uuid.UUID, nowNano int64) bool {
	h := crc32Castagnoli(&id)
	slotIdx := h % localBlockCacheSize

	packed := c.slots[slotIdx].Load()
	if packed == 0 {
		return false
	}
	if uint32(packed>>32) != h {
		return false
	}
	blockedMs := uint32(packed)
	nowMs := uint32(nowNano / int64(time.Millisecond))
	return nowMs-blockedMs < uint32(localBlockDuration/time.Millisecond)
}

// Block registers a campaign as locally blocked for localBlockDuration.
func (c *LocalQuotaCache) Block(id uuid.UUID, nowNano int64) {
	h := crc32Castagnoli(&id)
	slotIdx := h % localBlockCacheSize
	blockedMs := uint32(nowNano / int64(time.Millisecond))
	c.slots[slotIdx].Store(packLocalBlock(h, blockedMs))
}
