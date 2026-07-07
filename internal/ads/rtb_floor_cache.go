package ads

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const RtbFloorRedisKeyPrefix = "rtb:floor:"

// DealFloorCache holds read-only optimized deal floors mirrored from Redis.
type DealFloorCache struct {
	rdb  redis.UniversalClient
	snap atomic.Pointer[map[string]int64]
}

// NewDealFloorCache creates an empty deal floor cache.
func NewDealFloorCache(rdb redis.UniversalClient) *DealFloorCache {
	c := &DealFloorCache{rdb: rdb}
	empty := make(map[string]int64)
	c.snap.Store(&empty)
	return c
}

// Get returns an optimized floor for deal_id when present.
func (c *DealFloorCache) Get(dealID string) (int64, bool) {
	if dealID == "" {
		return 0, false
	}
	ptr := c.snap.Load()
	if ptr == nil {
		return 0, false
	}
	v, ok := (*ptr)[dealID]
	return v, ok
}

// Refresh loads rtb:floor:{deal_id} keys for all known deals from Redis shard 0.
func (c *DealFloorCache) Refresh(ctx context.Context, dealIDs []string) {
	if c == nil || c.rdb == nil || len(dealIDs) == 0 {
		return
	}
	keys := make([]string, len(dealIDs))
	for i, id := range dealIDs {
		keys[i] = RtbFloorRedisKeyPrefix + id
	}
	vals, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		slog.Warn("deal floor cache refresh failed", "error", err)
		return
	}
	next := make(map[string]int64, len(dealIDs))
	for i, raw := range vals {
		if raw == nil {
			continue
		}
		s, ok := raw.(string)
		if !ok || s == "" {
			continue
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil || v <= 0 {
			continue
		}
		next[dealIDs[i]] = v
	}
	c.snap.Store(&next)
}

// StartDealFloorRefresh polls Redis for optimizer floors on a fixed interval.
func StartDealFloorRefresh(ctx context.Context, cache *DealFloorCache, catalog *RtbCatalog, interval time.Duration) {
	if cache == nil || catalog == nil || interval <= 0 {
		return
	}
	refresh := func() {
		deals := catalog.AllDeals()
		if len(deals) == 0 {
			return
		}
		ids := make([]string, len(deals))
		for i, d := range deals {
			ids[i] = d.DealID
		}
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		cache.Refresh(rctx, ids)
		cancel()
	}
	refresh()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

// EffectiveDealFloor returns the highest applicable floor from publisher, Postgres deal, and optimizer cache.
func EffectiveDealFloor(catalog *RtbCatalog, floors *DealFloorCache, dealID string, publisherFloor int64) int64 {
	floor := publisherFloor
	if catalog != nil && dealID != "" {
		if deal, ok := catalog.LookupDeal(dealID); ok && deal.FloorMicro > floor {
			floor = deal.FloorMicro
		}
	}
	if floors != nil && dealID != "" {
		if optimized, ok := floors.Get(dealID); ok && optimized > floor {
			floor = optimized
		}
	}
	return floor
}
