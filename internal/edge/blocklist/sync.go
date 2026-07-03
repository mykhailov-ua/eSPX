package blocklist

import (
	"context"
	"fmt"

	"github.com/cilium/ebpf"
	"github.com/redis/go-redis/v9"
)

const (
	redisKeyBlacklistManual = "blacklist:manual"
	redisKeyBlacklistAuto   = "blacklist:auto"
	redisKeyBlacklistFraud  = "blacklist:fraud"
)

// denySetReader loads Redis SET members for XDP deny sync.
type denySetReader interface {
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
}

// SyncFromRedis mirrors blacklist:manual, blacklist:auto, and blacklist:fraud into the pinned BPF map.
func SyncFromRedis(ctx context.Context, rdb denySetReader, m *ebpf.Map, store *Store) (added, removed int, err error) {
	manual, err := rdb.SMembers(ctx, redisKeyBlacklistManual).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("smembers %s: %w", redisKeyBlacklistManual, err)
	}
	auto, err := rdb.SMembers(ctx, redisKeyBlacklistAuto).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("smembers %s: %w", redisKeyBlacklistAuto, err)
	}
	fraud, err := rdb.SMembers(ctx, redisKeyBlacklistFraud).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("smembers %s: %w", redisKeyBlacklistFraud, err)
	}
	return store.ApplyDiff(m, manual, auto, fraud)
}
