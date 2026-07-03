package allowlist

import (
	"context"
	"fmt"

	"github.com/cilium/ebpf"
	"github.com/redis/go-redis/v9"
)

const redisKeyAllowlistPartners = "allowlist:partners"

// setReader loads Redis SET members for XDP allow sync.
type setReader interface {
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
}

// SyncFromRedis mirrors allowlist:partners into the pinned BPF map.
func SyncFromRedis(ctx context.Context, rdb setReader, m *ebpf.Map, store *Store) (added, removed int, err error) {
	members, err := rdb.SMembers(ctx, redisKeyAllowlistPartners).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("smembers %s: %w", redisKeyAllowlistPartners, err)
	}
	return store.ApplyDiff(m, members)
}
