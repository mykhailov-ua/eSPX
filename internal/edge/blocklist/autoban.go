package blocklist

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RecordAutoBan adds an IP to blacklist:auto with a lease tracked in a ZSET.
func RecordAutoBan(ctx context.Context, rdb redis.Cmdable, ip string, ttl time.Duration) error {
	if rdb == nil || ip == "" {
		return fmt.Errorf("nil redis client or empty ip")
	}
	expiresAt := float64(time.Now().Add(ttl).Unix())
	pipe := rdb.Pipeline()
	pipe.SAdd(ctx, redisKeyBlacklistAuto, ip)
	pipe.ZAdd(ctx, redisKeyBlacklistAutoTTL, redis.Z{Score: expiresAt, Member: ip})
	_, err := pipe.Exec(ctx)
	return err
}

func loadAutoBans(ctx context.Context, rdb denySetReader) ([]string, error) {
	if ab, ok := rdb.(autoBanReader); ok {
		return activeAutoBans(ctx, ab)
	}
	return rdb.SMembers(ctx, redisKeyBlacklistAuto).Result()
}

func activeAutoBans(ctx context.Context, rdb autoBanReader) ([]string, error) {
	members, err := rdb.SMembers(ctx, redisKeyBlacklistAuto).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return members, nil
	}
	now := float64(time.Now().Unix())
	active := make([]string, 0, len(members))
	for _, ip := range members {
		score, err := rdb.ZScore(ctx, redisKeyBlacklistAutoTTL, ip).Result()
		if err == redis.Nil {
			active = append(active, ip)
			continue
		}
		if err != nil {
			return nil, err
		}
		if score > now {
			active = append(active, ip)
			continue
		}
		_ = rdb.SRem(ctx, redisKeyBlacklistAuto, ip).Err()
		_ = rdb.ZRem(ctx, redisKeyBlacklistAutoTTL, ip).Err()
	}
	return active, nil
}
