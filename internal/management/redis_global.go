package management

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisConfigValuesKey  = "config:values"
	redisConfigVersionKey = "config:version"
)

// syncGlobalConfigToAllShards mirrors dynamic config keys to every Redis shard so trackers survive shard-0 loss.
func syncGlobalConfigToAllShards(ctx context.Context, rdbs []redis.UniversalClient, settings map[string]string, version int64) error {
	if len(rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	for i, rdb := range rdbs {
		if rdb == nil {
			return fmt.Errorf("redis shard %d is nil", i)
		}
		_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			if len(settings) > 0 {
				pipe.HSet(ctx, redisConfigValuesKey, settings)
			}
			if version > 0 {
				pipe.Set(ctx, redisConfigVersionKey, version, 0)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("sync global config on shard %d: %w", i, err)
		}
	}
	return nil
}

// replicateConfigVersionFromPrimary copies config:version from the first shard to the rest after a cold settings sync.
func replicateConfigVersionFromPrimary(ctx context.Context, rdbs []redis.UniversalClient) error {
	if len(rdbs) < 2 || rdbs[0] == nil {
		return nil
	}
	version, err := rdbs[0].Get(ctx, redisConfigVersionKey).Int64()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read config version from primary shard: %w", err)
	}
	for i := 1; i < len(rdbs); i++ {
		if rdbs[i] == nil {
			return fmt.Errorf("redis shard %d is nil", i)
		}
		if err := rdbs[i].Set(ctx, redisConfigVersionKey, version, 0).Err(); err != nil {
			return fmt.Errorf("replicate config version on shard %d: %w", i, err)
		}
	}
	return nil
}

// syncGlobalStringToAllShards SETs a global (non-hash-tagged) key on every shard (M14-01).
func syncGlobalStringToAllShards(ctx context.Context, rdbs []redis.UniversalClient, key, value string, ttl time.Duration) error {
	if len(rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	for i, rdb := range rdbs {
		if rdb == nil {
			return fmt.Errorf("redis shard %d is nil", i)
		}
		if err := rdb.Set(ctx, key, value, ttl).Err(); err != nil {
			return fmt.Errorf("set %s on shard %d: %w", key, i, err)
		}
	}
	return nil
}

// deleteGlobalKeyFromAllShards DELs a global key on every shard (M14-01).
func deleteGlobalKeyFromAllShards(ctx context.Context, rdbs []redis.UniversalClient, key string) error {
	if len(rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	for i, rdb := range rdbs {
		if rdb == nil {
			continue
		}
		if err := rdb.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("del %s on shard %d: %w", key, i, err)
		}
	}
	return nil
}

// syncGlobalSetMemberToAllShards SAdds or SRems a member on every shard (blacklist:* fan-out, M14-01).
func syncGlobalSetMemberToAllShards(ctx context.Context, rdbs []redis.UniversalClient, key, member string, add bool) error {
	if len(rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	for i, rdb := range rdbs {
		if rdb == nil {
			return fmt.Errorf("redis shard %d is nil", i)
		}
		var err error
		if add {
			err = rdb.SAdd(ctx, key, member).Err()
		} else {
			err = rdb.SRem(ctx, key, member).Err()
		}
		if err != nil {
			return fmt.Errorf("set member sync on shard %d key %s: %w", i, key, err)
		}
	}
	return nil
}

// syncGlobalHashFieldToAllShards HSETs or HDELs a hash field on every shard (placement blacklist, M14-01).
func syncGlobalHashFieldToAllShards(ctx context.Context, rdbs []redis.UniversalClient, key, field, value string, del bool) error {
	if len(rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}
	for i, rdb := range rdbs {
		if rdb == nil {
			continue
		}
		var err error
		if del {
			err = rdb.HDel(ctx, key, field).Err()
		} else {
			err = rdb.HSet(ctx, key, field, value).Err()
		}
		if err != nil {
			return fmt.Errorf("hash field sync on shard %d key %s: %w", i, key, err)
		}
	}
	return nil
}
