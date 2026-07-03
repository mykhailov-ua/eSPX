package management

import (
	"context"
	"fmt"

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
