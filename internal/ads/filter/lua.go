package filter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"espx/internal/metrics"

	redis "github.com/redis/go-redis/v9"
)

// isNoScriptErr detects missing Lua script SHA so callers can fall back to EVAL once.
func isNoScriptErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.ErrNoScript) {
		return true
	}
	return strings.Contains(err.Error(), "NOSCRIPT")
}

// PreloadScripts warms EVALSHA on every shard to avoid NOSCRIPT latency on first requests.
func (f *UnifiedFilter) PreloadScripts(ctx context.Context) error {
	if f == nil || f.script == nil {
		return fmt.Errorf("unified filter script is nil")
	}
	for i, rdb := range f.rdbs {
		shard := strconv.Itoa(i)
		if err := f.script.Load(ctx, rdb).Err(); err != nil {
			metrics.RedisLuaScriptLoaded.WithLabelValues(shard).Set(0)
			return fmt.Errorf("preload unified filter script shard %d: %w", i, err)
		}
		metrics.RedisLuaScriptLoaded.WithLabelValues(shard).Set(1)
	}
	return nil
}

// evalScript prefers pooled EVALSHA and falls back once so cold Redis shards still load the unified filter script.
func (f *UnifiedFilter) evalScript(ctx context.Context, rdb redis.UniversalClient, shard int, keyArgs [unifiedFilterKeyCount]any, args []any) (int64, error) {
	res, err := evalShaPooled(ctx, rdb, f.scriptHashAny, keyArgs, args)
	if err != nil && isNoScriptErr(err) {
		incRedisLuaNoScript(f.luaNoScriptCounters, shard)
		return evalPooled(ctx, rdb, unifiedFilterLuaAny, keyArgs, args)
	}
	return res, err
}
