package ingestion

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"espx/internal/campaignmodel"
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

// PreloadScripts warms EVALSHA for filter_full and budget_fast on every shard.
func (f *UnifiedFilter) PreloadScripts(ctx context.Context) error {
	if f == nil || f.script == nil || f.fastScript == nil {
		return fmt.Errorf("unified filter scripts are nil")
	}
	for i, rdb := range f.rdbs {
		shard := strconv.Itoa(i)
		if err := f.script.Load(ctx, rdb).Err(); err != nil {
			metrics.RedisLuaScriptLoaded.WithLabelValues(shard).Set(0)
			metrics.RedisLuaFastScriptLoaded.WithLabelValues(shard).Set(0)
			return fmt.Errorf("preload filter full script shard %d: %w", i, err)
		}
		if err := f.fastScript.Load(ctx, rdb).Err(); err != nil {
			metrics.RedisLuaScriptLoaded.WithLabelValues(shard).Set(0)
			metrics.RedisLuaFastScriptLoaded.WithLabelValues(shard).Set(0)
			return fmt.Errorf("preload budget fast script shard %d: %w", i, err)
		}
		metrics.RedisLuaScriptLoaded.WithLabelValues(shard).Set(1)
		metrics.RedisLuaFastScriptLoaded.WithLabelValues(shard).Set(1)
	}
	return f.openFilterEvalPins(ctx)
}

// evalScript prefers pooled EVALSHA and falls back once so cold Redis shards still load the unified filter script.
func (f *UnifiedFilter) evalScript(ctx context.Context, rdb redis.UniversalClient, shard int, evt *campaignmodel.Event, keyArgs [unifiedFilterKeyCount]any, args []any) (int64, error) {
	res, err := f.evalShaPooled(ctx, rdb, shard, evt, f.scriptHashAny, keyArgs, args)
	if err != nil && isNoScriptErr(err) {
		incRedisLuaNoScript(f.luaNoScriptCounters, shard)
		return f.evalPooled(ctx, rdb, shard, evt, unifiedFilterLuaAny, keyArgs, args)
	}
	return res, err
}
