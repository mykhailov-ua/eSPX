package ads

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

// evalScript runs the unified filter Lua via EVALSHA with a one-shot EVAL fallback.
func (f *UnifiedFilter) evalScript(ctx context.Context, rdb redis.UniversalClient, shard int, keys []string, args []any) *redis.Cmd {
	shardLabel := strconv.Itoa(shard)
	cmd := f.script.EvalSha(ctx, rdb, keys, args...)
	if err := cmd.Err(); err != nil && isNoScriptErr(err) {
		metrics.RedisLuaNoScriptTotal.WithLabelValues(shardLabel).Inc()
		cmd = f.script.Eval(ctx, rdb, keys, args...)
	}
	return cmd
}
