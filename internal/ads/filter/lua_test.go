package filter

import (
	"context"
	"errors"
	"espx/internal/ads/sharding"
	"testing"

	adstest "espx/internal/ads/testutil"
	redis "github.com/redis/go-redis/v9"
)

// Guards NOSCRIPT Redis errors are detected for EVAL fallback path.
func TestIsNoScriptErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"ErrNoScript", redis.ErrNoScript, true},
		{"message", errors.New("NOSCRIPT No matching script. Please use EVAL."), true},
		{"other", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNoScriptErr(tt.err); got != tt.want {
				t.Fatalf("isNoScriptErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// Redis stub failing SCRIPT LOAD for preload error tests.
type failScriptLoadRedis struct {
	adstest.MockRedisClient
}

func (failScriptLoadRedis) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(errors.New("script load failed"))
	return cmd
}

// Guards script preload surfaces SCRIPT LOAD failures at startup.
func TestPreloadScripts_failsOnScriptLoadError(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&failScriptLoadRedis{}},
		sharding.NewJumpHashSharder(1),
		&adstest.MockRegistry{},
		nil,
		100,
		0,
		0,
		0,
		0,
		0,
		"events",
		1000,
	)
	err := f.PreloadScripts(context.Background())
	if err == nil {
		t.Fatal("expected preload error")
	}
}
