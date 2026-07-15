package ingestion

import (
	"context"
	"errors"
	"testing"

	redis "github.com/redis/go-redis/v9"
)

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

type failScriptLoadRedis struct {
	mockRedisClient
}

func (failScriptLoadRedis) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(errors.New("script load failed"))
	return cmd
}

func TestPreloadScripts_failsOnScriptLoadError(t *testing.T) {
	f := NewUnifiedFilter(
		[]redis.UniversalClient{&failScriptLoadRedis{}},
		NewJumpHashSharder(1),
		&mockRegistry{},
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
