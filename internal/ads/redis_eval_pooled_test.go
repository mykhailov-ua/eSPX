package ads

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	redis "github.com/redis/go-redis/v9"
)

// Guards redis.Cmd in-memory layout matches resetPooledRedisCmd unsafe mirror.
func TestRedisCmdLayoutMatchesResetMirror(t *testing.T) {
	t.Helper()
	cmd := redis.NewCmd(context.Background(), "ping")
	cmdTyp := reflect.TypeOf(*cmd)
	if cmdTyp.NumField() != 2 {
		t.Fatalf("redis.Cmd field count = %d, want 2", cmdTyp.NumField())
	}
	baseField, ok := cmdTyp.FieldByName("baseCmd")
	if !ok {
		t.Fatal("redis.Cmd missing baseCmd field")
	}
	valField, ok := cmdTyp.FieldByName("val")
	if !ok {
		t.Fatal("redis.Cmd missing val field")
	}
	headSize := valField.Offset + valField.Type.Size()
	mirrorSize := unsafe.Sizeof(redisCmdHead{})
	if headSize != mirrorSize {
		t.Fatalf("redisCmdHead size %d != Cmd size %d", mirrorSize, headSize)
	}
	if baseField.Offset != 0 {
		t.Fatalf("baseCmd offset = %d, want 0", baseField.Offset)
	}
}

func TestEvalShaPooled_MockRedis(t *testing.T) {
	rdb := &mockRedisClient{}
	var keyArgs [unifiedFilterKeyCount]any
	for i := range keyArgs {
		keyArgs[i] = &StringVal{s: "k"}
	}
	args := make([]any, 27)
	ctx := context.Background()

	n, err := evalShaPooled(ctx, rdb, "abc123", keyArgs, args)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("result = %d, want 0", n)
	}
}

func TestResetPooledRedisCmd_ReusesCmd(t *testing.T) {
	rdb := &mockRedisClient{}
	wire := []any{"evalsha", "hash", numKeys15Any, "k1"}
	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, context.Background(), wire, 3)
	if err := rdb.Process(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	n, err := cmd.Int64()
	if err != nil || n != 0 {
		t.Fatalf("first round: n=%d err=%v", n, err)
	}

	resetPooledRedisCmd(cmd, context.Background(), wire, 3)
	if err := rdb.Process(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	n, err = cmd.Int64()
	if err != nil || n != 0 {
		t.Fatalf("second round: n=%d err=%v", n, err)
	}
	evalCmdPool.Put(cmd)
}
