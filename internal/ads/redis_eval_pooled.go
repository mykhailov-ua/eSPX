package ads

import (
	"context"
	"sync"
	"time"
	"unsafe"

	redis "github.com/redis/go-redis/v9"
)

const unifiedFilterKeyCount = 15

var (
	evalShaCmdAny any = "evalsha"
	evalCmdAny    any = "eval"
	numKeys15Any  any = unifiedFilterKeyCount
)

// evalWirePool recycles EVALSHA wire argument slices (3 + keys + argv).
var evalWirePool = sync.Pool{
	New: func() any {
		s := make([]any, 0, 48)
		return &s
	},
}

// evalCmdPool recycles *redis.Cmd shells for unified-filter EVALSHA round trips.
var evalCmdPool = sync.Pool{
	New: func() any {
		return redis.NewCmd(context.Background())
	},
}

// redisCmdHead mirrors the exported redis.Cmd prefix through cmdType for in-place reset.
type redisCmdHead struct {
	ctx         context.Context
	args        []any
	err         error
	keyPos      int8
	stepCount   int8
	rawVal      any
	readTimeout *time.Duration
	cmdType     redis.CmdType
	val         any
}

func resetPooledRedisCmd(cmd *redis.Cmd, ctx context.Context, args []any, firstKeyPos int8) {
	h := (*redisCmdHead)(unsafe.Pointer(cmd))
	h.ctx = ctx
	h.args = args
	h.err = nil
	h.keyPos = firstKeyPos
	h.rawVal = nil
	h.val = nil
}

func fillEvalShaWire(dst []any, sha1 any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) []any {
	need := 3 + unifiedFilterKeyCount + len(scriptArgs)
	if cap(dst) < need {
		dst = make([]any, need, need+4)
	} else {
		dst = dst[:need]
	}
	dst[0] = evalShaCmdAny
	dst[1] = sha1
	dst[2] = numKeys15Any
	off := 3
	for i := range keyArgs {
		dst[off+i] = keyArgs[i]
	}
	off += unifiedFilterKeyCount
	for i := range scriptArgs {
		dst[off+i] = scriptArgs[i]
	}
	return dst
}

func fillEvalWire(dst []any, script any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) []any {
	need := 3 + unifiedFilterKeyCount + len(scriptArgs)
	if cap(dst) < need {
		dst = make([]any, need, need+4)
	} else {
		dst = dst[:need]
	}
	dst[0] = evalCmdAny
	dst[1] = script
	dst[2] = numKeys15Any
	off := 3
	for i := range keyArgs {
		dst[off+i] = keyArgs[i]
	}
	off += unifiedFilterKeyCount
	for i := range scriptArgs {
		dst[off+i] = scriptArgs[i]
	}
	return dst
}

func evalShaPooled(ctx context.Context, c redis.UniversalClient, sha1 any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) (int64, error) {
	wirePtr := evalWirePool.Get().(*[]any)
	wire := fillEvalShaWire(*wirePtr, sha1, keyArgs, scriptArgs)
	*wirePtr = wire

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, wire, 3)
	err := c.Process(ctx, cmd)
	val, intErr := cmd.Int64()
	if intErr != nil && err == nil {
		err = intErr
	}
	evalCmdPool.Put(cmd)
	evalWirePool.Put(wirePtr)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func evalPooled(ctx context.Context, c redis.UniversalClient, script any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) (int64, error) {
	wirePtr := evalWirePool.Get().(*[]any)
	wire := fillEvalWire(*wirePtr, script, keyArgs, scriptArgs)
	*wirePtr = wire

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, wire, 3)
	err := c.Process(ctx, cmd)
	val, intErr := cmd.Int64()
	if intErr != nil && err == nil {
		err = intErr
	}
	evalCmdPool.Put(cmd)
	evalWirePool.Put(wirePtr)
	if err != nil {
		return 0, err
	}
	return val, nil
}
