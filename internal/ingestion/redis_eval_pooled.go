package ingestion

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"espx/internal/campaignmodel"

	redis "github.com/redis/go-redis/v9"
)

const unifiedFilterKeyCount = 19

var (
	evalShaCmdAny any = "evalsha"
	evalCmdAny    any = "eval"
	numKeys15Any  any = unifiedFilterKeyCount
	numKeys19Any  any = unifiedFilterKeyCount
	numKeys9Any   any = budgetFastKeyCount
	numKeys1Any   any = 1
)

// evalWirePool recycles EVALSHA wire argument slices (3 + keys + argv).
var evalWirePool = sync.Pool{
	New: func() any {
		// Pre-length to unified-filter wire size (3 + 19 keys + 34 argv).
		s := make([]any, 56, 64)
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

func (f *UnifiedFilter) evalShaPooled(ctx context.Context, c redis.UniversalClient, shard int, evt *campaignmodel.Event, sha1 any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) (int64, error) {
	wirePtr := evalWirePool.Get().(*[]any)
	wire := fillEvalShaWire(*wirePtr, sha1, keyArgs, scriptArgs)
	*wirePtr = wire

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, wire, 3)
	err := f.processFilterEval(ctx, c, shard, evt, cmd)
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

func (f *UnifiedFilter) evalPooled(ctx context.Context, c redis.UniversalClient, shard int, evt *campaignmodel.Event, script any, keyArgs [unifiedFilterKeyCount]any, scriptArgs []any) (int64, error) {
	return f.evalPooledN(ctx, c, shard, evt, script, keyArgs[:], scriptArgs, unifiedFilterKeyCount)
}

func (f *UnifiedFilter) evalShaPooledN(ctx context.Context, c redis.UniversalClient, shard int, evt *campaignmodel.Event, sha1 any, keyArgs []any, scriptArgs []any, numKeys int) (int64, error) {
	wirePtr := evalWirePool.Get().(*[]any)
	wire := fillEvalShaWireN(*wirePtr, sha1, keyArgs, scriptArgs, numKeys)
	*wirePtr = wire

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, wire, 3)
	err := f.processFilterEval(ctx, c, shard, evt, cmd)
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

func (f *UnifiedFilter) evalPooledN(ctx context.Context, c redis.UniversalClient, shard int, evt *campaignmodel.Event, script any, keyArgs []any, scriptArgs []any, numKeys int) (int64, error) {
	wirePtr := evalWirePool.Get().(*[]any)
	wire := fillEvalWireN(*wirePtr, script, keyArgs, scriptArgs, numKeys)
	*wirePtr = wire

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, wire, 3)
	err := f.processFilterEval(ctx, c, shard, evt, cmd)
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

func fillEvalShaWireN(dst []any, sha1 any, keyArgs []any, scriptArgs []any, numKeys int) []any {
	need := 3 + numKeys + len(scriptArgs)
	if cap(dst) < need {
		dst = make([]any, need, need+4)
	} else {
		dst = dst[:need]
	}
	dst[0] = evalShaCmdAny
	dst[1] = sha1
	dst[2] = numKeysAny(numKeys)
	off := 3
	for i := range keyArgs {
		dst[off+i] = keyArgs[i]
	}
	off += numKeys
	for i := range scriptArgs {
		dst[off+i] = scriptArgs[i]
	}
	return dst
}

func fillEvalWireN(dst []any, script any, keyArgs []any, scriptArgs []any, numKeys int) []any {
	need := 3 + numKeys + len(scriptArgs)
	if cap(dst) < need {
		dst = make([]any, need, need+4)
	} else {
		dst = dst[:need]
	}
	dst[0] = evalCmdAny
	dst[1] = script
	dst[2] = numKeysAny(numKeys)
	off := 3
	for i := range keyArgs {
		dst[off+i] = keyArgs[i]
	}
	off += numKeys
	for i := range scriptArgs {
		dst[off+i] = scriptArgs[i]
	}
	return dst
}

func numKeysAny(n int) any {
	switch n {
	case 1:
		return numKeys1Any
	case unifiedFilterKeyCount:
		return numKeys19Any
	case budgetFastKeyCount:
		return numKeys9Any
	default:
		return n
	}
}
