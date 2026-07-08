package ads

import (
	"context"
	_ "embed"
	"time"

	"espx/internal/domain"
	redis "github.com/redis/go-redis/v9"
)

//go:embed ip-rate-limit.lua
var ipRateLimitLua string

var (
	ipRateLimitLuaAny = ipRateLimitLua
	ipRateLimitScript = redis.NewScript(ipRateLimitLua)
)

// IPRateLimiter caps per-IP event rates to mitigate abuse on the track endpoint.
type IPRateLimiter struct {
	rdb           redis.UniversalClient
	limit         int
	scriptHashAny any
	scriptAny     any
	windowMsAny   any
	wire          [5]any
}

func NewIPRateLimiter(rdb redis.UniversalClient, limit int, windowMs time.Duration) *IPRateLimiter {
	ms := windowMs.Milliseconds()
	l := &IPRateLimiter{
		rdb:           rdb,
		limit:         limit,
		scriptHashAny: ipRateLimitScript.Hash(),
		scriptAny:     ipRateLimitLuaAny,
		windowMsAny:   ms,
	}
	l.wire[0] = evalShaCmdAny
	l.wire[2] = numKeys1Any
	return l
}

// Check increments the IP counter and rejects when the window limit is exceeded.
func (l *IPRateLimiter) Check(ctx context.Context, evt *domain.Event) error {
	if evt.IP == "" {
		return nil
	}

	w := bufPool.Get().(*bufWrapper)
	w.buf = w.buf[:0]
	w.buf = append(w.buf, "ratelimit:ip:"...)
	w.buf = append(w.buf, evt.IP...)
	key := unsafeString(w.buf)

	count, err := l.evalRateLimit(ctx, key)
	bufPool.Put(w)
	if err != nil {
		return err
	}
	if count > int64(l.limit) {
		return ErrRateLimitExceeded
	}
	return nil
}

func (l *IPRateLimiter) evalRateLimit(ctx context.Context, key string) (int64, error) {
	l.wire[1] = l.scriptHashAny
	l.wire[3] = key
	l.wire[4] = l.windowMsAny

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, l.wire[:], 3)
	err := l.rdb.Process(ctx, cmd)
	val, intErr := cmd.Int64()
	if intErr != nil && err == nil {
		err = intErr
	}
	evalCmdPool.Put(cmd)
	if err != nil && isNoScriptErr(err) {
		return l.evalRateLimitScript(ctx, key)
	}
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (l *IPRateLimiter) evalRateLimitScript(ctx context.Context, key string) (int64, error) {
	l.wire[0] = evalCmdAny
	l.wire[1] = l.scriptAny
	l.wire[3] = key
	l.wire[4] = l.windowMsAny

	cmd := evalCmdPool.Get().(*redis.Cmd)
	resetPooledRedisCmd(cmd, ctx, l.wire[:], 3)
	err := l.rdb.Process(ctx, cmd)
	val, intErr := cmd.Int64()
	if intErr != nil && err == nil {
		err = intErr
	}
	evalCmdPool.Put(cmd)
	l.wire[0] = evalShaCmdAny
	l.wire[1] = l.scriptHashAny
	if err != nil {
		return 0, err
	}
	return val, nil
}
