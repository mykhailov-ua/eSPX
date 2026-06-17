package auth

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter abstracts fixed-window rate checks so auth can swap Redis for another backend in tests.
type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

// RedisLimiter counts attempts in Redis for simple per-key rate limiting outside login lockout.
type RedisLimiter struct {
	rdb *redis.Client
}

// NewRedisLimiter provides a swappable backend for simple rate checks in tests and auxiliary paths.
func NewRedisLimiter(rdb *redis.Client) Limiter {
	return &RedisLimiter{rdb: rdb}
}

// Allow throttles generic keys without the login-specific lockout semantics.
func (l *RedisLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	pipe := l.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.ExpireNX(ctx, key, window)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	return incr.Val() <= int64(limit), nil
}

// LockoutLimiter tracks failed login attempts in Redis to throttle credential stuffing without blocking accounts in Postgres.
type LockoutLimiter struct {
	rdb redis.UniversalClient
}

// NewLockoutLimiter keeps brute-force state in Redis so Postgres is not written on every failure.
func NewLockoutLimiter(rdb redis.UniversalClient) *LockoutLimiter {
	return &LockoutLimiter{rdb: rdb}
}

// MaxGlobalAttempts and GlobalLockoutDuration cap email-wide probing before a longer global lock applies.
const (
	MaxGlobalAttempts     = 50
	GlobalLockoutDuration = 3600
)

// lockoutScript atomically reserves a login attempt slot while respecting per-IP and global failure counters.
const lockoutScript = `
local fail_key = KEYS[1]
local inflight_key = KEYS[2]
local global_fail_key = KEYS[3]
local max_attempts = tonumber(ARGV[1])
local lockout_duration = tonumber(ARGV[2])
local attempt_window = tonumber(ARGV[3])
local max_global_attempts = tonumber(ARGV[4])

local batch = redis.call("MGET", global_fail_key, fail_key)
local global_fails = tonumber(batch[1]) or 0
if global_fails >= max_global_attempts then
    return -1
end

local fails = tonumber(batch[2]) or 0
if fails >= max_attempts then
    return 0
end

local inflight = tonumber(redis.call("INCR", inflight_key))
if inflight == 1 then
    redis.call("EXPIRE", inflight_key, 60)
end

if (fails + inflight) > max_attempts then
    redis.call("DECR", inflight_key)
    return 0
end

return 1
`

// decrInflightScript releases an in-flight login slot after verification finishes.
const decrInflightScript = `
local key = KEYS[1]
local val = tonumber(redis.call("GET", key) or "0")
if val > 0 then
    local res = redis.call("DECR", key)
    if res == 0 then
        redis.call("DEL", key)
    end
    return res
else
    redis.call("DEL", key)
    return 0
end
`

// incrementScript records a failed login and escalates lockout TTL when thresholds are crossed.
const incrementScript = `
local key = KEYS[1]
local global_key = KEYS[2]
local max_attempts = tonumber(ARGV[1])
local lockout_duration = tonumber(ARGV[2])
local attempt_window = tonumber(ARGV[3])
local max_global_attempts = tonumber(ARGV[4])
local global_lockout_duration = tonumber(ARGV[5])

local attempts = redis.call("INCR", key)
if attempts == 1 then
    redis.call("EXPIRE", key, attempt_window)
elseif attempts >= max_attempts then
    redis.call("EXPIRE", key, lockout_duration)
end

local global_attempts = redis.call("INCR", global_key)
if global_attempts == 1 then
    redis.call("EXPIRE", global_key, 3600)
elseif global_attempts >= max_global_attempts then
    redis.call("EXPIRE", global_key, global_lockout_duration)
end

if global_attempts >= max_global_attempts then
    return -1
end
return attempts
`

// AllowIP sheds credential-stuffing volume before expensive Argon2 verification runs.
func (l *LockoutLimiter) AllowIP(ctx context.Context, clientIP string, limit int, window time.Duration) (bool, error) {
	key := "ratelimit:ip:" + clientIP
	pipe := l.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.ExpireNX(ctx, key, window)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}
	return incr.Val() <= int64(limit), nil
}

// Allow reserves an attempt slot atomically so parallel guesses cannot bypass per-IP limits.
func (l *LockoutLimiter) Allow(ctx context.Context, clientIP, email string, maxAttempts int, lockoutDuration, attemptWindow time.Duration) (int64, error) {
	failKey := "lockout:ip_email:" + clientIP + ":{" + email + "}"
	inflightKey := "lockout:inflight:" + clientIP + ":{" + email + "}"
	globalFailKey := "lockout:global_email:{" + email + "}"
	res, err := l.rdb.Eval(ctx, lockoutScript, []string{failKey, inflightKey, globalFailKey}, maxAttempts, int(lockoutDuration.Seconds()), int(attemptWindow.Seconds()), MaxGlobalAttempts).Result()
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

// DecrementInflight releases the slot reserved during verification so inflight counters do not leak.
func (l *LockoutLimiter) DecrementInflight(ctx context.Context, clientIP, email string) error {
	key := "lockout:inflight:" + clientIP + ":{" + email + "}"
	_, err := l.rdb.Eval(ctx, decrInflightScript, []string{key}).Result()
	return err
}

// Increment escalates lockout TTL when thresholds are crossed to slow distributed guessing.
func (l *LockoutLimiter) Increment(ctx context.Context, clientIP, email string, maxAttempts int, lockoutDuration, attemptWindow time.Duration) (int64, error) {
	key := "lockout:ip_email:" + clientIP + ":{" + email + "}"
	globalKey := "lockout:global_email:{" + email + "}"
	res, err := l.rdb.Eval(ctx, incrementScript, []string{key, globalKey}, maxAttempts, int(lockoutDuration.Seconds()), int(attemptWindow.Seconds()), MaxGlobalAttempts, GlobalLockoutDuration).Result()
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

// Reset clears per-IP counters after success so legitimate users are not penalized for past failures.
func (l *LockoutLimiter) Reset(ctx context.Context, clientIP, email string) error {
	key := "lockout:ip_email:" + clientIP + ":{" + email + "}"
	return l.rdb.Del(ctx, key).Err()
}
