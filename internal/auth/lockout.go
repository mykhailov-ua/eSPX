package auth

import (
// "github.com/mykhailov-ua/ad-event-processor/internal/auth/db"
)

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

type RedisLimiter struct {
	rdb *redis.Client
}

func NewRedisLimiter(rdb *redis.Client) Limiter {
	return &RedisLimiter{rdb: rdb}
}

const rateLimitScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end

if current > limit then
    return 0
end
return 1
`

func (l *RedisLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	res, err := l.rdb.Eval(ctx, rateLimitScript, []string{key}, limit, int(window.Seconds())).Result()
	if err != nil {
		return false, err
	}

	return res.(int64) == 1, nil
}

type LockoutLimiter struct {
	rdb redis.UniversalClient
}

func NewLockoutLimiter(rdb redis.UniversalClient) *LockoutLimiter {
	return &LockoutLimiter{rdb: rdb}
}

const lockoutScript = `
local key = KEYS[1]
local max_attempts = tonumber(ARGV[1])
local lockout_duration = tonumber(ARGV[2])
local attempt_window = tonumber(ARGV[3])

local attempts = redis.call("GET", key)
if not attempts then
    return 1 -- allowed
end

attempts = tonumber(attempts)
if attempts >= max_attempts then
    return 0 -- locked out
end

return 1 -- allowed
`

const incrementScript = `
local key = KEYS[1]
local max_attempts = tonumber(ARGV[1])
local lockout_duration = tonumber(ARGV[2])
local attempt_window = tonumber(ARGV[3])

local attempts = redis.call("INCR", key)
if attempts == 1 then
    redis.call("EXPIRE", key, attempt_window)
elseif attempts >= max_attempts then
    redis.call("EXPIRE", key, lockout_duration)
end
return attempts
`

func (l *LockoutLimiter) Allow(ctx context.Context, email string, maxAttempts int, lockoutDuration, attemptWindow time.Duration) (bool, error) {
	key := "lockout:email:" + email
	res, err := l.rdb.Eval(ctx, lockoutScript, []string{key}, maxAttempts, int(lockoutDuration.Seconds()), int(attemptWindow.Seconds())).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

func (l *LockoutLimiter) Increment(ctx context.Context, email string, maxAttempts int, lockoutDuration, attemptWindow time.Duration) error {
	key := "lockout:email:" + email
	_, err := l.rdb.Eval(ctx, incrementScript, []string{key}, maxAttempts, int(lockoutDuration.Seconds()), int(attemptWindow.Seconds())).Result()
	return err
}

func (l *LockoutLimiter) Reset(ctx context.Context, email string) error {
	key := "lockout:email:" + email
	return l.rdb.Del(ctx, key).Err()
}
