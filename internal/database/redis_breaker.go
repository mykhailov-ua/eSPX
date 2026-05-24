package database

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	redis "github.com/redis/go-redis/v9"
)

var ErrRedisCircuitOpen = errors.New("redis circuit breaker is open")

type CircuitState int32

const (
	CircuitClosed   CircuitState = 0
	CircuitOpen     CircuitState = 1
	CircuitHalfOpen CircuitState = 2
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// RedisBreaker implements a lock-free, atomic-based circuit breaker to guard Redis clients.
// It avoids Mutex locks on hot execution paths to maximize performance and avoid lock contention at high RPS.
type RedisBreaker struct {
	state            int32 // CircuitState
	failures         int64
	successes        int64
	lastOpenedUnix   int64 // time.UnixNano
	failThreshold    int64
	successThreshold int64
	openTimeout      time.Duration
}

// NewRedisBreaker initializes a new RedisBreaker.
func NewRedisBreaker(failThreshold, successThreshold int64, openTimeout time.Duration) *RedisBreaker {
	return &RedisBreaker{
		state:            int32(CircuitClosed),
		failThreshold:    failThreshold,
		successThreshold: successThreshold,
		openTimeout:      openTimeout,
	}
}

// State returns the current CircuitState of the breaker.
func (b *RedisBreaker) State() CircuitState {
	return CircuitState(atomic.LoadInt32(&b.state))
}

// Allow determines if a request is allowed to proceed to Redis.
func (b *RedisBreaker) Allow() bool {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitClosed) {
		return true
	}

	if state == int32(CircuitOpen) {
		lastOpened := atomic.LoadInt64(&b.lastOpenedUnix)
		if time.Since(time.Unix(0, lastOpened)) >= b.openTimeout {
			// Single-probe synchronization: only one thread transitions the breaker to HalfOpen.
			// Concurrent threads are blocked to prevent stampede of requests if Redis is still dead.
			if atomic.CompareAndSwapInt32(&b.state, int32(CircuitOpen), int32(CircuitHalfOpen)) {
				atomic.StoreInt64(&b.successes, 0)
				atomic.StoreInt64(&b.failures, 0)
				return true
			}
		}
		return false
	}

	// In CircuitHalfOpen state, concurrent requests are rejected while the probe is in flight.
	return false
}

// RecordSuccess records a successful Redis command.
func (b *RedisBreaker) RecordSuccess() {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitHalfOpen) {
		successes := atomic.AddInt64(&b.successes, 1)
		if successes >= b.successThreshold {
			if atomic.CompareAndSwapInt32(&b.state, int32(CircuitHalfOpen), int32(CircuitClosed)) {
				atomic.StoreInt64(&b.failures, 0)
			}
		}
	} else if state == int32(CircuitClosed) {
		// Reset failures upon success on closed state to allow tolerance for intermittent faults.
		atomic.StoreInt64(&b.failures, 0)
	}
}

// RecordFailure records a failed Redis command.
func (b *RedisBreaker) RecordFailure() {
	state := atomic.LoadInt32(&b.state)
	if state == int32(CircuitHalfOpen) {
		// Any failure during HalfOpen re-opens the circuit instantly.
		b.trip()
	} else if state == int32(CircuitClosed) {
		failures := atomic.AddInt64(&b.failures, 1)
		if failures >= b.failThreshold {
			b.trip()
		}
	}
}

func (b *RedisBreaker) trip() {
	atomic.StoreInt64(&b.lastOpenedUnix, time.Now().UnixNano())
	atomic.StoreInt32(&b.state, int32(CircuitOpen))
}

// IsNetworkOrSystemError determines if a go-redis error is a transport/timeout issue
// (which should trigger a breaker failure) rather than a logic/schema issue.
func IsNetworkOrSystemError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.Nil) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	errStr := err.Error()
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "client is closed") ||
		strings.Contains(errStr, "use of closed network connection") {
		return true
	}
	return false
}

// RedisCircuitBreakerHook transparently intercepts go-redis commands.
type RedisCircuitBreakerHook struct {
	breaker *RedisBreaker
}

// NewRedisCircuitBreakerHook initializes a new RedisCircuitBreakerHook.
func NewRedisCircuitBreakerHook(breaker *RedisBreaker) *RedisCircuitBreakerHook {
	return &RedisCircuitBreakerHook{breaker: breaker}
}

// DialHook intercepts client dial connections. Left unmodified.
func (h *RedisCircuitBreakerHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook intercepts a single redis command.
func (h *RedisCircuitBreakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !h.breaker.Allow() {
			cmd.SetErr(ErrRedisCircuitOpen)
			return ErrRedisCircuitOpen
		}

		err := next(ctx, cmd)
		if err != nil {
			if IsNetworkOrSystemError(err) {
				h.breaker.RecordFailure()
			} else {
				h.breaker.RecordSuccess()
			}
		} else {
			h.breaker.RecordSuccess()
		}
		return err
	}
}

// ProcessPipelineHook intercepts pipeline queries.
func (h *RedisCircuitBreakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.breaker.Allow() {
			for _, cmd := range cmds {
				cmd.SetErr(ErrRedisCircuitOpen)
			}
			return ErrRedisCircuitOpen
		}

		err := next(ctx, cmds)
		if err != nil {
			if IsNetworkOrSystemError(err) {
				h.breaker.RecordFailure()
			} else {
				h.breaker.RecordSuccess()
			}
		} else {
			h.breaker.RecordSuccess()
		}
		return err
	}
}
