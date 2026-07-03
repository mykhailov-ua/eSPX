package notifier

import (
	"sync/atomic"
	"time"
)

type CircuitState int32

const (
	CircuitClosed   CircuitState = 0
	CircuitOpen     CircuitState = 1
	CircuitHalfOpen CircuitState = 2
)

// CircuitBreaker fast-fails provider calls after consecutive failures to limit cascade load.
type CircuitBreaker struct {
	state            int32
	failures         int64
	successes        int64
	lastOpenedUnix   int64
	failThreshold    int64
	successThreshold int64
	openTimeout      time.Duration
}

// NewCircuitBreaker opens the circuit after failThreshold failures and closes after successThreshold probes succeed.
func NewCircuitBreaker(failThreshold, successThreshold int64, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            int32(CircuitClosed),
		failThreshold:    failThreshold,
		successThreshold: successThreshold,
		openTimeout:      openTimeout,
	}
}

func (breaker *CircuitBreaker) State() CircuitState {
	return CircuitState(atomic.LoadInt32(&breaker.state))
}

func (breaker *CircuitBreaker) Allow() bool {
	state := atomic.LoadInt32(&breaker.state)
	if state == int32(CircuitClosed) || state == int32(CircuitHalfOpen) {
		return true
	}

	if state == int32(CircuitOpen) {
		lastOpened := atomic.LoadInt64(&breaker.lastOpenedUnix)
		if time.Since(time.Unix(0, lastOpened)) >= breaker.openTimeout {
			if atomic.CompareAndSwapInt32(&breaker.state, int32(CircuitOpen), int32(CircuitHalfOpen)) {
				atomic.StoreInt64(&breaker.successes, 0)
				atomic.StoreInt64(&breaker.failures, 0)
				return true
			}
		}
		return false
	}

	return false
}

func (breaker *CircuitBreaker) RecordSuccess() {
	state := atomic.LoadInt32(&breaker.state)
	if state == int32(CircuitHalfOpen) {
		successes := atomic.AddInt64(&breaker.successes, 1)
		if successes >= breaker.successThreshold {
			if atomic.CompareAndSwapInt32(&breaker.state, int32(CircuitHalfOpen), int32(CircuitClosed)) {
				atomic.StoreInt64(&breaker.failures, 0)
			}
		}
	} else if state == int32(CircuitClosed) {
		atomic.StoreInt64(&breaker.failures, 0)
	}
}

func (breaker *CircuitBreaker) RecordFailure() {
	state := atomic.LoadInt32(&breaker.state)
	if state == int32(CircuitHalfOpen) {
		breaker.trip()
	} else if state == int32(CircuitClosed) {
		failures := atomic.AddInt64(&breaker.failures, 1)
		if failures >= breaker.failThreshold {
			breaker.trip()
		}
	}
}

func (breaker *CircuitBreaker) trip() {
	for {
		state := atomic.LoadInt32(&breaker.state)
		if state == int32(CircuitOpen) {
			return
		}
		if atomic.CompareAndSwapInt32(&breaker.state, state, int32(CircuitOpen)) {
			atomic.StoreInt64(&breaker.lastOpenedUnix, time.Now().UnixNano())
			return
		}
	}
}
