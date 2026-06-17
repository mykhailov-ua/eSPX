package ads

import (
	"sync"
	"time"
)

// CircuitState tracks whether downstream writes are allowed after repeated failures.
type CircuitState int32

// Circuit breaker states for store write protection.
const (
	CircuitClosed   CircuitState = 0
	CircuitOpen     CircuitState = 1
	CircuitHalfOpen CircuitState = 2
)

// String returns a stable label for metrics and logs.
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

// CircuitBreaker isolates failing store workers so one bad shard does not stall the whole consumer.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         CircuitState
	failures      map[string]int32
	lastOpenedAt  time.Time
	failThreshold int32
	openTimeout   time.Duration
}

// NewCircuitBreaker creates a per-worker failure gate for stream flush retries.
func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         CircuitClosed,
		failures:      make(map[string]int32),
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

// Allow reports whether a worker may attempt another store flush.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		if time.Since(cb.lastOpenedAt) >= cb.openTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false

	case CircuitHalfOpen:
		return false

	default:
		return true
	}
}

// RecordSuccess clears worker failures and closes the circuit after a probe succeeds.
func (cb *CircuitBreaker) RecordSuccess(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.failures = make(map[string]int32)
		cb.state = CircuitClosed
	} else {
		delete(cb.failures, workerID)
	}
}

// RecordFailure opens the circuit when a worker exceeds the failure threshold.
func (cb *CircuitBreaker) RecordFailure(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures[workerID]++
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastOpenedAt = time.Now()
		return
	}
	if cb.failures[workerID] >= cb.failThreshold {
		if cb.state != CircuitOpen {
			cb.state = CircuitOpen
			cb.lastOpenedAt = time.Now()
		}
	}
}

// RecordCancellation reopens the circuit when a half-open probe is cancelled mid-flight.
func (cb *CircuitBreaker) RecordCancellation(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastOpenedAt = time.Now()
	}
}

// State returns the current circuit state for observability.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Failures returns the failure count for a specific worker.
func (cb *CircuitBreaker) Failures(workerID string) int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return int(cb.failures[workerID])
}

// WaitDuration returns time until the open circuit may probe again.
func (cb *CircuitBreaker) WaitDuration() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != CircuitOpen {
		return 0
	}
	remaining := cb.openTimeout - time.Since(cb.lastOpenedAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
