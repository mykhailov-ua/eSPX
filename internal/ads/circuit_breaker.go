package ads

import (
	"sync"
	"time"
)

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

// CircuitBreaker implements thread-safe status transitions and worker-granular failure isolation.
//
// Concurrency:
// - Thread-safe. Read/write operations on state and failure maps are protected by a local Mutex lock.
//
// Circuit Breaker State Machine & Math:
// 1. Closed (CircuitClosed = 0):
//   - All actions are allowed (Allow() returns true).
//   - Worker-granular failures are recorded in the failures map.
//   - Transition to Open: Triggered if failures[workerID] >= failThreshold.
//
// 2. Open (CircuitOpen = 1):
//   - Inbound actions are rejected (Allow() returns false).
//   - Transition to Half-Open: Occurs if time.Since(lastOpenedAt) >= openTimeout when Allow() is evaluated.
//
// 3. Half-Open (CircuitHalfOpen = 2):
//   - First check after Open cooldown completes allows a single pilot execution.
//   - Transition to Closed: Triggered if the pilot execution is successful (RecordSuccess).
//     The entire failures map is cleared.
//   - Transition to Open: Triggered if the pilot execution fails (RecordFailure) or is cancelled.
//     Cooldown interval is reset.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         CircuitState
	failures      map[string]int32
	lastOpenedAt  time.Time
	failThreshold int32
	openTimeout   time.Duration
}

// NewCircuitBreaker initializes a circuit breaker instance. The threshold is tracked per-worker
// to ensure granular worker faults trigger the breaker rather than global system noise.
func NewCircuitBreaker(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:         CircuitClosed,
		failures:      make(map[string]int32),
		failThreshold: int32(failThreshold),
		openTimeout:   openTimeout,
	}
}

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

func (cb *CircuitBreaker) RecordCancellation(workerID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.lastOpenedAt = time.Now()
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) Failures(workerID string) int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return int(cb.failures[workerID])
}

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
