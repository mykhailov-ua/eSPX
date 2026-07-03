package notifier

import (
	"testing"
	"time"
)

// Guards closed breaker allows traffic and resets failure count after a successful call.
func TestCircuitBreaker_closedAllowsAndResetsFailures(t *testing.T) {
	breaker := NewCircuitBreaker(2, 2, time.Second)

	if !breaker.Allow() {
		t.Fatal("expected closed breaker to allow requests")
	}

	breaker.RecordFailure()
	breaker.RecordSuccess()

	if breaker.State() != CircuitClosed {
		t.Fatalf("expected closed state, got %v", breaker.State())
	}
}

// Guards consecutive failures trip the breaker to open.
func TestCircuitBreaker_tripsAfterFailThreshold(t *testing.T) {
	breaker := NewCircuitBreaker(2, 2, time.Second)

	breaker.RecordFailure()
	breaker.RecordFailure()

	if breaker.State() != CircuitOpen {
		t.Fatalf("expected open state, got %v", breaker.State())
	}
	if breaker.Allow() {
		t.Fatal("expected open breaker to reject requests before timeout")
	}
}

// Guards open breaker transitions to half-open after the open timeout elapses.
func TestCircuitBreaker_halfOpenAfterTimeout(t *testing.T) {
	breaker := NewCircuitBreaker(1, 1, 10*time.Millisecond)

	breaker.RecordFailure()
	if breaker.State() != CircuitOpen {
		t.Fatalf("expected open state, got %v", breaker.State())
	}

	time.Sleep(15 * time.Millisecond)

	if !breaker.Allow() {
		t.Fatal("expected half-open probe after open timeout")
	}
	if breaker.State() != CircuitHalfOpen {
		t.Fatalf("expected half-open state, got %v", breaker.State())
	}
}

// Guards half-open breaker closes after enough consecutive successes.
func TestCircuitBreaker_closesFromHalfOpen(t *testing.T) {
	breaker := NewCircuitBreaker(1, 2, time.Second)

	breaker.trip()
	atomicSetState(breaker, CircuitHalfOpen)

	breaker.RecordSuccess()
	if breaker.State() != CircuitHalfOpen {
		t.Fatalf("expected half-open after first success, got %v", breaker.State())
	}

	breaker.RecordSuccess()
	if breaker.State() != CircuitClosed {
		t.Fatalf("expected closed after success threshold, got %v", breaker.State())
	}
}

// Guards half-open probe failure reopens the breaker immediately.
func TestCircuitBreaker_reopensOnHalfOpenFailure(t *testing.T) {
	breaker := NewCircuitBreaker(1, 2, time.Second)

	breaker.trip()
	atomicSetState(breaker, CircuitHalfOpen)

	breaker.RecordFailure()
	if breaker.State() != CircuitOpen {
		t.Fatalf("expected open after half-open failure, got %v", breaker.State())
	}
}

func atomicSetState(breaker *CircuitBreaker, state CircuitState) {
	breaker.state = int32(state)
}
