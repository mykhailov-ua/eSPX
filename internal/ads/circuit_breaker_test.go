package ads

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Guards new breakers start closed with zero failures and allow traffic.
func TestCircuitBreaker_StartsInClosedState(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
	assert.Equal(t, 0, cb.Failures("test"))
}

// Guards breaker opens only after configured consecutive failures.
func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	cb.RecordFailure("test")
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())

	cb.RecordFailure("test")
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())

	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
	assert.Equal(t, 3, cb.Failures("test"))
}

// Guards success clears failure count so transient errors do not trip breaker.
func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	cb.RecordFailure("test")
	cb.RecordFailure("test")
	assert.Equal(t, 2, cb.Failures("test"))

	cb.RecordSuccess("test")
	assert.Equal(t, CircuitClosed, cb.State())
	assert.Equal(t, 0, cb.Failures("test"))
	assert.True(t, cb.Allow())
}

// Guards open breaker probes one request after wait duration elapses.
func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure("test")
	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())

	time.Sleep(60 * time.Millisecond)

	assert.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())
}

// Guards successful half-open probe closes breaker and restores traffic.
func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure("test")
	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow())
	cb.RecordSuccess("test")
	assert.Equal(t, CircuitClosed, cb.State())
	assert.True(t, cb.Allow())
}

// Guards failed half-open probe reopens breaker immediately.
func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordFailure("test")
	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow())
	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

// Guards only one concurrent probe runs while half-open.
func TestCircuitBreaker_HalfOpenBlocksConcurrentProbes(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	first := cb.Allow()
	assert.True(t, first)
	assert.Equal(t, CircuitHalfOpen, cb.State())

	assert.False(t, cb.Allow())
	assert.False(t, cb.Allow())
}

// Guards WaitDuration reflects remaining open-state cooldown for callers.
func TestCircuitBreaker_WaitDuration(t *testing.T) {
	cb := NewCircuitBreaker(1, 100*time.Millisecond)

	assert.Equal(t, time.Duration(0), cb.WaitDuration())

	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	d := cb.WaitDuration()
	assert.Greater(t, d, time.Duration(0))
	assert.LessOrEqual(t, d, 100*time.Millisecond)
}

// Guards mixed success and failure under concurrency leave valid state.
func TestCircuitBreaker_ConcurrentMixedOps(t *testing.T) {
	cb := NewCircuitBreaker(50, 10*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cb.Allow()
			if idx%3 == 0 {
				cb.RecordSuccess("test")
			} else {
				cb.RecordFailure("test")
			}
		}(i)
	}
	wg.Wait()

	state := cb.State()
	assert.Contains(t, []CircuitState{CircuitClosed, CircuitOpen, CircuitHalfOpen}, state)
}

// Guards cancelled half-open probe reopens without counting as success.
func TestCircuitBreaker_CancellationResetsHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())

	cb.RecordCancellation("test")
	assert.Equal(t, CircuitOpen, cb.State())

	assert.False(t, cb.Allow())
}

// Guards any half-open failure reopens even below trip threshold.
func TestCircuitBreaker_HalfOpenSingleFailureReopensEvenBelowThreshold(t *testing.T) {
	cb := NewCircuitBreaker(5, 50*time.Millisecond)

	cb.RecordFailure("test")
	cb.RecordFailure("test")
	cb.RecordFailure("test")
	cb.RecordFailure("test")
	cb.RecordFailure("test")
	assert.Equal(t, CircuitOpen, cb.State())

	time.Sleep(60 * time.Millisecond)

	require.True(t, cb.Allow())
	assert.Equal(t, CircuitHalfOpen, cb.State())

	cb.RecordFailure("other-worker")
	assert.Equal(t, CircuitOpen, cb.State())
	assert.False(t, cb.Allow())
}

// Guards per-worker failure counts are not cleared by other workers.
func TestCircuitBreaker_SuccessDoesNotMaskConcurrentWorkerFailures(t *testing.T) {
	cb := NewCircuitBreaker(3, 1*time.Second)

	cb.RecordFailure("worker-A")
	cb.RecordFailure("worker-A")
	assert.Equal(t, 2, cb.Failures("worker-A"))

	cb.RecordSuccess("worker-B")

	assert.Equal(t, 2, cb.Failures("worker-A"))

	cb.RecordFailure("worker-A")
	assert.Equal(t, CircuitOpen, cb.State())
}
