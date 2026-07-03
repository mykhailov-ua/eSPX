package notifier

import (
	"testing"
)

// Guards first retry waits the base backoff duration.
func TestBackoffDuration_firstRetry(t *testing.T) {
	got := backoffDuration(1)
	if got != retryBackoffBase {
		t.Fatalf("expected %v, got %v", retryBackoffBase, got)
	}
}

// Guards exponential backoff doubles per retry attempt.
func TestBackoffDuration_exponential(t *testing.T) {
	got := backoffDuration(3)
	want := retryBackoffBase * 4
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// Guards zero retry count has no backoff delay.
func TestBackoffDuration_zeroRetries(t *testing.T) {
	if got := backoffDuration(0); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}
