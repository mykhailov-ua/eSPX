package management

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOutboxPollBackoff_ActiveThenIdle(t *testing.T) {
	b := newOutboxPollBackoff()

	assert.Equal(t, time.Duration(0), b.next(5), "work found resets to immediate repoll")
	assert.Equal(t, 40*time.Millisecond, b.next(0))
	assert.Equal(t, 80*time.Millisecond, b.next(0))
	assert.Equal(t, 160*time.Millisecond, b.next(0))
	assert.Equal(t, outboxPollIdleMax, b.next(0))
	assert.Equal(t, outboxPollIdleMax, b.next(0), "caps at idle max")
}

func TestOutboxPollBackoff_IdleMedianAboveDoD(t *testing.T) {
	b := newOutboxPollBackoff()
	var samples []time.Duration
	for i := 0; i < 8; i++ {
		samples = append(samples, b.next(0))
	}
	// 20→40→80→160→250→250→250→250 ms; p50 = 160 ms > 50 ms DoD
	assert.Greater(t, samples[3], 50*time.Millisecond)
	assert.Equal(t, outboxPollIdleMax, samples[len(samples)-1])
}
