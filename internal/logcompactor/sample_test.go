package logcompactor

import (
	"fmt"
	"testing"

	"espx/internal/ingestion/pb"

	"github.com/stretchr/testify/assert"
)

func TestShouldKeepEvent_billableAlwaysKept(t *testing.T) {
	for _, eventType := range []string{"click", "conversion"} {
		evt := &pb.AdStreamEvent{EventType: []byte(eventType), ClickId: []byte("x")}
		assert.True(t, shouldKeepEvent(evt, 1000))
	}
}

func TestShouldKeepEvent_fraudAlwaysKept(t *testing.T) {
	evt := &pb.AdStreamEvent{
		EventType:  []byte("impression"),
		ClickId:    []byte("fraud-click"),
		FraudScore: 42,
	}
	assert.True(t, shouldKeepEvent(evt, 1000))
}

func TestShouldKeepEvent_impressionDeterministic(t *testing.T) {
	seen := 0
	const n = 10_000
	for i := 0; i < n; i++ {
		clickID := []byte(fmt.Sprintf("click-%d", i))
		evt := &pb.AdStreamEvent{EventType: []byte("impression"), ClickId: clickID}
		if shouldKeepEvent(evt, 1000) {
			seen++
			assert.True(t, shouldKeepEvent(evt, 1000), "sampling must be deterministic")
		}
	}
	assert.InDelta(t, float64(n)/1000, float64(seen), float64(n)*0.05)
}

func TestShouldKeepEvent_emptyClickIDDropped(t *testing.T) {
	evt := &pb.AdStreamEvent{EventType: []byte("impression")}
	assert.False(t, shouldKeepEvent(evt, 1000))
}
