package adminapi

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestCollectFanOut_allSourcesOK(t *testing.T) {
	t.Parallel()
	collector := NewFanOutCollector(nil, "test_ok")
	sources := []FanOutSource[int]{
		{ID: "0", Poll: func(ctx context.Context) ([]int, error) { return []int{1}, nil }},
		{ID: "1", Poll: func(ctx context.Context) ([]int, error) { return []int{2}, nil }},
	}
	result := CollectFanOut(context.Background(), collector, sources)
	require.False(t, result.Partial)
	require.Empty(t, result.Errors)
	require.Len(t, result.Items, 2)
}

func TestCollectFanOut_partialFailure(t *testing.T) {
	t.Parallel()
	collector := NewFanOutCollector(nil, "test_partial")
	sources := []FanOutSource[int]{
		{ID: "0", Poll: func(ctx context.Context) ([]int, error) { return []int{10}, nil }},
		{ID: "1", Poll: func(ctx context.Context) ([]int, error) { return nil, errors.New("down") }},
		{ID: "2", Poll: func(ctx context.Context) ([]int, error) { return []int{30}, nil }},
		{ID: "3", Poll: func(ctx context.Context) ([]int, error) { return []int{40}, nil }},
	}
	result := CollectFanOut(context.Background(), collector, sources)
	require.True(t, result.Partial)
	require.Len(t, result.Errors, 1)
	require.Equal(t, "1", result.Errors[0].Source)
	require.Len(t, result.Items, 3)
}

func TestCollectFanOut_respectsConcurrencyCap(t *testing.T) {
	t.Parallel()
	collector := &FanOutCollector{maxConcurrency: 2, perSourceTO: time.Second, route: "cap"}
	var peak atomic.Int32
	var current atomic.Int32
	sources := make([]FanOutSource[int], 0, 6)
	for i := 0; i < 6; i++ {
		sources = append(sources, FanOutSource[int]{
			ID: "s",
			Poll: func(ctx context.Context) ([]int, error) {
				cur := current.Add(1)
				for {
					p := peak.Load()
					if cur > p {
						if peak.CompareAndSwap(p, cur) {
							break
						}
						continue
					}
					break
				}
				time.Sleep(20 * time.Millisecond)
				current.Add(-1)
				return []int{1}, nil
			},
		})
	}
	_ = CollectFanOut(context.Background(), collector, sources)
	assert.LessOrEqual(t, peak.Load(), int32(2))
}

func TestFanOutCursor_roundTrip(t *testing.T) {
	t.Parallel()
	state := map[string]string{"0": "1234-0", "pg": "42"}
	encoded, err := EncodeFanOutCursor(state)
	require.NoError(t, err)
	decoded, err := DecodeFanOutCursor(encoded)
	require.NoError(t, err)
	assert.Equal(t, state["0"], decoded["0"])
	assert.Equal(t, state["pg"], decoded["pg"])
}

func TestParseDLQRouteID(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 2, parseDLQShardFromRoute("shard-2-1700000000000-0"))
	assert.Equal(t, "1700000000000-0", parseDLQEntryIDFromRoute("shard-2-1700000000000-0"))
}
