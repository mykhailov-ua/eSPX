package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterEngine_monoDeadlineCheck(t *testing.T) {
	ctx := attachFilterDeadline(context.Background(), 50*time.Millisecond)
	require.False(t, filterDeadlineExceeded(ctx))
	time.Sleep(60 * time.Millisecond)
	assert.True(t, filterDeadlineExceeded(ctx))
}

func TestFilterEngine_deadlineBetweenFilters(t *testing.T) {
	slow := &slowFilter{delay: 200 * time.Millisecond}
	fast := &countingFilter{}
	engine := NewFilterEngine(30*time.Millisecond, slow, fast)

	err := engine.Check(context.Background(), &campaignmodel.Event{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Equal(t, 0, fast.calls)
}

func TestFilterEngine_noTimeoutRunsAll(t *testing.T) {
	first := &countingFilter{}
	second := &countingFilter{}
	engine := NewFilterEngine(0, first, second)

	err := engine.Check(context.Background(), &campaignmodel.Event{})
	require.NoError(t, err)
	assert.Equal(t, 1, first.calls)
	assert.Equal(t, 1, second.calls)
}

func TestFilterEngine_deadlineAttachedToContext(t *testing.T) {
	var gotDeadline bool
	checker := &countingFilter{}
	engine := NewFilterEngine(50*time.Millisecond, &deadlineProbeFilter{&gotDeadline})

	err := engine.Check(context.Background(), &campaignmodel.Event{})
	require.NoError(t, err)
	assert.True(t, gotDeadline)
	_ = checker
}

func TestCachedTimeUTC_stableWithinSecond(t *testing.T) {
	storeCachedNowUTC()
	a := CachedTimeUTC()
	b := CachedTimeUTC()
	assert.Equal(t, a, b)
}

func TestCachedTimeIn_nonUTC(t *testing.T) {
	storeCachedNowUTC()
	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	got := CachedTimeIn(loc)
	want := CachedTimeUTC().In(loc)
	assert.Equal(t, want, got)
}

// Tracks filter engine check cost without deadline enforcement.
func BenchmarkFilterEngine_Check_noTimeout(b *testing.B) {
	engine := NewFilterEngine(0, &countingFilter{})
	evt := &campaignmodel.Event{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Check(ctx, evt)
	}
}

// Tracks filter engine check cost with deadline enforcement enabled.
func BenchmarkFilterEngine_Check_withDeadline(b *testing.B) {
	engine := NewFilterEngine(5*time.Second, &countingFilter{})
	evt := &campaignmodel.Event{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Check(ctx, evt)
	}
}
