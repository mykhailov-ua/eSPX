package filter

import (
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/ads/clock"
	"espx/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type slowFilter struct {
	delay time.Duration
}

func (f *slowFilter) Check(ctx context.Context, evt *domain.Event) error {
	delay := f.delay
	if rem, ok := filterDeadlineRemaining(ctx); ok && rem < delay {
		delay = rem
	}
	if delay <= 0 {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		if filterDeadlineExceeded(ctx) {
			return context.DeadlineExceeded
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type countingFilter struct {
	calls int
	err   error
}

func (f *countingFilter) Check(ctx context.Context, evt *domain.Event) error {
	f.calls++
	return f.err
}

type deadlineProbeFilter struct {
	seen *bool
}

func (f *deadlineProbeFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt != nil && evt.FilterDeadlineMono > 0 {
		*f.seen = true
		return nil
	}
	_, ok := filterDeadlineMonoFromContext(ctx)
	*f.seen = ok
	return nil
}

// Guards filter engine uses monotonic clock for deadline checks under load.
func TestFilterEngine_monoDeadlineCheck(t *testing.T) {
	ctx := attachFilterDeadline(context.Background(), 50*time.Millisecond)
	require.False(t, filterDeadlineExceeded(ctx))
	time.Sleep(60 * time.Millisecond)
	assert.True(t, filterDeadlineExceeded(ctx))
}

// Guards engine stops running filters once deadline expires mid-chain.
func TestFilterEngine_deadlineBetweenFilters(t *testing.T) {
	slow := &slowFilter{delay: 200 * time.Millisecond}
	fast := &countingFilter{}
	engine := NewFilterEngine(30*time.Millisecond, slow, fast)

	err := engine.Check(context.Background(), &domain.Event{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Equal(t, 0, fast.calls)
}

// Guards zero deadline runs the full filter chain without early exit.
func TestFilterEngine_noTimeoutRunsAll(t *testing.T) {
	first := &countingFilter{}
	second := &countingFilter{}
	engine := NewFilterEngine(0, first, second)

	err := engine.Check(context.Background(), &domain.Event{})
	require.NoError(t, err)
	assert.Equal(t, 1, first.calls)
	assert.Equal(t, 1, second.calls)
}

// Guards filter context carries deadline for downstream Redis calls.
func TestFilterEngine_deadlineAttachedToContext(t *testing.T) {
	var gotDeadline bool
	engine := NewFilterEngine(50*time.Millisecond, &deadlineProbeFilter{&gotDeadline})

	err := engine.Check(context.Background(), &domain.Event{})
	require.NoError(t, err)
	assert.True(t, gotDeadline)
}

// Guards cached UTC time stays stable within a second to cut time syscalls.
func TestCachedTimeUTC_stableWithinSecond(t *testing.T) {
	clock.RefreshWallClockNow()
	a := clock.CachedTimeUTC()
	b := clock.CachedTimeUTC()
	assert.Equal(t, a, b)
}

// Guards cached local timezone conversion matches wall-clock expectations.
func TestCachedTimeIn_nonUTC(t *testing.T) {
	clock.RefreshWallClockNow()
	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	got := clock.CachedTimeIn(loc)
	want := clock.CachedTimeUTC().In(loc)
	assert.Equal(t, want, got)
}

// Tracks filter engine check cost without deadline enforcement.
func BenchmarkFilterEngine_Check_noTimeout(b *testing.B) {
	engine := NewFilterEngine(0, &countingFilter{})
	evt := &domain.Event{}
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
	evt := &domain.Event{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Check(ctx, evt)
	}
}
