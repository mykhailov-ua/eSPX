package ingest

import (
	"context"
	"time"

	"espx/internal/ads/filter"
	"espx/internal/domain"
)

// Filter stub returning a fixed error for handler error-mapping tests.
type errFilter struct {
	err error
}

func (f *errFilter) Check(ctx context.Context, evt *domain.Event) error {
	return f.err
}

// Filter stub that sleeps until filter deadline for engine timeout tests.
type slowFilter struct {
	delay time.Duration
}

func (f *slowFilter) Check(ctx context.Context, evt *domain.Event) error {
	delay := f.delay
	if rem, ok := filter.FilterDeadlineRemainingEvt(evt, ctx); ok && rem < delay {
		delay = rem
	}
	if delay <= 0 {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		if filter.FilterDeadlineExceededEvt(evt, ctx) {
			return context.DeadlineExceeded
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Filter stub counting Check invocations for engine chain tests.
type countingFilter struct {
	calls int
	err   error
}

func (f *countingFilter) Check(ctx context.Context, evt *domain.Event) error {
	f.calls++
	return f.err
}

// Filter stub recording whether monotonic deadline was attached to context.
type deadlineProbeFilter struct {
	seen *bool
}

func (f *deadlineProbeFilter) Check(ctx context.Context, evt *domain.Event) error {
	if evt != nil && evt.FilterDeadlineMono > 0 {
		*f.seen = true
		return nil
	}
	_, ok := filter.FilterDeadlineMonoFromContext(ctx)
	*f.seen = ok
	return nil
}
