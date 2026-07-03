package filter

import (
	"context"
	"time"

	"espx/internal/domain"
)

// AttachFilterDeadline exports monotonic deadline attachment for cross-package chaos tests.
func AttachFilterDeadline(ctx context.Context, timeout time.Duration) context.Context {
	return attachFilterDeadline(ctx, timeout)
}

// FilterDeadlineExceeded reports whether the monotonic filter deadline has passed.
func FilterDeadlineExceeded(ctx context.Context) bool {
	return filterDeadlineExceeded(ctx)
}

// FilterDeadlineExceededEvt reports whether the filter deadline on evt or ctx has elapsed.
func FilterDeadlineExceededEvt(evt *domain.Event, ctx context.Context) bool {
	return filterDeadlineExceededEvt(evt, ctx)
}

// FilterDeadlineRemainingEvt returns remaining filter budget from evt or ctx.
func FilterDeadlineRemainingEvt(evt *domain.Event, ctx context.Context) (time.Duration, bool) {
	return filterDeadlineRemainingEvt(evt, ctx)
}

// FilterDeadlineMonoFromContext returns the monotonic nanosecond deadline attached to ctx.
func FilterDeadlineMonoFromContext(ctx context.Context) (int64, bool) {
	return filterDeadlineMonoFromContext(ctx)
}
