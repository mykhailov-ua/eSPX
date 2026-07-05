package notifier

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrRecipientRequired     = errors.New("recipient is required")
	ErrBodyRequired          = errors.New("body is required")
	ErrInvalidNotificationID = errors.New("invalid notification ID")
	ErrNotificationNotFound  = errors.New("notification not found")
	ErrUnsupportedProvider   = errors.New("unsupported provider")
	ErrCircuitOpen           = errors.New("circuit breaker is open")
	ErrRateLimited           = errors.New("recipient rate limit exceeded")
	ErrBatchEmpty            = errors.New("batch must contain at least one notification")
)

// ProviderRateLimitedError signals an upstream provider asked us to slow down (e.g. Telegram 429).
type ProviderRateLimitedError struct {
	Provider   string
	RetryAfter time.Duration
}

func (err *ProviderRateLimitedError) Error() string {
	return fmt.Sprintf("%s rate limited, retry after %s", err.Provider, err.RetryAfter)
}
