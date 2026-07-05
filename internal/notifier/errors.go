package notifier

import "errors"

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
