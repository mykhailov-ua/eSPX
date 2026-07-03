package notifier

import "time"

// retryBackoffBase must match the 5s base in queries/notifier.sql GetPendingNotificationsForUpdate.
const (
	maxDeliveryAttempts = 5
	workerBatchSize     = 10
	workerErrorBackoff  = 2 * time.Second
	retryBackoffBase    = 5 * time.Second
)

func backoffDuration(retryCount int32) time.Duration {
	if retryCount <= 0 {
		return 0
	}
	return retryBackoffBase * time.Duration(1<<(retryCount-1))
}
