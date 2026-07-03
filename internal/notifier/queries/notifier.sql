-- name: CreateNotification :one
INSERT INTO notifier.notifications (
  id, provider, recipient, title, body, status
) VALUES ($1, $2, $3, $4, $5, 'PENDING')
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifier.notifications
WHERE id = $1;

-- name: GetPendingNotificationsForUpdate :many
-- Backoff base (5s) must match retryBackoffBase in retry_backoff.go.
SELECT * FROM notifier.notifications
WHERE status = 'PENDING'
  AND (
    retry_count = 0
    OR updated_at + (5 * power(2, retry_count - 1) * interval '1 second') <= now()
  )
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: UpdateNotificationStatus :one
UPDATE notifier.notifications
SET status = $2,
    provider = COALESCE(sqlc.narg('provider')::notifier.provider, provider),
    retry_count = COALESCE(sqlc.narg('retry_count')::int, retry_count),
    error_message = COALESCE(sqlc.narg('error_message')::text, error_message),
    updated_at = now()
WHERE id = $1
RETURNING *;
