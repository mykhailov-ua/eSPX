-- name: CreateNotification :one
INSERT INTO notifier.notifications (
  id, provider, recipient, title, body, status, delivery_mode, broadcast_providers, dedup_key,
  template_id, template_vars, attachment_url
) VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, $7, $8, $9, $10, $11)
RETURNING *;

-- name: GetTemplate :one
SELECT * FROM notifier.templates WHERE id = $1;

-- name: RetryNotification :one
UPDATE notifier.notifications
SET status = 'PENDING',
    retry_count = 0,
    error_message = NULL,
    claimed_at = NULL,
    updated_at = now()
WHERE id = $1 AND status = 'FAILED'
RETURNING *;

-- name: FindActiveNotificationByDedupKey :one
SELECT * FROM notifier.notifications
WHERE dedup_key = $1
  AND status IN ('PENDING', 'PROCESSING')
  AND created_at > NOW() - ($2::bigint * interval '1 second')
ORDER BY created_at DESC
LIMIT 1;

-- name: GetNotification :one
SELECT * FROM notifier.notifications
WHERE id = $1;

-- name: ReclaimStaleProcessing :execrows
UPDATE notifier.notifications
SET status = 'PENDING',
    claimed_at = NULL,
    updated_at = now()
WHERE status = 'PROCESSING'
  AND claimed_at IS NOT NULL
  AND claimed_at < NOW() - ($1::bigint * interval '1 second');

-- name: ClaimPendingNotifications :many
WITH due AS (
  SELECT id FROM notifier.notifications
  WHERE status = 'PENDING'
    AND (
      retry_count = 0
      OR updated_at + (5 * power(2, retry_count - 1) * interval '1 second') <= now()
    )
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE notifier.notifications AS n
SET status = 'PROCESSING',
    claimed_at = now(),
    updated_at = now()
FROM due
WHERE n.id = due.id
RETURNING n.*;

-- name: UpdateNotificationStatus :one
UPDATE notifier.notifications
SET status = $2,
    provider = COALESCE(sqlc.narg('provider')::notifier.provider, provider),
    retry_count = COALESCE(sqlc.narg('retry_count')::int, retry_count),
    error_message = COALESCE(sqlc.narg('error_message')::text, error_message),
    claimed_at = CASE
      WHEN $2 = 'PENDING'::notifier.notification_status THEN NULL
      WHEN $2 = 'PROCESSING'::notifier.notification_status THEN COALESCE(sqlc.narg('claimed_at')::timestamptz, now())
      ELSE claimed_at
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;
