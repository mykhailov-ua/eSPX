-- name: CreatePaymentIntent :one
INSERT INTO payment.payment_intents (
  id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING *;

-- name: GetPaymentIntent :one
SELECT * FROM payment.payment_intents
WHERE id = $1;

-- name: GetPaymentIntentByIdempotencyKey :one
SELECT * FROM payment.payment_intents
WHERE idempotency_key = $1;

-- name: ListPaymentIntents :many
SELECT * FROM payment.payment_intents
WHERE customer_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: CountPaymentIntents :one
SELECT COUNT(*) FROM payment.payment_intents
WHERE customer_id = $1;

-- name: UpdatePaymentIntentStatus :one
UPDATE payment.payment_intents
SET status = $2,
    provider_ref = COALESCE(sqlc.narg('provider_ref')::text, provider_ref),
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: CreateWebhookEvent :one
INSERT INTO payment.webhook_events (
  provider, provider_event_id, event_type, payload_hash, payload_redacted, status, error_message
) VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetWebhookEvent :one
SELECT * FROM payment.webhook_events
WHERE provider = $1 AND provider_event_id = $2;

-- name: UpdateWebhookEventStatus :exec
UPDATE payment.webhook_events
SET status = $3,
    error_message = COALESCE(sqlc.narg('error_message')::text, error_message),
    processed_at = now()
WHERE provider = $1 AND provider_event_id = $2;

-- name: CreateOutboxEvent :one
INSERT INTO payment.payment_outbox (
  event_type, payload, status
) VALUES ($1, $2, 'PENDING')
RETURNING *;

-- name: GetPendingOutboxEventsForUpdate :many
SELECT * FROM payment.payment_outbox
WHERE status = 'PENDING'
   OR (status = 'PROCESSING' AND lease_until < now())
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: LeaseOutboxEvents :exec
UPDATE payment.payment_outbox
SET status = 'PROCESSING',
    lease_until = $2,
    attempts = attempts + 1
WHERE id = ANY($1::bigint[]);

-- name: MarkOutboxEventProcessed :exec
UPDATE payment.payment_outbox
SET status = 'PROCESSED',
    processed_at = now()
WHERE id = $1;

-- name: MarkOutboxEventFailed :exec
UPDATE payment.payment_outbox
SET status = CASE WHEN attempts >= $2 THEN 'DEAD'::payment.outbox_status ELSE 'PENDING'::payment.outbox_status END,
    last_error = $3,
    lease_until = NULL
WHERE id = $1;
