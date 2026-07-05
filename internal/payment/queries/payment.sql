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

-- name: CreatePaymentRefund :one
INSERT INTO payment.payment_refunds (
  id, payment_intent_id, provider, provider_refund_id, amount_micro, status
) VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetPaymentRefundByProviderRefundID :one
SELECT * FROM payment.payment_refunds
WHERE provider = $1 AND provider_refund_id = $2;

-- name: UpdatePaymentRefundStatus :exec
UPDATE payment.payment_refunds
SET status = $3,
    updated_at = now()
WHERE provider = $1 AND provider_refund_id = $2;

-- name: ApplyIntentRefundAmount :one
UPDATE payment.payment_intents
SET refunded_amount_micro = refunded_amount_micro + $2,
    status = CASE
      WHEN refunded_amount_micro + $2 >= amount_micro THEN 'REFUNDED'::payment.payment_intent_status
      ELSE status
    END,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: CreatePaymentDispute :one
INSERT INTO payment.payment_disputes (
  id, payment_intent_id, provider, provider_dispute_id, amount_micro, status
) VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetPaymentDisputeByProviderDisputeID :one
SELECT * FROM payment.payment_disputes
WHERE provider = $1 AND provider_dispute_id = $2;

-- name: UpdatePaymentDisputeStatus :exec
UPDATE payment.payment_disputes
SET status = $3,
    updated_at = now()
WHERE provider = $1 AND provider_dispute_id = $2;

-- name: ApplyDisputeFundsWithdrawn :one
UPDATE payment.payment_disputes
SET withdrawn_amount_micro = withdrawn_amount_micro + $3,
    status = 'FUNDS_WITHDRAWN'::payment.dispute_status,
    updated_at = now()
WHERE provider = $1 AND provider_dispute_id = $2
RETURNING *;

-- name: ApplyDisputeFundsReinstated :one
UPDATE payment.payment_disputes
SET reinstated_amount_micro = reinstated_amount_micro + $3,
    status = 'FUNDS_REINSTATED'::payment.dispute_status,
    updated_at = now()
WHERE provider = $1 AND provider_dispute_id = $2
RETURNING *;

-- name: ClosePaymentDispute :exec
UPDATE payment.payment_disputes
SET status = $3,
    updated_at = now()
WHERE provider = $1 AND provider_dispute_id = $2;

-- name: CreateFinancialReconRun :one
INSERT INTO payment.financial_recon_runs (period_start, period_end, status)
VALUES ($1, $2, 'PENDING')
RETURNING *;

-- name: CompleteFinancialReconRun :exec
UPDATE payment.financial_recon_runs
SET status = 'COMPLETED',
    findings_count = $2,
    intents_checked = $3,
    completed_at = now()
WHERE id = $1;

-- name: FailFinancialReconRun :exec
UPDATE payment.financial_recon_runs
SET status = 'FAILED',
    error_message = $2,
    completed_at = now()
WHERE id = $1;

-- name: CreateFinancialReconFinding :one
INSERT INTO payment.financial_recon_findings (
  run_id, kind, payment_intent_id, customer_id,
  payment_amount_micro, ledger_amount_micro, delta_micro, detail
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;

-- name: CountFinancialReconFindings :one
SELECT COUNT(*) FROM payment.financial_recon_findings WHERE run_id = $1;

-- name: ListIntentsForFinancialRecon :many
SELECT * FROM payment.payment_intents
WHERE status IN ('SUCCEEDED', 'REFUNDED', 'DISPUTED', 'SETTLEMENT_FAILED')
ORDER BY updated_at DESC;

-- name: SumRefundsByIntentForRecon :many
SELECT payment_intent_id, COALESCE(SUM(amount_micro), 0)::bigint AS refund_micro
FROM payment.payment_refunds
WHERE status = 'SUCCEEDED'
GROUP BY payment_intent_id;

-- name: SumDisputeWithdrawnByIntentForRecon :many
SELECT payment_intent_id,
       COALESCE(SUM(withdrawn_amount_micro), 0)::bigint AS withdrawn_micro,
       COALESCE(SUM(reinstated_amount_micro), 0)::bigint AS reinstated_micro
FROM payment.payment_disputes
GROUP BY payment_intent_id;

-- name: ListDeadOutboxEventsForRecon :many
SELECT * FROM payment.payment_outbox
WHERE status = 'DEAD'
ORDER BY created_at DESC
LIMIT 500;
