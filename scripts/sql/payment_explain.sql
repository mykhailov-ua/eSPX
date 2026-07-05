-- Payment query plans: EXPLAIN (ANALYZE, BUFFERS)
-- Payment schema EXPLAIN baseline (run against db-payment).
-- Run: docker exec -i espx-db-payment-1 psql -h localhost -p 5431 -U espx_payment_user -d espx_payment -f - < scripts/sql/payment_explain.sql

\set ON_ERROR_STOP on
SET client_min_messages TO warning;

-- Seed (~500 intents, 50 webhooks, 30 outbox) for realistic planner stats
INSERT INTO payment.payment_intents (id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata)
SELECT
  gen_random_uuid(),
  ('00000000-0000-4000-8000-' || lpad(to_hex((g % 20)::int), 12, '0'))::uuid,
  1_000_000 + (g % 100) * 10_000,
  'USD',
  (ARRAY['CREATED','PENDING_PROVIDER','PROCESSING','SUCCEEDED','FAILED','CANCELLED']::payment.payment_intent_status[])[1 + (g % 6)],
  'stripe',
  CASE WHEN g % 3 = 0 THEN NULL ELSE 'pi_mock_' || g::text END,
  'idem-seed-' || g::text,
  '{}'::jsonb
FROM generate_series(1, 500) g
ON CONFLICT DO NOTHING;

INSERT INTO payment.webhook_events (provider, provider_event_id, event_type, payload_hash, payload_redacted, status)
SELECT
  'stripe',
  'evt_seed_' || g::text,
  'payment_intent.succeeded',
  decode(repeat('ab', 32), 'hex'),
  '{"id":"evt"}'::jsonb,
  (ARRAY['RECEIVED','PROCESSED','IGNORED','FAILED']::payment.webhook_event_status[])[1 + (g % 4)]
FROM generate_series(1, 50) g
ON CONFLICT DO NOTHING;

INSERT INTO payment.payment_outbox (event_type, payload, status, lease_until, attempts)
SELECT
  'SETTLE_BALANCE',
  jsonb_build_object('customer_id', '00000000-0000-4000-8000-000000000001', 'amount_micro', 1000000),
  (ARRAY['PENDING','PROCESSING','PROCESSED','DEAD']::payment.outbox_status[])[1 + (g % 4)],
  CASE WHEN g % 4 = 1 THEN now() - interval '1 minute' ELSE now() + interval '30 seconds' END,
  g % 6
FROM generate_series(1, 30) g;

ANALYZE payment.payment_intents;
ANALYZE payment.webhook_events;
ANALYZE payment.payment_outbox;

-- Pick stable fixture rows
CREATE TEMP TABLE pay_fix AS
SELECT
  (SELECT id FROM payment.payment_intents ORDER BY created_at DESC LIMIT 1) AS intent_id,
  (SELECT customer_id FROM payment.payment_intents ORDER BY created_at DESC LIMIT 1) AS customer_id,
  (SELECT idempotency_key FROM payment.payment_intents ORDER BY created_at DESC LIMIT 1) AS idem_key,
  (SELECT provider_ref FROM payment.payment_intents WHERE provider_ref IS NOT NULL LIMIT 1) AS provider_ref,
  (SELECT id FROM payment.payment_outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 1) AS outbox_pending_id,
  (SELECT array_agg(id) FROM (SELECT id FROM payment.payment_outbox WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 5) s) AS outbox_ids,
  (SELECT provider_event_id FROM payment.webhook_events LIMIT 1) AS webhook_event_id;

\echo 1. GetPaymentIntent (PK)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.payment_intents
WHERE id = (SELECT intent_id FROM pay_fix);

\echo 2. GetPaymentIntentByIdempotencyKey (UNIQUE)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.payment_intents
WHERE idempotency_key = (SELECT idem_key FROM pay_fix);

\echo 3. ListPaymentIntents (customer + sort)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.payment_intents
WHERE customer_id = (SELECT customer_id FROM pay_fix)
ORDER BY created_at DESC
LIMIT 10 OFFSET 0;

\echo 4. CountPaymentIntents
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*) FROM payment.payment_intents
WHERE customer_id = (SELECT customer_id FROM pay_fix);

\echo 5. CreatePaymentIntent (INSERT, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO payment.payment_intents (
  id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata
) VALUES (
  gen_random_uuid(),
  (SELECT customer_id FROM pay_fix),
  2_500_000,
  'USD',
  'CREATED',
  'stripe',
  'pi_explain_new',
  'idem-explain-new-' || gen_random_uuid()::text,
  '{}'::jsonb
)
RETURNING *;
ROLLBACK;

\echo 6. UpdatePaymentIntentStatus (UPDATE by PK, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_intents
SET status = 'SUCCEEDED',
    provider_ref = COALESCE('pi_explain_updated', provider_ref),
    updated_at = now()
WHERE id = (SELECT intent_id FROM pay_fix)
RETURNING *;
ROLLBACK;

\echo 7. ProcessStripeWebhook intent lookup (provider_ref FOR UPDATE)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, customer_id, amount_micro, currency, status, provider, provider_ref, idempotency_key, metadata, created_at, updated_at
FROM payment.payment_intents
WHERE provider = 'stripe' AND provider_ref = (SELECT provider_ref FROM pay_fix)
FOR UPDATE;
ROLLBACK;

\echo 8. GetWebhookEvent (UNIQUE provider+event_id)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.webhook_events
WHERE provider = 'stripe' AND provider_event_id = (SELECT webhook_event_id FROM pay_fix);

\echo 9. CreateWebhookEvent (INSERT, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO payment.webhook_events (
  provider, provider_event_id, event_type, payload_hash, payload_redacted, status, error_message
) VALUES (
  'stripe',
  'evt_explain_new',
  'payment_intent.succeeded',
  decode(repeat('cd', 32), 'hex'),
  '{"id":"evt_explain_new"}'::jsonb,
  'RECEIVED',
  NULL
)
RETURNING *;
ROLLBACK;

\echo 10. UpdateWebhookEventStatus (UPDATE by UNIQUE key, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.webhook_events
SET status = 'PROCESSED',
    error_message = COALESCE(NULL::text, error_message),
    processed_at = now()
WHERE provider = 'stripe' AND provider_event_id = (SELECT webhook_event_id FROM pay_fix);
ROLLBACK;

\echo 11. CreateOutboxEvent (INSERT, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO payment.payment_outbox (event_type, payload, status)
VALUES ('SETTLE_BALANCE', '{"customer_id":"00000000-0000-4000-8000-000000000001"}'::jsonb, 'PENDING')
RETURNING *;
ROLLBACK;

\echo 12. GetPendingOutboxEventsForUpdate (SKIP LOCKED)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM payment.payment_outbox
WHERE status = 'PENDING' OR (status = 'PROCESSING' AND lease_until < now())
ORDER BY created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
ROLLBACK;

\echo 13. LeaseOutboxEvents (batch UPDATE by id array, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_outbox
SET status = 'PROCESSING',
    lease_until = now() + interval '30 seconds',
    attempts = attempts + 1
WHERE id = ANY((SELECT outbox_ids FROM pay_fix)::bigint[]);
ROLLBACK;

\echo 14. MarkOutboxEventProcessed (UPDATE by PK, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_outbox
SET status = 'PROCESSED',
    processed_at = now()
WHERE id = (SELECT outbox_pending_id FROM pay_fix);
ROLLBACK;

\echo 15. MarkOutboxEventFailed (UPDATE by PK, rolled back)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_outbox
SET status = CASE WHEN attempts >= 5 THEN 'DEAD'::payment.outbox_status ELSE 'PENDING'::payment.outbox_status END,
    last_error = 'explain test error',
    lease_until = NULL
WHERE id = (SELECT outbox_pending_id FROM pay_fix);
ROLLBACK;

\echo 16. reclaimStaleProcessing (ad-hoc outbox_worker)
BEGIN;
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE payment.payment_outbox
SET status = 'PENDING', lease_until = NULL
WHERE status = 'PROCESSING'
  AND lease_until IS NOT NULL
  AND lease_until < now();
ROLLBACK;

\echo Index / table sizes
SELECT schemaname, relname, pg_size_pretty(pg_relation_size(relid)) AS heap,
       pg_size_pretty(pg_indexes_size(relid)) AS indexes
FROM pg_catalog.pg_statio_user_tables
WHERE schemaname = 'payment'
ORDER BY relname;
