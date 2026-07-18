-- Milestone 3 licensing/subscription EXPLAIN templates.
-- Executed with seed data in internal/billing/licensing_explain_test.go (TestM3ExplainQueryPlans).

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM billing.subscription_plans;

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM billing.subscription_plans WHERE code = 'basic';

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT s.*, p.display_name, p.limits_json, p.features_json, p.base_fee_micro
FROM billing.customer_subscriptions s
JOIN billing.subscription_plans p ON s.plan_code = p.code
WHERE s.customer_id = $1;

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM billing.usage_meters WHERE customer_id = $1 AND meter = 'events' AND period = $1;

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM billing.license_status LIMIT 1;

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM vendor.licenses WHERE license_key = $1;

EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM outbox_events
WHERE status = 'PENDING' AND event_type = 'UPDATE_ENTITLEMENTS'
ORDER BY created_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
