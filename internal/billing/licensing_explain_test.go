package billing

import (
	"context"
	"testing"
	"time"

	billingdb "espx/internal/billing/db"
	"espx/internal/ingestion"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestM3ExplainQueryPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping M3 EXPLAIN in short mode")
	}

	ctx := context.Background()
	pool, cleanup := setupBillingTestDB(t)
	defer cleanup()

	customerID := seedCustomerOnly(t, pool)
	depID := uuid.New()
	licID := uuid.New()
	monthStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	_, err := pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start)
		VALUES ($1, 'basic', 'active', $2)
	`, customerID, monthStart)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO billing.usage_meters (customer_id, meter, period, value)
		VALUES ($1, 'events', $2, 12000)
	`, customerID, monthStart)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO billing.usage_daily (customer_id, usage_date, meter, value)
		VALUES ($1, $2, 'ingress', 500)
	`, customerID, monthStart)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO billing.license_status (
			deployment_id, license_id, plan_code, valid_until, state, entitlements_json, last_verified_at
		) VALUES ($1, $2, 'growth', NOW() + interval '30 days', 'ACTIVE', '{"limits":{},"features":{}}', NOW())
	`, depID, licID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO vendor.licenses (
			license_key, customer_name, plan_code, valid_until, limits_json, features_json
		) VALUES ('lic-m3-explain', 'Explain Buyer', 'growth', NOW() + interval '30 days', '{}', '{}')
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO vendor.deployments (deployment_id, license_key, fingerprint)
		VALUES ($1, 'lic-m3-explain', 'fp-explain')
	`, depID)
	require.NoError(t, err)

	pgQueries := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "list_subscription_plans",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.subscription_plans`,
		},
		{
			name: "get_subscription_plan",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.subscription_plans WHERE code = $1`,
			args: []any{"basic"},
		},
		{
			name: "get_customer_subscription",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT s.*, p.display_name, p.limits_json, p.features_json, p.base_fee_micro
FROM billing.customer_subscriptions s
JOIN billing.subscription_plans p ON s.plan_code = p.code
WHERE s.customer_id = $1`,
			args: []any{customerID},
		},
		{
			name: "upsert_customer_subscription",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start, period_end, overrides_json, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (customer_id) DO UPDATE SET
  plan_code = EXCLUDED.plan_code,
  status = EXCLUDED.status,
  period_start = EXCLUDED.period_start,
  period_end = EXCLUDED.period_end,
  overrides_json = EXCLUDED.overrides_json,
  updated_at = NOW()
RETURNING *`,
			args: []any{customerID, "pro", "active", monthStart, nil, []byte(`{}`)},
		},
		{
			name: "get_usage_meter",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.usage_meters WHERE customer_id = $1 AND meter = $2 AND period = $3`,
			args: []any{customerID, "events", monthStart},
		},
		{
			name: "list_usage_meters",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.usage_meters WHERE customer_id = $1 AND period = $2`,
			args: []any{customerID, monthStart},
		},
		{
			name: "increment_usage_meter",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO billing.usage_meters (customer_id, meter, period, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (customer_id, meter, period) DO UPDATE
SET value = billing.usage_meters.value + EXCLUDED.value
RETURNING *`,
			args: []any{customerID, "events", monthStart, int64(1)},
		},
		{
			name: "increment_usage_daily",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO billing.usage_daily (customer_id, usage_date, meter, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (customer_id, usage_date, meter) DO UPDATE
SET value = billing.usage_daily.value + EXCLUDED.value
RETURNING *`,
			args: []any{customerID, monthStart, "ingress", int64(1)},
		},
		{
			name: "get_usage_daily",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.usage_daily WHERE customer_id = $1 AND usage_date = $2 AND meter = $3`,
			args: []any{customerID, monthStart, "ingress"},
		},
		{
			name: "list_usage_daily",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.usage_daily WHERE customer_id = $1 AND usage_date >= $2 AND usage_date <= $3`,
			args: []any{customerID, monthStart, monthStart.AddDate(0, 0, 30)},
		},
		{
			name: "get_license_status",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM billing.license_status LIMIT 1`,
		},
		{
			name: "upsert_license_status",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO billing.license_status (deployment_id, license_id, plan_code, valid_until, state, entitlements_json, last_verified_at, last_refresh_error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (deployment_id) DO UPDATE SET
  license_id = EXCLUDED.license_id,
  plan_code = EXCLUDED.plan_code,
  valid_until = EXCLUDED.valid_until,
  state = EXCLUDED.state,
  entitlements_json = EXCLUDED.entitlements_json,
  last_verified_at = EXCLUDED.last_verified_at,
  last_refresh_error = EXCLUDED.last_refresh_error
RETURNING *`,
			args: []any{
				depID,
				licID,
				"growth",
				time.Now().Add(24 * time.Hour),
				"ACTIVE",
				[]byte(`{"limits":{},"features":{}}`),
				time.Now(),
				nil,
			},
		},
		{
			name: "get_vendor_license",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM vendor.licenses WHERE license_key = $1`,
			args: []any{"lic-m3-explain"},
		},
		{
			name: "get_vendor_deployment",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM vendor.deployments WHERE deployment_id = $1`,
			args: []any{depID},
		},
		{
			name: "outbox_update_entitlements_lane",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM outbox_events
WHERE status = 'PENDING' AND event_type = 'UPDATE_ENTITLEMENTS'
ORDER BY created_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED`,
		},
	}

	for _, q := range pgQueries {
		t.Run(q.name, func(t *testing.T) {
			rows, err := pool.Query(ctx, q.sql, q.args...)
			require.NoError(t, err)
			defer rows.Close()
			for rows.Next() {
				var plan string
				require.NoError(t, rows.Scan(&plan))
				t.Log(plan)
			}
		})
	}

	// Sanity: generated sqlc path still works with seeded row (separate customer untouched by EXPLAIN DML).
	sanityID := seedCustomerOnly(t, pool)
	_, err = pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start)
		VALUES ($1, 'basic', 'active', $2)
	`, sanityID, monthStart)
	require.NoError(t, err)

	q := billingdb.New(pool)
	sub, err := q.GetCustomerSubscription(ctx, ingestion.ToUUID(sanityID))
	require.NoError(t, err)
	require.Equal(t, "basic", sub.PlanCode)
}
