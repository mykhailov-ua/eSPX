package database

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestExplainAudit_AllApplicationQueries runs EXPLAIN (ANALYZE, BUFFERS) on seeded
// production-shaped data and reports suboptimal plans.
func TestExplainAudit_AllApplicationQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full EXPLAIN audit in short mode")
	}
	if os.Getenv("EXPLAIN_AUDIT") == "" {
		t.Skip("set EXPLAIN_AUDIT=1 to run full query plan audit (slow, needs Docker)")
	}

	ctx := context.Background()
	pool, cleanup := SetupTestDB(t)
	defer cleanup()

	seedExplainAuditData(t, ctx, pool)

	type queryCase struct {
		name    string
		sql     string
		args    []any
		hotPath bool
	}

	campID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	custID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	hash := "explain-audit-hash-0001"

	queries := []queryCase{
		// --- Hot path / processor ---
		{name: "budget.GetCampaignBudget", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.id, c.customer_id, c.budget_limit, c.current_spend, c.status, cust.balance AS customer_balance
FROM campaigns c JOIN customers cust ON c.customer_id = cust.id WHERE c.id = $1 LIMIT 1`, args: []any{campID}},
		{name: "sync.InsertSyncIdempotency", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING`, args: []any{"explain-audit-sync-tx-0001"}},
		{name: "sync.DecreaseCampaignQuotaReserved", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE campaign_quotas SET reserved_amount = GREATEST(0, reserved_amount - $3), updated_at = NOW()
WHERE shard_id = $1 AND campaign_id = $2`, args: []any{int16(0), campID, int64(1000)}},
		{name: "budget.UpdateCampaignSpend", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
UPDATE campaigns SET current_spend = current_spend + $2, updated_at = NOW() WHERE id = $1`, args: []any{campID, int64(1000)}},
		{name: "budget.ListActiveCampaigns", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, customer_id, budget_limit, current_spend, status FROM campaigns WHERE status = 'ACTIVE' AND deleted_at IS NULL`},
		{name: "management.GetCampaignFull", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.*, sa.primary_a_shard, sa.primary_b_shard, sa.reserve_shard, sa.h_ema, sa.c_ema
FROM campaigns c LEFT JOIN campaign_shard_assignment sa ON c.id = sa.campaign_id WHERE c.id = $1`, args: []any{campID}},
		{name: "management.GetCustomerForUpdate", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM customers WHERE id = $1 FOR UPDATE`, args: []any{custID}},
		{name: "management.CreateLedgerEntry", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, idempotency_hash)
VALUES ($1, $2, $3, 'FEE', $4) RETURNING id`, args: []any{custID, campID, int64(500), hash + "-new"}},
		{name: "management.GetLedgerByHash", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM balance_ledger WHERE idempotency_hash = $1`, args: []any{hash}},
		{name: "management.GetPendingOutboxEventsForUpdate", hotPath: true, sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM outbox_events WHERE status = 'PENDING'
ORDER BY CASE event_type WHEN 'UPDATE_BLACKLIST' THEN 0 WHEN 'PAUSE_CAMPAIGN' THEN 0 ELSE 1 END, created_at ASC
LIMIT 100 FOR UPDATE SKIP LOCKED`},
		{name: "management.GetDrainingCampaignsForUpdate", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM campaigns WHERE status = 'DRAINING' AND updated_at < $1 ORDER BY updated_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`,
			args: []any{time.Now()}},
		{name: "management.ListCustomersForScoring", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.id, COALESCE(FLOOR(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - c.created_at)) / 86400), 0)::integer AS age_days,
COALESCE(SUM(l.amount), 0)::bigint AS topup_sum_30d
FROM customers c LEFT JOIN balance_ledger l ON l.customer_id = c.id AND (l.type = 'TOPUP' OR l.type = 'PAYMENT_TOPUP') AND l.created_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY c.id`},
		{name: "management.GetCampaignsWithStats", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.id, c.name, c.status, c.budget_limit, c.created_at, c.updated_at, c.customer_id, c.current_spend, c.deleted_at, c.pacing_mode, c.daily_budget, c.timezone, c.freq_limit, c.freq_window, c.target_countries, c.brand_id, c.brand_fcap_key,
COALESCE(SUM(s.impressions_count), 0)::bigint, COALESCE(SUM(s.clicks_count), 0)::bigint, COALESCE(SUM(s.conversions_count), 0)::bigint
FROM campaigns c LEFT JOIN campaign_stats s ON c.id = s.campaign_id
WHERE c.customer_id = $1 AND c.status = 'ACTIVE' GROUP BY c.id`, args: []any{custID}},
		{name: "management.GetAllActiveCampaignsWithStats", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.id, c.name, c.status, c.budget_limit, c.created_at, c.updated_at, c.customer_id, c.current_spend, c.deleted_at, c.pacing_mode, c.daily_budget, c.timezone, c.freq_limit, c.freq_window, c.target_countries, c.brand_id, c.brand_fcap_key,
COALESCE(SUM(s.impressions_count), 0)::bigint, COALESCE(SUM(s.clicks_count), 0)::bigint, COALESCE(SUM(s.conversions_count), 0)::bigint
FROM campaigns c LEFT JOIN campaign_stats s ON c.id = s.campaign_id WHERE c.status = 'ACTIVE' GROUP BY c.id`},
		{name: "partial.idx_ledger_fee_created", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT campaign_id, COALESCE(SUM(ABS(amount)), 0)::bigint FROM balance_ledger
WHERE type = 'FEE' AND created_at >= $1 AND created_at < $2 GROUP BY campaign_id`,
			args: []any{time.Now().Add(-24 * time.Hour), time.Now()}},
		{name: "partial.idx_ledger_topup_recent", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT customer_id, amount, created_at FROM balance_ledger
WHERE type IN ('TOPUP', 'PAYMENT_TOPUP') AND customer_id = $1
ORDER BY created_at DESC LIMIT 50`, args: []any{custID}},
		{name: "partial.idx_campaigns_draining_updated", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM campaigns WHERE status = 'DRAINING' AND updated_at < $1 ORDER BY updated_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`,
			args: []any{time.Now()}},
		{name: "management.ListAuditPaginated", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM admin_audit_log ORDER BY created_at DESC LIMIT 50 OFFSET 0`},
		{name: "management.CountAuditLogs", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT COUNT(*) FROM admin_audit_log`},
		{name: "management.ListCustomerLedger", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM balance_ledger WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 50 OFFSET 0`, args: []any{custID}},
		{name: "management.ListCampaigns", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM campaigns WHERE customer_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 50 OFFSET 0`, args: []any{custID}},
		{name: "management.GetAllBlacklist", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT ip, reason FROM ip_blacklist`},
		{name: "management.SumCampaignStatsInRange", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COALESCE(SUM(impressions_count),0)::bigint, COALESCE(SUM(clicks_count),0)::bigint, COALESCE(SUM(conversions_count),0)::bigint
FROM campaign_stats WHERE campaign_id = $1 AND date >= $2::date AND date < $3::date`,
			args: []any{campID, time.Now().Add(-7 * 24 * time.Hour), time.Now()}},
		{name: "events.UpdateCampaignStatsBatch", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT val.campaign_id, CURRENT_DATE, val.impression, val.click, val.conversion
FROM (SELECT unnest($1::uuid[]) AS campaign_id, unnest($2::bigint[]) AS impression, unnest($3::bigint[]) AS click, unnest($4::bigint[]) AS conversion) val
ORDER BY val.campaign_id
ON CONFLICT (campaign_id, date) DO UPDATE SET impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count`,
			args: []any{[]uuid.UUID{campID}, []int64{10}, []int64{1}, []int64{0}}},
		{name: "cost_sync.InsertCampaignCost", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
INSERT INTO campaign_costs (customer_id, campaign_id, cost_date, network, placement_id, amount_micro, currency, line_type)
VALUES ($1,$2,$3,'facebook','zone-1',1000000,'USD','spend') ON CONFLICT DO NOTHING`,
			args: []any{custID, campID, time.Now().UTC().Truncate(24 * time.Hour)}},
		{name: "cost_sync.SumCampaignCostsUSDForDate", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COALESCE(SUM(amount_micro),0)::bigint FROM campaign_costs WHERE customer_id = $1 AND cost_date = $2`,
			args: []any{custID, time.Now().UTC().Truncate(24 * time.Hour)}},
		{name: "postback.GetPendingPostbackEventsForUpdate", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM outbox_events WHERE status = 'PENDING' AND event_type = 'SEND_POSTBACK' ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED`},
		{name: "inline.volume_meter_campaigns", sql: `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT id, customer_id FROM campaigns`},
	}

	var allFindings []ExplainFinding
	var summaries []string

	for _, qc := range queries {
		rows, err := pool.Query(ctx, qc.sql, qc.args...)
		require.NoError(t, err, qc.name)
		raw, err := collectExplainText(rows)
		rows.Close()
		require.NoError(t, err, qc.name)

		plan := ParseExplainPlan(raw)
		findings := AnalyzeExplainPlan(qc.name, plan, qc.hotPath, 500)
		allFindings = append(allFindings, findings...)

		summaries = append(summaries, fmt.Sprintf("%s: plan=%.2fms exec, %d nodes, %d findings",
			qc.name, plan.ExecutionTimeMS, len(plan.Nodes), len(findings)))
		t.Logf("=== %s ===\n%s", qc.name, raw)
	}

	t.Log("--- EXPLAIN AUDIT SUMMARY ---")
	for _, s := range summaries {
		t.Log(s)
	}

	warnCount := 0
	for _, f := range allFindings {
		if f.Severity == "warn" {
			warnCount++
			t.Logf("WARN [%s] %s", f.Query, f.Message)
		}
	}
	t.Logf("Total findings: %d (%d warnings, %d info)", len(allFindings), warnCount, len(allFindings)-warnCount)
	require.Equal(t, 0, warnCount, "EXPLAIN audit must have zero warn-level findings at 50k ledger scale (M-DB-PG-6)")

	dead, err := QueryPgTableDeadTuples(ctx, pool)
	require.NoError(t, err)
	for _, table := range WatchedPgTables {
		require.Contains(t, dead, table, "pg_stat_user_tables must track %s", table)
	}
	t.Logf("pg_stat dead tuples after seed: %v", dead)
}

func seedExplainAuditData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "duplicate") {
				t.Fatalf("seed: %v\nsql: %s", err, sql)
			}
		}
	}

	// Extra schemas/tables from later milestones
	exec(`CREATE TABLE IF NOT EXISTS campaign_costs (
		id BIGSERIAL PRIMARY KEY,
		customer_id UUID NOT NULL,
		campaign_id UUID NOT NULL,
		cost_date DATE NOT NULL,
		network TEXT NOT NULL,
		placement_id TEXT NOT NULL DEFAULT '',
		amount_micro BIGINT NOT NULL,
		currency TEXT NOT NULL DEFAULT 'USD',
		line_type TEXT NOT NULL DEFAULT 'spend',
		UNIQUE (customer_id, campaign_id, cost_date, network, placement_id)
	)`)
	exec(`CREATE TABLE IF NOT EXISTS campaign_shard_assignment (
		campaign_id UUID PRIMARY KEY,
		primary_a_shard SMALLINT NOT NULL DEFAULT 0,
		primary_b_shard SMALLINT NOT NULL DEFAULT 1,
		reserve_shard SMALLINT NOT NULL DEFAULT 2,
		h_ema DOUBLE PRECISION NOT NULL DEFAULT 0,
		c_ema DOUBLE PRECISION NOT NULL DEFAULT 0
	)`)

	exec(`INSERT INTO customers (id, name, balance, currency)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid, 'cust-' || g, (g % 100) * 1000000, 'USD'
FROM generate_series(1, 500) g ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO campaigns (id, name, status, budget_limit, current_spend, customer_id, pacing_mode, timezone)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
  'camp-' || g,
  (ARRAY['ACTIVE','PAUSED','DRAINING']::campaign_status_type[])[1 + (g % 3)],
  100000000 + (g % 50) * 1000000,
  (g % 30) * 1000000,
  ('00000000-0000-4000-8000-' || lpad(to_hex(1 + (g % 500)), 12, '0'))::uuid,
  'ASAP'::pacing_mode_type, 'UTC'
FROM generate_series(1, 5000) g ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, idempotency_hash)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(1 + (g % 500)), 12, '0'))::uuid,
  ('00000000-0000-4000-8000-' || lpad(to_hex(1 + (g % 5000)), 12, '0'))::uuid,
  (g % 10) * 100000,
  (ARRAY['FEE','TOPUP','PAYMENT_TOPUP','RELEASE']::ledger_type[])[1 + (g % 4)],
  'explain-hash-' || g
FROM generate_series(1, 50000) g ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO outbox_events (event_type, payload, status)
SELECT (ARRAY['UPDATE_BLACKLIST','PAUSE_CAMPAIGN','CREATE_CAMPAIGN','SEND_POSTBACK','UPDATE_CAMPAIGN_PACING'])[1 + (g % 5)],
  '{"campaign_id":"00000000-0000-4000-8000-000000000001"}'::jsonb,
  (ARRAY['PENDING','PROCESSED','PENDING','PENDING','PROCESSED'])[1 + (g % 5)]
FROM generate_series(1, 10000) g`)

	exec(`INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(1 + (g % 5000)), 12, '0'))::uuid,
  CURRENT_DATE - ((g % 30) || ' days')::interval,
  g % 1000, g % 100, g % 10
FROM generate_series(1, 50000) g ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO admin_audit_log (admin_id, action, target_type, target_id, changes, metadata)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(g % 20), 12, '0'))::uuid,
  'ACTION_' || (g % 10), 'campaign', ('00000000-0000-4000-8000-' || lpad(to_hex(g % 5000), 12, '0'))::uuid,
  '{}'::jsonb, '{}'::jsonb
FROM generate_series(1, 10000) g`)

	exec(`INSERT INTO ip_blacklist (ip, reason, expires_at)
SELECT ('203.0.113.' || (g % 255))::inet, 'audit', NOW() + interval '1 day'
FROM generate_series(1, 200) g ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO campaign_shard_assignment (campaign_id, primary_a_shard, primary_b_shard, reserve_shard)
SELECT id, 0, 1, 2 FROM campaigns WHERE id = '00000000-0000-4000-8000-000000000001'::uuid
ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size)
VALUES (0, '00000000-0000-4000-8000-000000000001'::uuid, 5000000, 1000000)
ON CONFLICT DO NOTHING`)

	exec(`INSERT INTO campaigns (id, name, status, budget_limit, current_spend, customer_id, pacing_mode, timezone, updated_at)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(9000 + g), 12, '0'))::uuid,
  'drain-' || g, 'DRAINING', 100000000, 0,
  ('00000000-0000-4000-8000-' || lpad(to_hex(1 + (g % 500)), 12, '0'))::uuid,
  'ASAP'::pacing_mode_type, 'UTC', NOW() - ((g % 60) || ' minutes')::interval
FROM generate_series(1, 200) g ON CONFLICT DO NOTHING`)

	exec(`ANALYZE customers`)
	exec(`ANALYZE campaigns`)
	exec(`ANALYZE balance_ledger`)
	exec(`ANALYZE outbox_events`)
	exec(`ANALYZE campaign_stats`)
	exec(`ANALYZE admin_audit_log`)
	exec(`ANALYZE ip_blacklist`)
}
