package marginguard

import (
	"context"
	"testing"

	"espx/internal/database"

	"github.com/google/uuid"
)

func TestMarginGuardExplainQueryPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping EXPLAIN in short mode")
	}

	ctx := context.Background()
	pool, cleanup := database.SetupTestDB(t)
	defer cleanup()

	// Apply M17 migration
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS margin_guard_policies (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			campaign_id UUID NOT NULL,
			name TEXT NOT NULL,
			min_clicks INT NOT NULL DEFAULT 50,
			roi_floor_pct FLOAT NOT NULL DEFAULT -30.0,
			zero_conv_streak INT NOT NULL DEFAULT 100,
			is_active BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS idx_margin_guard_policies_campaign_id ON margin_guard_policies(campaign_id);
		CREATE TABLE IF NOT EXISTS margin_guard_activity (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			policy_id UUID NOT NULL,
			campaign_id UUID NOT NULL,
			placement_id TEXT NOT NULL,
			action TEXT NOT NULL,
			reason TEXT NOT NULL,
			metrics JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS idx_margin_guard_activity_campaign_id ON margin_guard_activity(campaign_id);
		CREATE INDEX IF NOT EXISTS idx_margin_guard_activity_created_at ON margin_guard_activity(created_at);
	`)
	if err != nil {
		t.Fatalf("ddl: %v", err)
	}

	campID := uuid.New()
	policyID := uuid.New()
	placementID := "zone-42"

	queries := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "fetch_active_policies",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT id, campaign_id, name, min_clicks, roi_floor_pct, zero_conv_streak, is_active
FROM margin_guard_policies WHERE is_active = true`,
		},
		{
			name: "dedupe_pause_exists",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT EXISTS(
  SELECT 1 FROM margin_guard_activity
  WHERE campaign_id = $1 AND placement_id = $2 AND action = 'pause'
    AND created_at > now() - INTERVAL '1 day'
)`,
			args: []any{campID, placementID},
		},
		{
			name: "insert_activity",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO margin_guard_activity (policy_id, campaign_id, placement_id, action, reason, metrics)
VALUES ($1, $2, $3, 'pause', 'test', '{"roi_pct": -40}'::jsonb)`,
			args: []any{policyID, campID, placementID},
		},
		{
			name: "list_activity_by_campaign",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT id, policy_id, campaign_id, placement_id, action, reason, metrics, created_at
FROM margin_guard_activity
WHERE campaign_id = $1
ORDER BY created_at DESC
LIMIT 100`,
			args: []any{campID},
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rows, err := pool.Query(ctx, q.sql, q.args...)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()
			for rows.Next() {
				var plan string
				if err := rows.Scan(&plan); err != nil {
					t.Fatal(err)
				}
				t.Log(plan)
			}
		})
	}
}
