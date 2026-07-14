package management

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"espx/internal/database"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	clickhousecontainer "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func TestMLExplainQueryPlans(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping ML EXPLAIN in short mode")
	}

	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	pgQueries := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "ml_enforcement_idempotency_claim",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
INSERT INTO ml_enforcement_idempotency (ip, model_version, reason) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			args: []any{"1.2.3.4", "v1", "boost"},
		},
		{
			name: "ml_model_versions_syncing",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT id, artifact_hash FROM ml_model_versions WHERE status = 'SYNCING' LIMIT 1`,
		},
		{
			name: "ml_model_versions_by_hash",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT EXISTS(SELECT 1 FROM ml_model_versions WHERE artifact_hash = $1)`,
			args: []any{"abc123hash"},
		},
		{
			name: "ml_shard_sync_state",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT shard_id, phase, started_at FROM ml_shard_sync_state WHERE model_version = $1`,
			args: []any{"v2"},
		},
		{
			name: "campaigns_fraud_thresholds",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT id, fraud_threshold_pass, fraud_threshold_suspect, fraud_threshold_block, ghost_ivt_enabled FROM campaigns`,
		},
		{
			name: "outbox_ml_priority_lane",
			sql: `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT)
SELECT * FROM outbox_events
WHERE status = 'PENDING'
ORDER BY
  CASE event_type
    WHEN 'UPDATE_BLACKLIST' THEN 0
    WHEN 'PAUSE_CAMPAIGN' THEN 0
    WHEN 'CANCEL_CAMPAIGN' THEN 0
    WHEN 'BUDGET_FREEZE' THEN 0
    WHEN 'QUOTA_REPAIR' THEN 0
    WHEN 'ML_MODEL_VERSION' THEN 0
    ELSE 1
  END,
  created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED`,
		},
	}

	for _, q := range pgQueries {
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

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	initSQL := filepath.Join(filepath.Dir(filename), "..", "..", "deploy", "clickhouse", "init.sql")

	chContainer, err := clickhousecontainer.Run(ctx,
		"clickhouse/clickhouse-server:24.3-alpine",
		clickhousecontainer.WithInitScripts(initSQL),
		clickhousecontainer.WithDatabase("ad_event_processor"),
	)
	if err != nil {
		t.Fatalf("clickhouse container: %v", err)
	}
	defer func() { _ = chContainer.Terminate(ctx) }()

	dsn, err := chContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := chgo.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := chgo.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chDDL := `CREATE TABLE IF NOT EXISTS ad_event_processor.ml_features_1m (
		window_start DateTime,
		ip_address String,
		campaign_id UUID,
		events UInt64,
		clicks UInt64,
		spend_micro Int64,
		budget_limit_micro Int64,
		unique_users UInt64,
		unique_uas UInt64
	) ENGINE = SummingMergeTree()
	ORDER BY (window_start, ip_address, campaign_id)`
	if err := conn.Exec(ctx, chDDL); err != nil {
		t.Fatalf("create ml_features_1m: %v", err)
	}

	chQuery := `EXPLAIN indexes = 1
SELECT
    window_start,
    ip_address,
    campaign_id,
    events,
    clicks,
    spend_micro,
    budget_limit_micro,
    unique_users,
    unique_uas
FROM ad_event_processor.ml_features_1m
WHERE window_start >= now() - INTERVAL 5 MINUTE
ORDER BY window_start DESC
LIMIT 1000`

	rows, err := conn.Query(ctx, chQuery)
	if err != nil {
		t.Fatalf("clickhouse explain failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		t.Log(line)
	}
}
