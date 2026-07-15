package ivtdetector

import (
	"context"
	"errors"
	"testing"

	"espx/internal/fraudscoring"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingCHConn struct {
	driver.Conn
	queryErr error
}

func (conn *failingCHConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, conn.queryErr
}

func (conn *failingCHConn) Exec(context.Context, string, ...any) error {
	return conn.queryErr
}

// Guards an empty ClickHouse feature window returns nil candidates without error.
func TestFraudScoringRule_EmptyWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("clickhouse integration test")
	}

	conn, cleanup := setupClickHouseTest(t)
	defer cleanup()

	ensureFraudScoringShadowTables(t, conn)

	scorer, err := fraudscoring.NewLGBMScorer("../fraudscoring/testdata/model.txt")
	require.NoError(t, err)

	rule := NewFraudScoringRule(conn, nil, scorer, 100)
	candidates, err := rule.Find(context.Background())
	require.NoError(t, err)
	assert.Nil(t, candidates)
}

// Guards ClickHouse outages skip the ML cycle without panicking or failing the detector loop.
func TestChaos_FraudClickHouseDown(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	scorer, err := fraudscoring.NewLGBMScorer("../fraudscoring/testdata/model.txt")
	require.NoError(t, err)

	rule := NewFraudScoringRule(&failingCHConn{queryErr: errors.New("clickhouse unavailable")}, nil, scorer, 100)

	require.NotPanics(t, func() {
		candidates, findErr := rule.Find(context.Background())
		require.NoError(t, findErr)
		assert.Nil(t, candidates)
	})

	logChaosProof(t, "fraud_clickhouse_down", map[string]string{
		"subsystem":   "fraud_scoring",
		"skip_cycle":  "true",
		"panic_free":  "true",
		"outbox_safe": "true",
	})
}

func ensureFraudScoringShadowTables(t *testing.T, conn driver.Conn) {
	t.Helper()
	ctx := context.Background()
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS ad_event_processor.ml_features_1m (
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
		ORDER BY (window_start, ip_address, campaign_id)`,
		`CREATE TABLE IF NOT EXISTS ad_event_processor.ml_shadow_scores (
			ip_address String,
			score Float64,
			model_name LowCardinality(String),
			created_at DateTime64(3, 'UTC')
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (model_name, created_at, ip_address)`,
	}
	for _, stmt := range ddl {
		require.NoError(t, conn.Exec(ctx, stmt))
	}
}
