package ivtdetector

import (
	"context"
	"testing"
	"time"

	"espx/internal/database"
	"espx/internal/fraudscoring"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFraudScoringRule_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	conn, cleanup := setupClickHouseTest(t)
	defer cleanup()

	ctx := context.Background()
	ensureFraudScoringShadowTables(t, conn)

	// 2. Insert test features directly into ml_features_1m
	campaignID := uuid.New()
	now := time.Now().Truncate(time.Minute)

	insertQuery := `
		INSERT INTO ad_event_processor.ml_features_1m
		(window_start, ip_address, campaign_id, events, clicks, spend_micro, budget_limit_micro, unique_users, unique_uas)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// IP 1: low clicks -> score 0.1
	err := conn.Exec(ctx, insertQuery, now, "1.2.3.4", campaignID, uint64(10), uint64(2), int64(1000000), int64(5000000), uint64(1), uint64(1))
	require.NoError(t, err)

	// IP 2: high clicks -> score 0.9
	err = conn.Exec(ctx, insertQuery, now, "5.6.7.8", campaignID, uint64(100), uint64(10), int64(10000000), int64(50000000), uint64(5), uint64(2))
	require.NoError(t, err)

	// 3. Load the model and create the ML rule
	scorer, err := fraudscoring.NewLGBMScorer("../fraudscoring/testdata/model.txt")
	require.NoError(t, err)

	rule := NewFraudScoringRule(conn, nil, scorer, 100)
	assert.Equal(t, "fraud_scoring_shadow", rule.Name())

	// 4. Run the ML rule
	candidates, err := rule.Find(ctx)
	require.NoError(t, err)
	assert.Len(t, candidates, 1)
	assert.Equal(t, "1.2.3.4", candidates[0].IP)
	assert.Equal(t, "boost", candidates[0].Action)
	assert.Equal(t, int32(52), candidates[0].Boost)

	// 5. Verify shadow scores were written to ClickHouse
	var count uint64
	err = conn.QueryRow(ctx, "SELECT count() FROM ad_event_processor.ml_shadow_scores").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), count)

	// Verify specific scores
	var score1, score2 float64
	err = conn.QueryRow(ctx, "SELECT score FROM ad_event_processor.ml_shadow_scores WHERE ip_address = '1.2.3.4' LIMIT 1").Scan(&score1)
	require.NoError(t, err)
	assert.InDelta(t, 0.52497, score1, 1e-4)

	err = conn.QueryRow(ctx, "SELECT score FROM ad_event_processor.ml_shadow_scores WHERE ip_address = '5.6.7.8' LIMIT 1").Scan(&score2)
	require.NoError(t, err)
	assert.InDelta(t, 0.71094, score2, 1e-4)

	assert.Greater(t, score2, score1, "fraud-like IP must score higher than control IP")
}

func TestFraudScoringRule_FraudScoresHigherThanControl(t *testing.T) {
	if testing.Short() {
		t.Skip("clickhouse integration test")
	}

	conn, cleanup := setupClickHouseTest(t)
	defer cleanup()

	ctx := context.Background()
	ensureFraudScoringShadowTables(t, conn)

	campaignID := uuid.New()
	now := time.Now().Truncate(time.Minute)
	insertQuery := `
		INSERT INTO ad_event_processor.ml_features_1m
		(window_start, ip_address, campaign_id, events, clicks, spend_micro, budget_limit_micro, unique_users, unique_uas)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	controlIP := "203.0.113.10"
	fraudIP := "203.0.113.20"

	require.NoError(t, conn.Exec(ctx, insertQuery, now, controlIP, campaignID, uint64(20), uint64(1), int64(1000000), int64(5000000), uint64(1), uint64(1)))
	require.NoError(t, conn.Exec(ctx, insertQuery, now, fraudIP, campaignID, uint64(200), uint64(50), int64(10000000), int64(50000000), uint64(20), uint64(1)))

	scorer, err := fraudscoring.NewLGBMScorer("../fraudscoring/testdata/model.txt")
	require.NoError(t, err)

	_, err = NewFraudScoringRule(conn, nil, scorer, 100).Find(ctx)
	require.NoError(t, err)

	var controlScore, fraudScore float64
	require.NoError(t, conn.QueryRow(ctx, "SELECT score FROM ad_event_processor.ml_shadow_scores WHERE ip_address = ? LIMIT 1", controlIP).Scan(&controlScore))
	require.NoError(t, conn.QueryRow(ctx, "SELECT score FROM ad_event_processor.ml_shadow_scores WHERE ip_address = ? LIMIT 1", fraudIP).Scan(&fraudScore))

	assert.Greater(t, fraudScore, controlScore, "seeded fraud IP should outrank control IP")
}

type mockScorer struct {
	scores []float64
}

func (m *mockScorer) Name() string {
	return "mock-scorer"
}

func (m *mockScorer) ScoreBatch(ctx context.Context, rows []fraudscoring.FeatureRow) ([]float64, error) {
	return m.scores, nil
}

func (m *mockScorer) Dims() int {
	return 8
}

func TestFraudScoringRule_WithCampaignThresholds(t *testing.T) {
	if testing.Short() {
		t.Skip("clickhouse integration test")
	}

	conn, cleanupCH := setupClickHouseTest(t)
	defer cleanupCH()

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	ctx := context.Background()
	ensureFraudScoringShadowTables(t, conn)

	// 1. Create a campaign with custom thresholds in Postgres
	campaignID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO campaigns (id, name, status, budget_limit, fraud_threshold_pass, fraud_threshold_suspect, fraud_threshold_block, ghost_ivt_enabled)
		VALUES ($1, 'Test Campaign', 'ACTIVE', 1000000000, 20, 50, 90, true)
	`, campaignID)
	require.NoError(t, err)

	// 2. Insert test features into ClickHouse
	now := time.Now().Truncate(time.Minute)
	insertQuery := `
		INSERT INTO ad_event_processor.ml_features_1m
		(window_start, ip_address, campaign_id, events, clicks, spend_micro, budget_limit_micro, unique_users, unique_uas)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// IP 1: low clicks -> score 0.1 (mapped fraudScore = 10 < pass 20) -> no action
	require.NoError(t, conn.Exec(ctx, insertQuery, now, "1.1.1.1", campaignID, uint64(10), uint64(2), int64(1000000), int64(5000000), uint64(1), uint64(1)))

	// IP 2: medium clicks -> score 0.4 (mapped fraudScore = 40, in [pass 20, suspect 50)) -> boost action
	require.NoError(t, conn.Exec(ctx, insertQuery, now, "2.2.2.2", campaignID, uint64(50), uint64(5), int64(5000000), int64(10000000), uint64(3), uint64(1)))

	// IP 3: high clicks -> score 0.75 (mapped fraudScore = 75, in [suspect 50, block 90)) -> ghost action (ghostEnabled = true)
	require.NoError(t, conn.Exec(ctx, insertQuery, now, "3.3.3.3", campaignID, uint64(100), uint64(10), int64(10000000), int64(20000000), uint64(5), uint64(2)))

	// IP 4: extreme clicks -> score 0.95 (mapped fraudScore = 95, >= block 90) -> blacklist action
	require.NoError(t, conn.Exec(ctx, insertQuery, now, "4.4.4.4", campaignID, uint64(200), uint64(30), int64(20000000), int64(30000000), uint64(10), uint64(3)))

	// 3. Create the mock scorer and ML rule
	scorer := &mockScorer{
		scores: []float64{0.1, 0.4, 0.75, 0.95},
	}

	rule := NewFraudScoringRule(conn, pool, scorer, 100)

	// 4. Find candidates
	candidates, err := rule.Find(ctx)
	require.NoError(t, err)

	// Since ClickHouse might return rows in any order, let's map IP to candidate
	candidateMap := make(map[string]SuspiciousIP)
	for _, c := range candidates {
		candidateMap[c.IP] = c
	}

	// IP 4 (4.4.4.4) should have no candidate (score 0.1 -> fraudScore 10 < pass 20)
	_, exists := candidateMap["4.4.4.4"]
	assert.False(t, exists)

	// IP 3 (3.3.3.3) should be "boost" (score 0.4 -> fraudScore 40, in [20, 50))
	c3, exists := candidateMap["3.3.3.3"]
	require.True(t, exists)
	assert.Equal(t, "boost", c3.Action)
	assert.Equal(t, int32(40), c3.Boost)

	// IP 2 (2.2.2.2) should be "ghost" (score 0.75 -> fraudScore 75, in [50, 90))
	c2, exists := candidateMap["2.2.2.2"]
	require.True(t, exists)
	assert.Equal(t, "ghost", c2.Action)

	// IP 1 (1.1.1.1) should be "blacklist" (score 0.95 -> fraudScore 95, >= block 90)
	c1, exists := candidateMap["1.1.1.1"]
	require.True(t, exists)
	assert.Equal(t, "blacklist", c1.Action)
}
