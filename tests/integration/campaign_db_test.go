package integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"espx/internal/ads/db"

	"espx/internal/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCampaignQueries ensures ListCampaignIDs returns only ACTIVE campaigns;
// the registry hot-reload depends on this filter to avoid serving paused spend.
func TestIntegration_CampaignQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := testutil.SetupAdsPostgres(t)
	defer cleanup()

	ctx := context.Background()
	queries := db.New(pool)

	campaignID := uuid.New()
	_, err := pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)",
		campaignID, "Test Campaign", "ACTIVE")
	require.NoError(t, err)

	ids, err := queries.ListCampaignIDs(ctx)
	require.NoError(t, err)

	found := false
	for _, id := range ids {
		if id.Bytes == campaignID {
			found = true
			break
		}
	}
	assert.True(t, found, "Active campaign ID should be in the list")

	inactiveID := uuid.New()
	_, err = pool.Exec(ctx,
		"INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)",
		inactiveID, "Inactive Campaign", "PAUSED")
	require.NoError(t, err)

	ids, err = queries.ListCampaignIDs(ctx)
	require.NoError(t, err)
	for _, id := range ids {
		assert.NotEqual(t, inactiveID, id.Bytes, "Inactive campaign should not be listed")
	}
}

// TestStatsBatching verifies that repeated batch UPSERTs accumulate counters
// correctly; billing and pacing dashboards rely on this aggregation semantics.
func TestIntegration_StatsBatching(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := testutil.SetupAdsPostgres(t)
	defer cleanup()

	ctx := context.Background()
	queries := db.New(pool)

	campaignID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "Stats Test", "ACTIVE")
	require.NoError(t, err)

	err = queries.UpdateCampaignStatsBatch(ctx, db.UpdateCampaignStatsBatchParams{
		CampaignIds: []pgtype.UUID{{Bytes: campaignID, Valid: true}},
		Impressions: []int64{10},
		Clicks:      []int64{5},
		Conversions: []int64{1},
	})
	require.NoError(t, err)

	err = queries.UpdateCampaignStatsBatch(ctx, db.UpdateCampaignStatsBatchParams{
		CampaignIds: []pgtype.UUID{{Bytes: campaignID, Valid: true}},
		Impressions: []int64{20},
		Clicks:      []int64{2},
		Conversions: []int64{0},
	})
	require.NoError(t, err)

	var imps, clicks, convs int64
	err = pool.QueryRow(ctx,
		"SELECT impressions_count, clicks_count, conversions_count FROM campaign_stats WHERE campaign_id = $1 AND date = CURRENT_DATE",
		campaignID).Scan(&imps, &clicks, &convs)

	require.NoError(t, err)
	assert.Equal(t, int64(30), imps)
	assert.Equal(t, int64(7), clicks)
	assert.Equal(t, int64(1), convs)
}

// TestInvalidEventType confirms the DB rejects unknown event types so corrupt
// ingest cannot pollute analytics partitions downstream of the stream consumer.
func TestIntegration_InvalidEventType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := testutil.SetupAdsPostgres(t)
	defer cleanup()

	ctx := context.Background()

	campaignID := uuid.New()
	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)", campaignID, "Constraint Test", "ACTIVE")
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		"INSERT INTO events (click_id, campaign_id, event_type, payload, created_date) VALUES ($1, $2, $3, $4, CURRENT_DATE)",
		uuid.New().String(), campaignID, "invalid_type", "{}")

	assert.Error(t, err)
}

// TestExplainQueries is a manual regression probe for the stats batch UPSERT
// plan; run locally when changing indexes or the unnest-based batch query.
func TestIntegration_ExplainQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := testutil.SetupAdsPostgres(t)
	defer cleanup()

	ctx := context.Background()

	explainUpdateBatch := `
EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
INSERT INTO campaign_stats (campaign_id, date, impressions_count, clicks_count, conversions_count)
SELECT 
    val.campaign_id,
    CURRENT_DATE,
    val.impression,
    val.click,
    val.conversion
FROM (
    SELECT 
        unnest($1::uuid[]) as campaign_id,
        unnest($2::bigint[]) as impression,
        unnest($3::bigint[]) as click,
        unnest($4::bigint[]) as conversion
) val
ORDER BY val.campaign_id
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions_count = campaign_stats.impressions_count + EXCLUDED.impressions_count,
    clicks_count = campaign_stats.clicks_count + EXCLUDED.clicks_count,
    conversions_count = campaign_stats.conversions_count + EXCLUDED.conversions_count;
`

	u1 := uuid.New()
	u2 := uuid.New()

	_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3), ($4, $5, $6)",
		u1, "Camp 1", "ACTIVE", u2, "Camp 2", "ACTIVE")
	require.NoError(t, err)

	rows, err := pool.Query(ctx, explainUpdateBatch,
		[]pgtype.UUID{{Bytes: u1, Valid: true}, {Bytes: u2, Valid: true}},
		[]int64{10, 20},
		[]int64{1, 2},
		[]int64{0, 1},
	)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var line string
		err = rows.Scan(&line)
		require.NoError(t, err)
		fmt.Println(line)
	}
	require.NoError(t, rows.Err())
}

// TestStats_DeadlockStress exercises concurrent batch updates with shuffled
// campaign order to catch lock-ordering deadlocks before they hit production RPS.
func TestIntegration_StatsDeadlockStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := testutil.SetupAdsPostgres(t)
	defer cleanup()

	ctx := context.Background()
	queries := db.New(pool)

	campaignIDs := make([]uuid.UUID, 5)
	for i := 0; i < 5; i++ {
		campaignIDs[i] = uuid.New()
		_, err := pool.Exec(ctx, "INSERT INTO campaigns (id, name, status) VALUES ($1, $2, $3)",
			campaignIDs[i], fmt.Sprintf("Stress Camp %d", i), "ACTIVE")
		require.NoError(t, err)
	}

	const workers = 10
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(workers)

	errChan := make(chan error, workers*iterations)

	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			for i := 0; i < iterations; i++ {
				indices := []int{0, 1, 2, 3, 4}
				rng.Shuffle(len(indices), func(i, j int) {
					indices[i], indices[j] = indices[j], indices[i]
				})

				selectedIDs := make([]pgtype.UUID, 3)
				for k := 0; k < 3; k++ {
					selectedIDs[k] = pgtype.UUID{Bytes: campaignIDs[indices[k]], Valid: true}
				}

				err := queries.UpdateCampaignStatsBatch(ctx, db.UpdateCampaignStatsBatchParams{
					CampaignIds: selectedIDs,
					Impressions: []int64{1, 2, 3},
					Clicks:      []int64{0, 1, 0},
					Conversions: []int64{0, 0, 0},
				})
				if err != nil {
					errChan <- err
				}
			}
		}(w)
	}

	wg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}

	for _, err := range errs {
		assert.NoError(t, err, "Should not produce any deadlock or other errors")
	}
	assert.Empty(t, errs, "All batch updates should succeed without errors")
}
