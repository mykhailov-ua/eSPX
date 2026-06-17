package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"espx/internal/ads/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatsBatching verifies that repeated batch UPSERTs accumulate counters
// correctly; billing and pacing dashboards rely on this aggregation semantics.
func TestStatsBatching(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
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

// TestStats_DeadlockStress exercises concurrent batch updates with shuffled
// campaign order to catch lock-ordering deadlocks before they hit production RPS.
func TestStats_DeadlockStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanup := setupTestDB(t)
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
