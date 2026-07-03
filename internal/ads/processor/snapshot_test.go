package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"espx/internal/ads/filter"
	"espx/internal/ads/sharding"
	"espx/internal/ads/testutil"
	"espx/internal/domain"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// Guards disaster replay reconciles Postgres spend from ClickHouse aggregate.
func TestSnapshotRecovery_DisasterStressReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping heavy HA/DR PITR stress integration test")
	}

	campID := uuid.New()
	custID := uuid.New()
	reg := &testutil.MockRegistry{}

	camp := &domain.Campaign{
		ID:                  campID,
		CustomerID:          custID,
		IDStr:               campID.String(),
		CustomerIDStr:       custID.String(),
		IDStrAny:            campID.String(),
		CustomerIDStrAny:    custID.String(),
		DailyBudgetMicroAny: int64(50_000_000),
		Location:            time.UTC,
	}
	testutil.CachedMockCamp.Store(camp)
	t.Cleanup(func() { testutil.CachedMockCamp.Store(nil) })

	rdb, cleanup := testutil.SetupTestRedis(t)
	defer cleanup()
	ctx := context.Background()

	pg := &testutil.MockPostgresDB{
		Spends:      make(map[uuid.UUID]int64),
		Limits:      map[uuid.UUID]int64{campID: int64(50_000_000)},
		Idempotency: make(map[string]bool),
	}
	pg.Healthy.Store(true)

	ch := &testutil.MockClickHouseDB{}

	budgetSourceKey := "budget:campaign:" + campID.String()
	_ = rdb.Set(ctx, budgetSourceKey, int64(50_000_000), 24*time.Hour).Err()

	sharder := sharding.NewJumpHashSharder(1)
	f := filter.NewUnifiedFilter(
		[]redis.UniversalClient{rdb},
		sharder,
		reg,
		nil,
		100000,
		time.Minute,
		time.Hour,
		time.Hour,
		10_000,
		1_000,
		"events-stream-sla",
		100000,
	)

	replicator := NewSnapshotReplicator(pg, ch, []redis.UniversalClient{rdb}, sharder, 10_000, 1_000)

	const concurrency = 20
	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(concurrency)

	startTime := time.Now().Add(-5 * time.Minute)

	for g := 0; g < concurrency; g++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				evtType := "click"
				if i%5 == 0 {
					evtType = "impression"
				}

				evt := &domain.Event{
					CampaignID: campID,
					ClickID:    fmt.Sprintf("clk_%d_%d", workerID, i),
					IP:         "192.168.1.100",
					Payload:    []byte(`{"bid_micro":5000}`),
					Type:       evtType,
					CreatedAt:  startTime.Add(time.Duration(workerID*10+i) * time.Second),
				}

				err := f.Check(ctx, evt)
				if err != nil {
					continue
				}

				ch.LogEvent(evt)
			}
		}(g)
	}

	wg.Wait()

	checkpointTime := startTime.Add(2 * time.Minute)
	snapshotData, err := replicator.CreateSnapshot(ctx, checkpointTime)
	assert.NoError(t, err)

	var snap Snapshot
	err = json.Unmarshal(snapshotData, &snap)
	assert.NoError(t, err)
	checkpointSpend := snap.CampaignSpends[campID]
	assert.Greater(t, checkpointSpend, int64(0))

	liveStart := checkpointTime.Add(time.Second)
	var postWg sync.WaitGroup
	postWg.Add(10)
	for g := 0; g < 10; g++ {
		go func(workerID int) {
			defer postWg.Done()
			for i := 0; i < 50; i++ {
				evt := &domain.Event{
					CampaignID: campID,
					ClickID:    fmt.Sprintf("post_clk_%d_%d", workerID, i),
					IP:         "192.168.1.100",
					Payload:    []byte(`{"bid_micro":5000}`),
					Type:       "click",
					CreatedAt:  liveStart.Add(time.Duration(workerID*10+i) * time.Second),
				}

				err := f.Check(ctx, evt)
				if err != nil {
					continue
				}

				ch.LogEvent(evt)
			}
		}(g)
	}
	postWg.Wait()

	totalActualSpend, err := ch.QueryAggregatedSpend(ctx, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)
	expectedFinalSpend := totalActualSpend[campID]

	_ = rdb.Del(ctx, budgetSourceKey).Err()

	pg.Spends[campID] = 0
	for k := range pg.Idempotency {
		delete(pg.Idempotency, k)
	}

	restoredSnap, err := replicator.RestoreSnapshot(ctx, snapshotData)
	assert.NoError(t, err)
	assert.Equal(t, checkpointSpend, restoredSnap.CampaignSpends[campID])

	assert.Equal(t, checkpointSpend, pg.Spends[campID])

	redisBudget, err := rdb.Get(ctx, budgetSourceKey).Int64()
	assert.NoError(t, err)
	assert.Equal(t, 50_000_000-checkpointSpend, redisBudget)

	replayedCount, err := replicator.ReplayTelemetrySince(ctx, checkpointTime, f)
	assert.NoError(t, err)
	assert.Greater(t, replayedCount, 0)

	finalPgSpend := pg.Spends[campID]

	assert.Equal(t, expectedFinalSpend, finalPgSpend)
}
