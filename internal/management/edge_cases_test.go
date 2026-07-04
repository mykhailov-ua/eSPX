package management

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEdge_RoundingAndSmallAmounts guards cancellation fee rounding on small campaign budgets.
func TestEdge_RoundingAndSmallAmounts(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10.0
	cfg.Lifecycle.WaitTimeoutMs = 1
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	customerID := uuid.New()
	_ = svc.CreateCustomer(context.Background(), customerID, "Small Saver", 100_000_000, "USD")

	budget := int64(1_050_000)
	id, err := svc.CreateCampaign(context.Background(), testCampaignSpec(customerID, "Tiny Camp", budget, "idemp-1"))
	require.NoError(t, err)

	err = svc.CancelCampaign(context.Background(), id, "Too small")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var finalBalance int64
		_ = pool.QueryRow(context.Background(), "SELECT balance FROM customers WHERE id = $1", customerID).Scan(&finalBalance)
		return finalBalance == 99895000
	}, 2*time.Second, 20*time.Millisecond)
}

// TestEdge_ConcurrentBalanceDepletion re-exports chaos balance depletion coverage for the edge-case suite.
func TestEdge_ConcurrentBalanceDepletion(t *testing.T) {
	t.Run("delegatesToChaosTest", func(t *testing.T) {
		TestChaos_ConcurrentBalanceDepletion(t)
	})
}

// TestEdge_ResumingStuckSettlement guards cancel can finish settlement when a campaign is stuck in DRAINING.
func TestEdge_ResumingStuckSettlement(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{}
	cfg.Management.CancellationFeePercent = 10
	cfg.Lifecycle.WaitTimeoutMs = 1
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	customerID := uuid.New()
	_ = svc.CreateCustomer(context.Background(), customerID, "Crash Test", 1_000_000_000, "USD")
	campaignID, _ := svc.CreateCampaign(context.Background(), testCampaignSpec(customerID, "Zombie", 500_000_000, "idemp-crash"))

	_, _ = pool.Exec(context.Background(), "UPDATE campaigns SET status = 'DRAINING' WHERE id = $1", campaignID)

	err := svc.CancelCampaign(context.Background(), campaignID, "Resume")
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		var status string
		_ = pool.QueryRow(context.Background(), "SELECT status FROM campaigns WHERE id = $1", campaignID).Scan(&status)
		return status == "DELETED"
	}, 2*time.Second, 20*time.Millisecond)
}

type failingRedisClient struct {
	redis.UniversalClient
	failCampaignID string
}

func (c *failingRedisClient) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	failPipe := &failingPipeliner{
		Pipeliner:      c.UniversalClient.Pipeline(),
		failCampaignID: c.failCampaignID,
	}
	err := fn(failPipe)
	if err != nil {
		return nil, err
	}
	if failPipe.shouldFail {
		return nil, errors.New("simulated redis pipeline failure")
	}
	return failPipe.Pipeliner.Exec(ctx)
}

func (c *failingRedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	if msgStr, ok := message.(string); ok && msgStr == c.failCampaignID {
		cmd := redis.NewIntCmd(ctx)
		cmd.SetErr(errors.New("simulated redis publish failure"))
		return cmd
	}
	return c.UniversalClient.Publish(ctx, channel, message)
}

type failingPipeliner struct {
	redis.Pipeliner
	failCampaignID string
	shouldFail     bool
}

func (p *failingPipeliner) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	if msgStr, ok := message.(string); ok && msgStr == p.failCampaignID {
		p.shouldFail = true
	}
	return p.Pipeliner.Publish(ctx, channel, message)
}

// TestEdge_OutboxPartialRedisFailure guards partial Redis failures leave failed events pending for retry.
func TestEdge_OutboxPartialRedisFailure(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	failCampaignID := uuid.New().String()
	wrappedRDB := &failingRedisClient{
		UniversalClient: rdb,
		failCampaignID:  failCampaignID,
	}

	cfg := &config.Config{
		CampaignUpdateChannel: "campaigns:update-test",
	}
	svc := NewService(pool, []redis.UniversalClient{wrappedRDB}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	ctx := context.Background()
	queries := db.New(pool)

	campaignIDs := []string{uuid.New().String(), failCampaignID, uuid.New().String()}
	for _, cid := range campaignIDs {
		payload := CampaignPayload{
			CampaignID:  cid,
			BudgetLimit: 100_500_000,
		}
		payloadBytes, err := json.Marshal(payload)
		require.NoError(t, err)

		_, err = queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "CREATE_CAMPAIGN",
			Payload:   payloadBytes,
		})
		require.NoError(t, err)
	}

	worker := NewOutboxWorker(svc)
	processed, err := worker.ProcessOutboxWithCount(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, 2, processed)

	rows, err := pool.Query(ctx, "SELECT id, status, payload FROM outbox_events ORDER BY id ASC")
	require.NoError(t, err)
	defer rows.Close()

	var statuses []string
	for rows.Next() {
		var id int64
		var status string
		var payload []byte
		err := rows.Scan(&id, &status, &payload)
		require.NoError(t, err)
		statuses = append(statuses, status)
	}

	require.Len(t, statuses, 3)
	assert.Equal(t, "PROCESSED", statuses[0])
	assert.Equal(t, "PROCESSED", statuses[2])
	assert.Equal(t, "PENDING", statuses[1])
}

// TestEdge_OutboxWorkerRecoveryOfProcessingEvents guards stale PROCESSING leases revert to PENDING and reprocess.
func TestEdge_OutboxWorkerRecoveryOfProcessingEvents(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		CampaignUpdateChannel: "campaigns:update-test",
	}
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	ctx := context.Background()
	queries := db.New(pool)

	payload := CampaignPayload{
		CampaignID:  uuid.New().String(),
		BudgetLimit: 50_000_000,
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	row, err := queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "CREATE_CAMPAIGN",
		Payload:   payloadBytes,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PROCESSING', processing_started_at = NOW() - INTERVAL '10 minutes'
		WHERE id = $1`, row.ID)
	require.NoError(t, err)

	worker := NewOutboxWorker(svc)

	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, processed, "Normal polling must ignore 'PROCESSING' events")

	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", row.ID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "PROCESSING", status)

	_, err = pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PENDING', processing_started_at = NULL
		WHERE status = 'PROCESSING'
		  AND processing_started_at IS NOT NULL
		  AND processing_started_at < NOW() - INTERVAL '1 minute'`)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", row.ID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", status, "Recovery query must revert expired 'PROCESSING' events to 'PENDING'")

	processed, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, processed, "Reverted event must be processed successfully")

	err = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", row.ID).Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "PROCESSED", status)
}

// TestEdge_OutboxSetsRemainingBudget guards resume outbox handler sets Redis budget to limit minus spend.
func TestEdge_OutboxSetsRemainingBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{CampaignUpdateChannel: "campaigns:update-test"}
	svc := NewService(pool, []redis.UniversalClient{rdb}, ads.NewJumpHashSharder(1), cfg)
	defer svc.Close()

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Budget Test", 1_000_000_000, "USD"))

	budget := int64(100_000_000)
	spend := int64(30_000_000)
	spec := testCampaignSpec(customerID, "Spend Camp", budget, "remaining-idem")
	spec.DaypartHours = []int16{}
	campaignID, err := svc.CreateCampaign(ctx, spec)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, "DELETE FROM outbox_events")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE campaigns SET current_spend = $1 WHERE id = $2", spend, campaignID)
	require.NoError(t, err)
	_, err = rdb.Del(ctx, "budget:campaign:"+campaignID.String()).Result()
	require.NoError(t, err)

	payload, err := json.Marshal(CampaignPayload{CampaignID: campaignID.String(), BudgetLimit: budget})
	require.NoError(t, err)
	_, err = db.New(pool).CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "RESUME_CAMPAIGN",
		Payload:   payload,
	})
	require.NoError(t, err)

	worker := NewOutboxWorker(svc)
	processed, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	remaining, err := rdb.Get(ctx, "budget:campaign:"+campaignID.String()).Int64()
	require.NoError(t, err)
	assert.Equal(t, budget-spend, remaining)
}
