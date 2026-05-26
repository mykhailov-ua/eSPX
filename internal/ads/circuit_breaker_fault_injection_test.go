package ads

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mykhailov-ua/ad-event-processor/internal/domain"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FailingRedisClient simulates various connection issues (timeouts, refuse, etc.)
type FailingRedisClient struct {
	redis.UniversalClient
	failSet  bool
	failEval bool
	failErr  error
}

func (m *FailingRedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	if m.failSet {
		cmd.SetErr(m.failErr)
	}
	return cmd
}

func (m *FailingRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if m.failEval {
		cmd.SetErr(m.failErr)
	} else {
		cmd.SetVal(int64(-1)) // Force budget cache miss
	}
	return cmd
}

func (m *FailingRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	if m.failEval {
		cmd.SetErr(m.failErr)
	} else {
		cmd.SetVal(int64(-1)) // Force budget cache miss
	}
	return cmd
}

func (m *FailingRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	if m.failSet {
		cmd.SetErr(m.failErr)
	}
	return cmd
}

func (m *FailingRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetErr(m.failErr)
	return cmd
}

// FailingCampaignRepo simulates a database connection failure on budget misses
type FailingCampaignRepo struct {
	failErr error
}

func (r *FailingCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	return nil, r.failErr
}

func (r *FailingCampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	return r.failErr
}

func (r *FailingCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	return r.failErr
}

func (r *FailingCampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	return nil, r.failErr
}

// Scenario 1: Redis Node Timeout/Refusal during Ingestion via API
func TestFaultInjection_RedisTimeoutDuringIngestion(t *testing.T) {
	rdb := &FailingRedisClient{
		failSet: true,
		failErr: errors.New("redis command timeout (deadline exceeded)"),
	}

	geo := &MockGeoProvider{}
	f := NewFraudFilter(geo, rdb, 300*time.Millisecond)

	evt := &domain.Event{
		Type:       "impression",
		IP:         "1.1.1.1",
		UserID:     "user123",
		CampaignID: uuid.New(),
	}

	// Fraud check should handle Redis command timeout without crashing or leaking.
	// In the real system, it logs a warning but allows the ingestion flow to continue (asynchronous fail-safe).
	err := f.Check(context.Background(), evt)
	assert.NoError(t, err, "Ingestion filter must survive transient Redis errors gracefully")
}

// Scenario 2: Postgres Database Connection Failure on Budget Miss
func TestFaultInjection_PostgresCrashOnBudgetMiss(t *testing.T) {
	// Eval returns -1 to force a cache miss, prompting the budget manager to seed from Postgres
	rdb := &FailingRedisClient{
		failEval: false,
	}
	dbRepo := &FailingCampaignRepo{
		failErr: errors.New("fatal: pgx connection pool exhausted"),
	}

	bm := NewRedisBudgetManager(rdb, dbRepo, time.Hour)

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()
	clickID := "click_fail_1"
	amount := int64(150_000)

	// Verify that the postgres connection failure is propagated safely and doesn't leak or hang
	allowed, err := bm.CheckAndSpend(ctx, customerID, campaignID, clickID, amount)
	assert.False(t, allowed)
	assert.ErrorContains(t, err, "failed to load campaign from db on cache miss")
}

// Scenario 3: Stream Consumer Poison Pill Ingestion (Decomposition & DLQ)
func TestFaultInjection_StreamConsumerPoisonPillToDLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping testcontainers-based integration test in short mode")
	}

	rdb, cleanup := setupTestRedis(t)
	defer cleanup()

	// Store rejects zero/corrupted CampaignID events (poison pills)
	mockStore := &MockEventStore{
		Err: errors.New("postgres: null constraint violation on campaign_id"),
	}

	consumer := NewStreamConsumer(
		mockStore, rdb, "poison-stream", "poison-group", "poison-c",
		1, 1,
		10*time.Millisecond,
		50*time.Millisecond,
		5*time.Millisecond,
		10*time.Millisecond,
		1, // maxRetries=1 to trip DLQ immediately
		1*time.Minute,
		1*time.Second,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer.Start(ctx)

	// Produce a completely corrupt/malformed payload (invalid protobuf bytes)
	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "poison-stream",
		MaxLen: 1000,
		Approx: true,
		Values: []any{"d", "\xff\xff\xff\xff"}, // Invalid proto wire format bytes
	}).Result()
	require.NoError(t, err)

	// StreamConsumer should attempt to process it, fail, decompose the batch, and move it to DLQ
	assert.Eventually(t, func() bool {
		size, err := rdb.XLen(ctx, "ad:events:dlq").Result()
		return err == nil && size == 1
	}, 5*time.Second, 50*time.Millisecond, "Corrupt stream message should be moved to DLQ as a poison pill")

	// Ensure it is deleted from the main stream pending list
	pending, err := rdb.XPending(ctx, "poison-stream", "poison-group").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count, "DLQ'ed message must be deleted from main stream")

	consumer.Close()
	consumer.Wait(ctx)
}

// Scenario 4: Concurrency and Race Detector Verification under sustained failure
func TestFaultInjection_CircuitBreakerConcurrency(t *testing.T) {
	cb := NewCircuitBreaker(10, 50*time.Millisecond)

	var wg sync.WaitGroup
	var activeWorkers int32 = 10
	var successCount atomic.Int64
	var failureCount atomic.Int64

	// Spawn multiple concurrent workers driving state transitions under load
	for i := 0; i < int(activeWorkers); i++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				if cb.Allow() {
					successCount.Add(1)
					// Simulate intermittent failures and successes
					if j%3 == 0 {
						cb.RecordFailure(workerID)
					} else {
						cb.RecordSuccess(workerID)
					}
				} else {
					failureCount.Add(1)
					cb.RecordFailure(workerID)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(uuid.NewString()[:6])
	}

	wg.Wait()

	// Verify states are stable and consistent
	assert.Contains(t, []CircuitState{CircuitClosed, CircuitOpen, CircuitHalfOpen}, cb.State())
	assert.Greater(t, successCount.Load()+failureCount.Load(), int64(0))
}
