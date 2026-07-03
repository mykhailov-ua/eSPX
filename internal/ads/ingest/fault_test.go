package ingest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"espx/internal/ads/filter"
	"espx/internal/ads/sharding"
	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/metrics"

	adstest "espx/internal/ads/testutil"
	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedMockCampaign(campID, custID uuid.UUID) {
	idStr := campID.String()
	custStr := custID.String()
	adstest.CachedMockCamp.Store(&domain.Campaign{
		ID:                  campID,
		CustomerID:          custID,
		IDStr:               idStr,
		CustomerIDStr:       custStr,
		IDStrAny:            idStr,
		CustomerIDStrAny:    custStr,
		DailyBudgetMicroAny: int64(10_000_000),
		Location:            time.UTC,
	})
}

// Test helper type for infraErrFilter scenarios.
type infraErrFilter struct{}

func (infraErrFilter) Check(ctx context.Context, evt *domain.Event) error {
	return errors.New("redis: connection refused")
}

// Redis stub failing XAdd for fraud stream error counter tests.
type mockRedisXAddFail struct {
	adstest.MockRedisClient
}

func (m *mockRedisXAddFail) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(errors.New("fraud stream write failed"))
	return cmd
}

func (m *mockRedisXAddFail) Pipeline() redis.Pipeliner {
	return &adstest.MockPipeliner{
		IncrCmd:  redis.NewIntCmd(context.Background()),
		DoCmd:    redis.NewCmd(context.Background()),
		XAddFail: true,
	}
}

// Redis stub returning budget miss for timeout recovery tests.
type budgetMissRedis struct {
	adstest.MockRedisClient
}

func (m *budgetMissRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(-1))
	return cmd
}

func (m *budgetMissRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	adstest.SetProcessLuaInt64(cmd, -1)
	return nil
}

func (m *budgetMissRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(-1))
	return cmd
}

// Test helper type for slowCampaignRepo scenarios.
type slowCampaignRepo struct {
	delay time.Duration
}

func (r *slowCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return &domain.Campaign{
		ID:           id,
		BudgetLimit:  10_000_000,
		CurrentSpend: 0,
	}, nil
}

func (r *slowCampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	return nil
}

func (r *slowCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	return nil
}

func (r *slowCampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	return nil, nil
}

// Guards infra filter errors return 503 with Retry-After header.
func TestGnetHandler_infraFilterErr_503WithRetryAfter(t *testing.T) {
	before := promtest.ToFloat64(filter.FilterEngineFailures)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &adstest.MockRegistry{}, filter.NewFilterEngine(0, infraErrFilter{}), nil, nil, sharding.NewJumpHashSharder(1), "fraud", nil)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := &mockGnetConn{written: make([]byte, 0, 512)}
	h.React(parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}, conn)

	assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 503")))
	assert.True(t, bytes.Contains(conn.written, []byte("Retry-After: 1")))
	assert.Equal(t, before, promtest.ToFloat64(filter.FilterEngineFailures))
}

// Guards filter engine failure returns 500 and increments error counter.
func TestGnetHandler_filterEngineFailure_500AndCounter(t *testing.T) {
	before := promtest.ToFloat64(filter.FilterEngineFailures)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &adstest.MockRegistry{}, filter.NewFilterEngine(0, &errFilter{err: errors.New("unexpected filter bug")}), nil, nil, sharding.NewJumpHashSharder(1), "fraud", nil)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := &mockGnetConn{written: make([]byte, 0, 512)}
	h.React(parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}, conn)

	assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 500")))
	assert.Equal(t, before+1, promtest.ToFloat64(filter.FilterEngineFailures))
}

// Guards fraud stream write failure increments telemetry without blocking response.
func TestGnetHandler_fraudStreamWriteError_incrementsCounter(t *testing.T) {
	before := promtest.ToFloat64(filter.FilterFraudStreamWriteErrors)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024, StreamMaxLen: 1000}
	rdb := &mockRedisXAddFail{}
	h := NewAdsPacketHandler(cfg, &adstest.MockRegistry{}, filter.NewFilterEngine(0, &errFilter{err: filter.ErrFraudDetected}), nil, []redis.UniversalClient{rdb}, sharding.NewJumpHashSharder(1), "fraud-stream", nil)
	defer h.fraudWriter.Stop()

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := &mockGnetConn{written: make([]byte, 0, 512)}
	h.React(parsedHTTPRequest{
		Method:           []byte("POST"),
		Path:             []byte("/track"),
		ContentType:      []byte("application/json"),
		Body:             body,
		ContentLength:    len(body),
		HasContentLength: true,
	}, conn)

	assert.True(t, bytes.HasPrefix(conn.written, []byte("HTTP/1.1 202")))
	require.Eventually(t, func() bool {
		return promtest.ToFloat64(filter.FilterFraudStreamWriteErrors) == before+1
	}, time.Second, 2*time.Millisecond)
}

// Guards budget miss Postgres lookup respects configured timeout.
func TestUnifiedFilter_budgetMiss_respectsDBLookupTimeout(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &adstest.MockRegistry{}
	seedMockCampaign(campID, custID)

	f := filter.NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissRedis{}},
		sharding.NewJumpHashSharder(1),
		reg,
		&slowCampaignRepo{delay: 500 * time.Millisecond},
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000,
		10_000,
		"events-budget-miss",
		10000,
	)
	f.SetDBLookupTimeoutForTest(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := f.Check(ctx, &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// Guards budget miss recovery respects filter engine deadline.
func TestFilterEngine_budgetMissRespectsEngineDeadline(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &adstest.MockRegistry{}
	seedMockCampaign(campID, custID)

	uf := filter.NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissRedis{}},
		sharding.NewJumpHashSharder(1),
		reg,
		&slowCampaignRepo{delay: 200 * time.Millisecond},
		1000,
		time.Minute,
		time.Hour,
		time.Hour,
		1_000_000,
		10_000,
		"events-deadline-gap",
		10000,
	)
	uf.SetDBLookupTimeoutForTest(2 * time.Second)

	engine := filter.NewFilterEngine(50*time.Millisecond, uf)
	start := time.Now()
	err := engine.Check(context.Background(), &domain.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 120*time.Millisecond, "budget miss PG lookup must honor FilterEngine deadline")
}

// Guards saturated worker pool rejects work instead of blocking event loop.
func TestPinnedWorkerPool_queueFullReject(t *testing.T) {
	pool := NewPinnedWorkerPool(1, 1)
	unblock := make(chan struct{})
	defer func() {
		close(unblock)
		pool.Shutdown()
	}()

	started := make(chan struct{})
	require.True(t, pool.Submit(func() {
		close(started)
		<-unblock
	}))
	<-started

	require.True(t, pool.Submit(func() { <-unblock }))
	require.False(t, pool.Submit(func() {}))
}

// Guards saturated handler pool rejects requests and increments reject counter.
func TestAdsPacketHandler_workerPoolSaturated_rejectsAndCounts(t *testing.T) {
	before := promtest.ToFloat64(metrics.WorkerPoolRejectTotal)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}

	pool := NewPinnedWorkerPool(1, 1)
	unblock := make(chan struct{})
	defer func() {
		close(unblock)
		pool.Shutdown()
	}()

	started := make(chan struct{})
	require.True(t, pool.Submit(func() {
		close(started)
		<-unblock
	}))
	<-started
	require.True(t, pool.Submit(func() { <-unblock }))

	h := NewAdsPacketHandler(cfg, &adstest.MockRegistry{}, nil, nil, nil, sharding.NewJumpHashSharder(1), "fraud", nil)
	h.SetWorkerPool(pool)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := NewGnetHarnessConn(BuildGnetPostTrackJSON(body))

	_ = h.OnTraffic(conn)

	assert.True(t, bytes.Contains(conn.Written(), []byte("server overloaded")))
	assert.Equal(t, before+1, promtest.ToFloat64(metrics.WorkerPoolRejectTotal))
}
