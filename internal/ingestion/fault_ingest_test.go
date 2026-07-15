package ingestion

import (
	"bytes"
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type infraErrFilter struct{}

var infraFilterNetErr = &net.OpError{
	Op:  "dial",
	Net: "tcp",
	Err: syscall.ECONNREFUSED,
}

func (infraErrFilter) Check(ctx context.Context, evt *campaignmodel.Event) error {
	return infraFilterNetErr
}

type mockRedisXAddFail struct {
	mockRedisClient
}

func (m *mockRedisXAddFail) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(errors.New("fraud stream write failed"))
	return cmd
}

func (m *mockRedisXAddFail) Pipeline() redis.Pipeliner {
	return &mockPipeliner{
		incrCmd:  redis.NewIntCmd(context.Background()),
		doCmd:    redis.NewCmd(context.Background()),
		xaddFail: true,
	}
}

type budgetMissRedis struct {
	mockRedisClient
}

func (m *budgetMissRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(-1))
	return cmd
}

func (m *budgetMissRedis) Process(ctx context.Context, cmd redis.Cmder) error {
	setProcessLuaInt64(cmd, -1)
	return nil
}

func (m *budgetMissRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(-1))
	return cmd
}

type slowCampaignRepo struct {
	delay time.Duration
}

func (r *slowCampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*campaignmodel.Campaign, error) {
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	return &campaignmodel.Campaign{
		ID:           id,
		BudgetLimit:  10_000_000,
		CurrentSpend: 0,
	}, nil
}

func (r *slowCampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status campaignmodel.CampaignStatus) error {
	return nil
}

func (r *slowCampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	return nil
}

func (r *slowCampaignRepo) ListActive(ctx context.Context) ([]*campaignmodel.Campaign, error) {
	return nil, nil
}

func TestGnetHandler_infraFilterErr_503WithRetryAfter(t *testing.T) {
	before := testutil.ToFloat64(filterEngineFailures)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, NewFilterEngine(0, infraErrFilter{}), nil, nil, NewJumpHashSharder(1), "fraud", nil)

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
	assert.Equal(t, before, testutil.ToFloat64(filterEngineFailures))
}

func TestGnetHandler_filterEngineFailure_500AndCounter(t *testing.T) {
	before := testutil.ToFloat64(filterEngineFailures)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, NewFilterEngine(0, &errFilter{err: errors.New("unexpected filter bug")}), nil, nil, NewJumpHashSharder(1), "fraud", nil)

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
	assert.Equal(t, before+1, testutil.ToFloat64(filterEngineFailures))
}

func TestGnetHandler_fraudStreamWriteError_incrementsCounter(t *testing.T) {
	before := testutil.ToFloat64(filterFraudStreamWriteErrors)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024, StreamMaxLen: 1000}
	rdb := &mockRedisXAddFail{}
	h := NewAdsPacketHandler(cfg, &mockRegistry{}, NewFilterEngine(0, &errFilter{err: ErrFraudDetected}), nil, []redis.UniversalClient{rdb}, NewJumpHashSharder(1), "fraud-stream", nil)
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
		return testutil.ToFloat64(filterFraudStreamWriteErrors) == before+1
	}, time.Second, 2*time.Millisecond)
}

func TestUnifiedFilter_budgetMiss_respectsDBLookupTimeout(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &mockRegistry{}
	staticCampaign.ID = campID
	staticCampaign.CustomerID = custID
	staticCampaign.IDStr = campID.String()
	staticCampaign.CustomerIDStr = custID.String()
	staticCampaign.IDStrAny = campID.String()
	staticCampaign.CustomerIDStrAny = custID.String()
	staticCampaign.DailyBudgetMicroAny = int64(10_000_000)
	staticCampaign.Location = time.UTC

	f := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissRedis{}},
		NewJumpHashSharder(1),
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
	f.dbLookupTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := f.Check(ctx, &campaignmodel.Event{
		CampaignID: campID,
		ClickID:    uuid.NewString(),
		Type:       "click",
		IP:         "1.1.1.1",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestFilterEngine_budgetMissRespectsEngineDeadline(t *testing.T) {
	campID := uuid.New()
	custID := uuid.New()
	reg := &mockRegistry{}
	staticCampaign.ID = campID
	staticCampaign.CustomerID = custID
	staticCampaign.IDStr = campID.String()
	staticCampaign.CustomerIDStr = custID.String()
	staticCampaign.IDStrAny = campID.String()
	staticCampaign.CustomerIDStrAny = custID.String()
	staticCampaign.DailyBudgetMicroAny = int64(10_000_000)
	staticCampaign.Location = time.UTC

	uf := NewUnifiedFilter(
		[]redis.UniversalClient{&budgetMissRedis{}},
		NewJumpHashSharder(1),
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
	uf.dbLookupTimeout = 2 * time.Second

	engine := NewFilterEngine(50*time.Millisecond, uf)
	start := time.Now()
	err := engine.Check(context.Background(), &campaignmodel.Event{
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

func TestPinnedWorkerPool_queueFullReject(t *testing.T) {
	pool := NewPinnedWorkerPool(1, 1)
	unblock := make(chan struct{})
	defer func() {
		close(unblock)
		pool.Shutdown()
	}()

	started := make(chan struct{})
	require.True(t, pool.Submit(func(_ int) {
		close(started)
		<-unblock
	}))
	<-started

	require.True(t, pool.Submit(func(_ int) { <-unblock }))
	require.False(t, pool.Submit(func(_ int) {}))
}

func TestAdsPacketHandler_workerPoolSaturated_rejectsAndCounts(t *testing.T) {
	before := testutil.ToFloat64(metrics.WorkerPoolRejectTotal)
	cfg := &config.Config{MaxRequestBodySize: 1024 * 1024}

	pool := NewPinnedWorkerPool(1, 1)
	unblock := make(chan struct{})
	defer func() {
		close(unblock)
		pool.Shutdown()
	}()

	started := make(chan struct{})
	require.True(t, pool.Submit(func(_ int) {
		close(started)
		<-unblock
	}))
	<-started
	require.True(t, pool.Submit(func(_ int) { <-unblock }))

	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.SetWorkerPool(pool)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := NewGnetHarnessConn(BuildGnetPostTrackJSON(body))

	_ = h.OnTraffic(conn)

	assert.True(t, bytes.Contains(conn.Written(), []byte("server overloaded")))
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.WorkerPoolRejectTotal))
}
