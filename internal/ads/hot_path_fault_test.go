package ads

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/metrics"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper type for infraErrFilter scenarios.
type infraErrFilter struct{}

func (infraErrFilter) Check(ctx context.Context, evt *domain.Event) error {
	return errors.New("redis: connection refused")
}

// Redis stub failing XAdd for fraud stream error counter tests.
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

// Redis stub returning budget miss for timeout recovery tests.
type budgetMissRedis struct {
	mockRedisClient
}

func (m *budgetMissRedis) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetVal(int64(-1))
	return cmd
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

// Test helper type for trafficMockConn scenarios.
type trafficMockConn struct {
	mockGnetConn
	inbound []byte
}

func (m *trafficMockConn) InboundBuffered() int { return len(m.inbound) }

func (m *trafficMockConn) Peek(n int) ([]byte, error) {
	if n > len(m.inbound) {
		n = len(m.inbound)
	}
	return m.inbound[:n], nil
}

func (m *trafficMockConn) Discard(n int) (int, error) {
	if n > len(m.inbound) {
		n = len(m.inbound)
	}
	m.inbound = m.inbound[n:]
	return n, nil
}

func (m *trafficMockConn) AsyncWrite(buf []byte, callback gnet.AsyncCallback) error {
	m.written = append(m.written[:0], buf...)
	if callback != nil {
		_ = callback(m, nil)
	}
	return nil
}

// Wraps track body in minimal HTTP bytes for gnet handler tests.
func makeTrackHTTPBytes(body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("POST /track HTTP/1.1\r\n")
	buf.WriteString("Content-Type: application/json\r\n")
	buf.WriteString("Content-Length: ")
	buf.WriteString(strconv.Itoa(len(body)))
	buf.WriteString("\r\nConnection: keep-alive\r\n\r\n")
	buf.Write(body)
	return buf.Bytes()
}

// Guards infra filter errors return 503 with Retry-After header.
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

// Guards filter engine failure returns 500 and increments error counter.
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

// Guards fraud stream write failure increments telemetry without blocking response.
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

// Guards budget miss Postgres lookup respects configured timeout.
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
	before := testutil.ToFloat64(metrics.WorkerPoolRejectTotal)
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

	h := NewAdsPacketHandler(cfg, &mockRegistry{}, nil, nil, nil, NewJumpHashSharder(1), "fraud", nil)
	h.SetWorkerPool(pool)

	body := []byte(`{"campaign_id":"` + uuid.NewString() + `","type":"click","click_id":"c1"}`)
	conn := &trafficMockConn{
		mockGnetConn: mockGnetConn{written: make([]byte, 0, 512)},
		inbound:      makeTrackHTTPBytes(body),
	}

	_ = h.OnTraffic(conn)

	assert.True(t, bytes.Contains(conn.written, []byte("server overloaded")))
	assert.Equal(t, before+1, testutil.ToFloat64(metrics.WorkerPoolRejectTotal))
}
