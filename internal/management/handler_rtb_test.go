package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRtbDealsAPI_CRUDAndOutbox(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "RTB Advertiser", 1_000_000, "USD"))

	body, _ := json.Marshal(RtbDealCreateSpec{
		DealID:     "deal-premium-1",
		FloorMicro: 250_000,
		GeoMask:    255,
		CatMask:    31,
		Pacing:     "open",
		CustomerID: customerID.String(),
	})
	req, _ := http.NewRequest("POST", "/admin/rtb/deals", bytes.NewReader(body))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())

	var created RtbDealDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
	assert.Equal(t, "deal-premium-1", created.DealID)
	assert.Equal(t, "open", created.Pacing)

	req, _ = http.NewRequest("GET", "/admin/rtb/deals", nil)
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	var listResp struct {
		Deals []RtbDealDTO `json:"deals"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&listResp))
	require.Len(t, listResp.Deals, 1)

	updateBody, _ := json.Marshal(RtbDealUpdateSpec{
		DealID:     "deal-premium-1",
		FloorMicro: 300_000,
		GeoMask:    255,
		CatMask:    31,
		Pacing:     "closed",
		CustomerID: customerID.String(),
	})
	req, _ = http.NewRequest("PUT", "/admin/rtb/deals/"+strconv.FormatInt(created.ID, 10), bytes.NewReader(updateBody))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var updated RtbDealDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&updated))
	assert.Equal(t, "closed", updated.Pacing)
	assert.Equal(t, int64(300_000), updated.FloorMicro)

	sub := rdb.Subscribe(ctx, "rtb:catalog:reload")
	defer sub.Close()
	_, recvErr := sub.Receive(ctx)
	require.NoError(t, recvErr)

	worker := NewOutboxWorker(svc)
	n, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 2, n, "create + update each enqueue RELOAD_RTB_CATALOG")

	msg, err := sub.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)
	m, ok := msg.(*redis.Message)
	require.True(t, ok, "expected redis.Message, got %T", msg)
	assert.Equal(t, "reload", m.Payload)

	req, _ = http.NewRequest("DELETE", "/admin/rtb/deals/"+strconv.FormatInt(created.ID, 10), nil)
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var outboxCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM outbox_events WHERE event_type = 'RELOAD_RTB_CATALOG'`).Scan(&outboxCount))
	assert.Equal(t, 3, outboxCount)

	var auditCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM admin_audit_log WHERE action IN ('CREATE_RTB_DEAL', 'UPDATE_RTB_DEAL', 'DELETE_RTB_DEAL')`).Scan(&auditCount))
	assert.Equal(t, int64(3), auditCount)
}

func TestRtbDealsAPI_duplicateDealID(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Dup Co", 1_000_000, "USD"))

	spec := RtbDealCreateSpec{
		DealID:     "dup-deal",
		FloorMicro: 100,
		CustomerID: customerID.String(),
	}
	body, _ := json.Marshal(spec)
	req, _ := http.NewRequest("POST", "/admin/rtb/deals", bytes.NewReader(body))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	body, _ = json.Marshal(spec)
	req, _ = http.NewRequest("POST", "/admin/rtb/deals", bytes.NewReader(body))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestRtbShadowDiffAPI(t *testing.T) {
	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, nil, nil, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ads.ResetRtbShadowDiffBuckets()

	req, _ := http.NewRequest("GET", "/admin/rtb/shadow-diff?window=1h", nil)
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var snap ads.RtbShadowDiffSnapshotDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snap))
	assert.Equal(t, "1h0m0s", snap.Window)
	assert.Equal(t, "memory", snap.Source)
}

func TestRtbDealsAPI_invalidSeats(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ctx := context.Background()
	customerID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, customerID, "Seats Co", 1_000_000, "USD"))

	body, _ := json.Marshal(RtbDealCreateSpec{
		DealID:     "bad-seats",
		FloorMicro: 100,
		Seats:      -1,
		CustomerID: customerID.String(),
	})
	req, _ := http.NewRequest("POST", "/admin/rtb/deals", bytes.NewReader(body))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestRtbDeals_ExplainAnalyze(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rtb_deals EXPLAIN ANALYZE in short mode")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	ctx := context.Background()
	customerID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO customers (id, name, balance, currency)
		VALUES ($1, 'Explain Co', 1000000, 'USD')`, customerID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO rtb_deals (deal_id, floor_micro, geo_mask, cat_mask, pacing, customer_id, seats)
		VALUES ('explain-deal', 250000, 255, 31, 1, $1, 2)`, customerID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE rtb_deals`)
	require.NoError(t, err)

	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "list_by_deal_id",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM rtb_deals ORDER BY deal_id`,
		},
		{
			name: "lookup_by_id",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM rtb_deals WHERE id = 1`,
		},
		{
			name: "lookup_by_deal_id",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM rtb_deals WHERE deal_id = 'explain-deal'`,
		},
		{
			name: "lookup_by_customer",
			sql:  `EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT TEXT) SELECT * FROM rtb_deals WHERE customer_id = $1`,
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			var args []any
			if q.name == "lookup_by_customer" {
				args = append(args, customerID)
			}
			rows, err := pool.Query(ctx, q.sql, args...)
			require.NoError(t, err)
			defer rows.Close()

			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())
			require.NotEmpty(t, plan)
			t.Logf("plan %s:\n%s", q.name, joinPlan(plan))
		})
	}
}

func TestRtbBudgetAuthority_settingsPropagation(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	ctx := context.Background()

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rtb_budget_authority": "rtb"}))
	worker := NewOutboxWorker(svc)
	n, err := worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	val, err := rdb.HGet(ctx, "config:values", "rtb_budget_authority").Result()
	require.NoError(t, err)
	assert.Equal(t, "rtb", val)

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"rtb_budget_authority": "lua"}))
	n, err = worker.ProcessOutboxWithCount(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	val, err = rdb.HGet(ctx, "config:values", "rtb_budget_authority").Result()
	require.NoError(t, err)
	assert.Equal(t, "lua", val)
}

func joinPlan(lines []string) string {
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}
