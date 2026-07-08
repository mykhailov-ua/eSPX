package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelfServe_CreateCampaign_requiresIdempotencyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	authMW, tokenMaker := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "SelfServe", 50_000_000, "USD"))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(map[string]any{
		"name":               "Self-serve camp",
		"budget_limit_micro": int64(5_000_000),
	})
	req, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	withSessionUser(req, tokenMaker, RoleUser, custID)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestSelfServe_CreateCampaign_insufficientBalance(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:             "test-secret",
		TokenSymmetricKey:       "01234567890123456789012345678901",
		SelfServeBudgetMinMicro: 1_000_000,
		SelfServeBudgetMaxMicro: 100_000_000_000,
	}
	authMW, tokenMaker := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Poor", 1_000_000, "USD"))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(map[string]any{
		"name":               "Too big",
		"budget_limit_micro": int64(50_000_000),
	})
	req, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "ss-create-1")
	withSessionUser(req, tokenMaker, RoleUser, custID)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestSelfServe_APIKey_requiresAuthService(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	authMW, _ := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("X-API-Key", "placeholder-key")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestSelfServe_PauseResume(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	authMW, tokenMaker := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "SS", 100_000_000, "USD"))
	ctx := context.Background()
	campID, err := svc.CreateCampaign(ctx, testCampaignSpec(custID, "SS Camp", 10_000_000, "ss-pause-idem"))
	require.NoError(t, err)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	pauseReq, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns/"+campID.String()+"/pause", bytes.NewReader([]byte("{}")))
	pauseReq.Header.Set("Content-Type", "application/json")
	withSessionUser(pauseReq, tokenMaker, RoleUser, custID)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, pauseReq)
	assert.Equal(t, http.StatusAccepted, rr.Code)

	resumeReq, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns/"+campID.String()+"/resume", bytes.NewReader([]byte("{}")))
	resumeReq.Header.Set("Content-Type", "application/json")
	withSessionUser(resumeReq, tokenMaker, RoleUser, custID)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, resumeReq)
	assert.Equal(t, http.StatusAccepted, rr.Code)
}

func TestSelfServe_PaymentIntent_requiresIdempotencyKey(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		AdminAPIKey:       "test-secret",
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	authMW, tokenMaker := integrationTestAuth(t, rdb, cfg)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Pay", 10_000_000, "USD"))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(map[string]any{"amount_micro": int64(5_000_000)})
	req, _ := http.NewRequest("POST", "/api/v1/selfserve/payment-intents", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	withSessionUser(req, tokenMaker, RoleUser, custID)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
