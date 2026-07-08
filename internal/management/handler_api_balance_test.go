package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_GetCustomerBalance(t *testing.T) {
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
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Balance API", 250_000_000, "USD"))

	for i := 0; i < 3; i++ {
		_, err := pool.Exec(context.Background(), `
			INSERT INTO balance_ledger (customer_id, amount, type, idempotency_hash)
			VALUES ($1, $2, 'TOPUP', $3)`,
			ads.ToUUID(custID), int64((i+1)*1_000_000), fmt.Sprintf("hash-%d", i))
		require.NoError(t, err)
	}

	req, _ := http.NewRequest("GET", "/api/v1/customers/"+custID.String()+"/balance", nil)
	withSessionUser(req, tokenMaker, RoleUser, custID)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var report CustomerBalanceDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&report))
	assert.Equal(t, "250.00", report.Balance)
	assert.Len(t, report.Ledger, 3)
	assert.Equal(t, "3.00", report.Ledger[0].Amount)
}

func TestAPI_GetCustomerBalance_TenantIsolation(t *testing.T) {
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
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	ownerID := uuid.New()
	otherID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), ownerID, "Owner", 100_000_000, "USD"))

	req, _ := http.NewRequest("GET", "/api/v1/customers/"+ownerID.String()+"/balance", nil)
	withSessionUser(req, tokenMaker, RoleUser, otherID)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}

func TestAPI_ExportCustomerBalance_CSV(t *testing.T) {
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
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Export", 0, "USD"))
	_, err := pool.Exec(context.Background(), `
		INSERT INTO balance_ledger (customer_id, amount, type, idempotency_hash)
		VALUES ($1, 1000000, 'TOPUP', 'export-1')`, ads.ToUUID(custID))
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", "/api/v1/customers/"+custID.String()+"/balance/export?format=csv", nil)
	withSessionUser(req, tokenMaker, RoleUser, custID)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "text/csv; charset=utf-8", resp.Header().Get("Content-Type"))
	body := resp.Body.String()
	assert.True(t, strings.HasPrefix(body, "id,customer_id"))
	assert.Contains(t, body, "TOPUP")
}

func TestAPI_ExportCustomerBalance_BufferOverflowCap(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
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

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Overflow", 0, "USD"))

	padding := strings.Repeat("X", 4096)
	for i := 0; i < 3000; i++ {
		_, err := pool.Exec(context.Background(), `
			INSERT INTO balance_ledger (customer_id, amount, type, idempotency_hash)
			VALUES ($1, 1000, 'FEE', $2)`,
			ads.ToUUID(custID), fmt.Sprintf("%s-%d", padding, i))
		require.NoError(t, err)
	}

	req, _ := http.NewRequest("GET", "/api/v1/customers/"+custID.String()+"/balance/export?format=csv", nil)
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "true", resp.Header().Get("X-Export-Truncated"))
	assert.NotEmpty(t, resp.Header().Get("X-Next-Cursor"))

	bytesWritten, _ := strconv.Atoi(resp.Header().Get("X-Export-Bytes"))
	assert.LessOrEqual(t, bytesWritten, ledgerExportMaxBytes)
	assert.Greater(t, bytesWritten, ledgerExportMaxBytes-50_000)
}

func TestAPI_ExportCustomerBalance_RateLimit(t *testing.T) {
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	h.customerLimiter = newCustomerRateLimiter()
	h.customerLimiter.limit = 0
	h.customerLimiter.burst = 1
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "RL", 0, "USD"))

	url := "/api/v1/customers/" + custID.String() + "/balance/export?format=csv"
	req1, _ := http.NewRequest("GET", url, nil)
	withAdminAPIKey(req1, cfg)
	resp1 := httptest.NewRecorder()
	mux.ServeHTTP(resp1, req1)
	require.Equal(t, http.StatusOK, resp1.Code)

	req2, _ := http.NewRequest("GET", url, nil)
	withAdminAPIKey(req2, cfg)
	resp2 := httptest.NewRecorder()
	mux.ServeHTTP(resp2, req2)
	assert.Equal(t, http.StatusTooManyRequests, resp2.Code)
}

func TestAPI_ExportCustomerBalance_CursorResume(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
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

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Cursor", 0, "USD"))
	padding := strings.Repeat("Y", 2048)
	for i := 0; i < 6000; i++ {
		_, err := pool.Exec(context.Background(), `
			INSERT INTO balance_ledger (customer_id, amount, type, idempotency_hash)
			VALUES ($1, 1000, 'FEE', $2)`,
			ads.ToUUID(custID), fmt.Sprintf("%s-%d", padding, i))
		require.NoError(t, err)
	}

	url := "/api/v1/customers/" + custID.String() + "/balance/export?format=csv"
	req1, _ := http.NewRequest("GET", url, nil)
	withAdminAPIKey(req1, cfg)
	resp1 := httptest.NewRecorder()
	mux.ServeHTTP(resp1, req1)
	require.Equal(t, http.StatusOK, resp1.Code)
	cursor := resp1.Header().Get("X-Next-Cursor")
	require.NotEmpty(t, cursor)

	req2, _ := http.NewRequest("GET", url+"&cursor="+cursor, nil)
	withAdminAPIKey(req2, cfg)
	resp2 := httptest.NewRecorder()
	mux.ServeHTTP(resp2, req2)
	require.Equal(t, http.StatusOK, resp2.Code)
	assert.NotEmpty(t, resp2.Body.String())
}
