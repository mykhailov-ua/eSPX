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

// TestChaos_SelfServeOverdraftReject proves budget headroom is enforced before campaign insert.
func TestChaos_SelfServeOverdraftReject(t *testing.T) {
	t.Parallel()
	TestSelfServe_CreateCampaign_insufficientBalance(t)
}

// TestChaos_SelfServeIdempotentCreate proves duplicate Idempotency-Key returns the same campaign id.
func TestChaos_SelfServeIdempotentCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos integration test")
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
	require.NoError(t, svc.CreateCustomer(context.Background(), custID, "Idem", 100_000_000, "USD"))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(map[string]any{
		"name":               "Idempotent",
		"budget_limit_micro": int64(5_000_000),
	})
	idemKey := "ss-idem-chaos-" + uuid.New().String()

	var firstID string
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", "/api/v1/selfserve/campaigns", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)
		withSessionUser(req, tokenMaker, RoleUser, custID)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		require.Equal(t, http.StatusCreated, rr.Code, "attempt %d", i+1)
		var resp map[string]any
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		id, ok := resp["id"].(string)
		require.True(t, ok)
		if i == 0 {
			firstID = id
		} else {
			assert.Equal(t, firstID, id)
		}
	}
}
