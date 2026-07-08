package management

import (
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

func TestHandlerAudit_pagination(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()

	ctx := context.Background()
	adminID := uuid.New()
	for i := 0; i < 55; i++ {
		svc.AuditLog(ctx, nil, adminID, "PAGINATION_TEST", "system", nil, map[string]int{"i": i}, nil)
	}

	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("default_limit_50", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/audit", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "55", resp.Header().Get("X-Total-Count"))

		var items []map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))
		assert.Len(t, items, 50)
	})

	t.Run("limit_and_offset", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/audit?limit=10&offset=50", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, "55", resp.Header().Get("X-Total-Count"))

		var items []map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))
		assert.Len(t, items, 5)
	})

	t.Run("limit_capped_at_1000", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/admin/audit?limit=1000000", nil)
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)

		var items []map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))
		assert.Len(t, items, 55)
	})
}
