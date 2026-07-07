package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/config"
	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_ListReconRuns_Management(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	svc.SetPaymentPool(pool)
	h := NewHandler(svc, cfg, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO recon_runs (period_start, period_end, status, total_delta, campaigns_checked, discrepancies_found, completed_at)
		VALUES ($1, $2, 'COMPLETED', 5000, 10, 1, NOW())`, start, end)
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", "/api/v1/recon/runs?service=management", nil)
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
	var runs []ReconRunDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&runs))
	require.Len(t, runs, 1)
	assert.Equal(t, "management", runs[0].Service)
	assert.Equal(t, "COMPLETED", runs[0].Status)
}
