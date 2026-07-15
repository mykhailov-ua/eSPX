package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/clickhouse/migrate"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChaos_ForecastDeterministic proves identical inputs yield identical forecast outputs.
func TestChaos_ForecastDeterministic(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}

	conn, cleanupCH := setupClickHouseStatsTest(t)
	defer cleanupCH()
	ctx := context.Background()
	require.NoError(t, migrate.ApplyClickHouseMigrations(ctx, conn))

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
	svc.SetClickHouse(conn)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	custID := uuid.New()
	require.NoError(t, svc.CreateCustomer(ctx, custID, "Forecast", 100_000_000, "USD"))

	start := time.Now().UTC().Truncate(time.Hour).Add(24 * time.Hour)
	end := start.Add(48 * time.Hour)
	body, _ := json.Marshal(map[string]any{
		"budget_limit_micro": int64(20_000_000),
		"pacing_mode":        "EVEN",
		"start_at":           start.Format(time.RFC3339),
		"end_at":             end.Format(time.RFC3339),
		"timezone":           "UTC",
	})

	var first map[string]any
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", "/api/v1/forecast/campaign", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		withSessionUser(req, tokenMaker, RoleUser, custID)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "attempt %d body=%s", i+1, rr.Body.String())
		var resp map[string]any
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		if i == 0 {
			first = resp
		} else {
			assert.Equal(t, first["impressions_p50"], resp["impressions_p50"])
			assert.Equal(t, first["impressions_p90"], resp["impressions_p90"])
			assert.Equal(t, first["low_confidence"], resp["low_confidence"])
		}
	}
}

// TestChaos_ForecastCHTimeout proves forecast ClickHouse timeouts return HTTP 503 with retry_after (M5.3).
func TestChaos_ForecastCHTimeout(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeForecastError(rec, ErrForecastClickHouseTimeout)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "30", rec.Header().Get("Retry-After"))
	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.EqualValues(t, 30, resp["retry_after"])
	errObj, ok := resp["error"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "FORECAST_UNAVAILABLE", errObj["code"])
}
