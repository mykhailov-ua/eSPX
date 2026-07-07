package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateBidRequestAPI(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	valid26 := []byte(`{
	  "id": "req-1",
	  "imp": [{"id": "1", "banner": {"w": 300, "h": 250}}]
	}`)
	req, _ := http.NewRequest("POST", "/admin/rtb/validate-bid-request", bytes.NewReader(valid26))
	withAdminAPIKey(req, cfg)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	var result ads.OpenRTBValidationResultDTO
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.True(t, result.Valid)
	assert.Equal(t, "2.6", result.Version)

	invalid := []byte(`{"openrtb":{"request":{"id":"r","cur":["JPY"],"item":[{"id":"1"}]}}}`)
	req, _ = http.NewRequest("POST", "/admin/rtb/validate-bid-request", bytes.NewReader(invalid))
	withAdminAPIKey(req, cfg)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.False(t, result.Valid)
	assert.Equal(t, "3.0", result.Version)
	assert.NotEmpty(t, result.Errors)
}
