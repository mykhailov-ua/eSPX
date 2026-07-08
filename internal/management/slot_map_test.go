package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/auth"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotMapAPI_RBAC(t *testing.T) {
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
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	svc := NewService(pool, []redis.UniversalClient{rdb}, nil, cfg)
	defer svc.Close()

	authMW := NewAuthMiddleware(tokenMaker, rdb, cfg, nil)
	h := NewHandler(svc, cfg, authMW, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	managerToken, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), "manager", uuid.New(), time.Hour)
	require.NoError(t, err)

	t.Run("manager forbidden write", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"overrides": []any{}})
		req, _ := http.NewRequest("POST", "/admin/shards/slot-map/versions", bytes.NewReader(body))
		req.AddCookie(&http.Cookie{Name: "accessToken", Value: managerToken})
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusForbidden, resp.Code)
	})

	t.Run("admin can create version via API key", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"overrides": []map[string]any{
				{"slot": 5, "shard_id": 2, "state": "MIGRATING"},
			},
		})
		req, _ := http.NewRequest("POST", "/admin/shards/slot-map/versions", bytes.NewReader(body))
		req.Header.Set("X-Admin-API-Key", "test-secret")
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		require.Equal(t, http.StatusCreated, resp.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
		assert.Equal(t, float64(2), out["version"])
	})

	t.Run("admin audit log written", func(t *testing.T) {
		var count int64
		err := pool.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM admin_audit_log WHERE action = 'SLOT_MAP_VERSION_CREATED'",
		).Scan(&count)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, int64(1))
	})
}

func TestSlotMapAPI_markMigratingAndActivate(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{AdminAPIKey: "test-secret"}
	rdbs := []redis.UniversalClient{rdb, rdb, rdb, rdb}
	svc := NewService(pool, rdbs, nil, cfg)
	defer svc.Close()
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Create version 2
	createBody, _ := json.Marshal(map[string]any{"overrides": []any{}})
	req, _ := http.NewRequest("POST", "/admin/shards/slot-map/versions", bytes.NewReader(createBody))
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code)

	migrateBody, _ := json.Marshal(map[string]any{
		"slots":        []int16{100, 101},
		"target_shard": 3,
	})
	req, _ = http.NewRequest("POST", "/admin/shards/slot-map/versions/2/migrate", bytes.NewReader(migrateBody))
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	req, _ = http.NewRequest("POST", "/admin/shards/slot-map/versions/2/copy", nil)
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	req, _ = http.NewRequest("POST", "/admin/shards/slot-map/versions/2/activate", nil)
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var active int32
	err := pool.QueryRow(context.Background(), "SELECT active_version FROM redis_slot_map_meta WHERE id = 1").Scan(&active)
	require.NoError(t, err)
	assert.Equal(t, int32(2), active)
}

func TestHasPermission_shardsRBAC(t *testing.T) {
	assert.True(t, HasPermission(RoleAdmin, PermShardsWrite))
	assert.True(t, HasPermission(RoleAdmin, PermShardsRead))
	assert.False(t, HasPermission(RoleManager, PermShardsWrite))
	assert.False(t, HasPermission(RoleUser, PermShardsRead))
}
