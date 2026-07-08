package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads/db"
	"espx/internal/auth"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetShardHealth_reportsPingAndConfigVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, &config.Config{})

	require.NoError(t, svc.UpdateSettings(ctx, map[string]string{"emergency_breaker": "false"}))
	worker := NewOutboxWorker(svc)
	require.NoError(t, worker.ProcessOutbox(ctx))

	var lastProcessed int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(id), 0) FROM outbox_events WHERE status = 'PROCESSED'`,
	).Scan(&lastProcessed))
	require.Greater(t, lastProcessed, int64(0))

	version, err := rdb.Get(ctx, redisConfigVersionKey).Int64()
	require.NoError(t, err)
	require.Equal(t, lastProcessed, version)

	report, err := svc.GetShardHealth(ctx)
	require.NoError(t, err)
	require.Equal(t, "false", report.EmergencyBreaker)
	require.Equal(t, lastProcessed, report.Outbox.LastProcessedEventID)
	require.Len(t, report.Shards, 1)
	require.True(t, report.Shards[0].PingOK)
	require.NotNil(t, report.Shards[0].ConfigVersion)
	require.True(t, report.Shards[0].ConfigVersionSynced)
	require.Equal(t, int64(0), report.Shards[0].ConfigVersionLag)
}

func TestGetShardHealth_configVersionLag(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	ctx := context.Background()
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, &config.Config{})

	for i := 0; i < 2; i++ {
		_, err := db.New(pool).CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			EventType: "UPDATE_SETTINGS",
			Payload:   []byte(`{"settings":{"rate_limit_per_min":"100"}}`),
		})
		require.NoError(t, err)
	}
	worker := NewOutboxWorker(svc)
	require.NoError(t, worker.ProcessOutbox(ctx))

	var lastProcessed int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(id), 0) FROM outbox_events WHERE status = 'PROCESSED'`,
	).Scan(&lastProcessed))
	require.GreaterOrEqual(t, lastProcessed, int64(2))

	require.NoError(t, rdb.Set(ctx, redisConfigVersionKey, lastProcessed-1, 0).Err())

	report, err := svc.GetShardHealth(ctx)
	require.NoError(t, err)
	require.False(t, report.Shards[0].ConfigVersionSynced)
	require.Equal(t, int64(1), report.Shards[0].ConfigVersionLag)
}

func TestHandler_OpsShards_requiresPermShardsRead(t *testing.T) {
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

	req, _ := http.NewRequest("GET", "/admin/ops/shards", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusUnauthorized, resp.Code)

	req, _ = http.NewRequest("GET", "/admin/ops/shards", nil)
	req.Header.Set("X-Admin-API-Key", "test-secret")
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)

	var report ShardHealthReport
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&report))
	require.Len(t, report.Shards, 1)
}

func TestHandler_OpsShards_roleUserForbidden(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		TokenSymmetricKey: "01234567890123456789012345678901",
	}
	tokenMaker, err := auth.NewPasetoMaker(string(cfg.TokenSymmetricKey))
	require.NoError(t, err)

	authMdl := NewAuthMiddleware(tokenMaker, rdb, cfg, nil)
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, authMdl, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	token, err := tokenMaker.CreateToken(uuid.New(), uuid.New(), "user", uuid.New(), time.Hour)
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", "/admin/ops/shards", nil)
	req.AddCookie(&http.Cookie{Name: "accessToken", Value: token})
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusForbidden, resp.Code)
}
