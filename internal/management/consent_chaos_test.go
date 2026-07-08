package management

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/database"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func signConsentBody(secret []byte, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// TestChaos_ConsentWebhookReplay proves signed consent ingestion accepts replayed webhooks.
func TestChaos_ConsentWebhookReplay(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	secret := []byte("consent-test-secret")
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{ConsentHMACSecret: config.Secret(secret)}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	h := NewHandler(svc, cfg, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body, _ := json.Marshal(ConsentRecordInput{
		UserID:   "replay-user",
		Purposes: ads.ConsentPurposeAdStorage,
		Source:   "cmp",
	})
	sig := signConsentBody(secret, body)

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest("POST", "/api/v1/consent", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Consent-Signature", sig)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		require.Equal(t, http.StatusNoContent, rr.Code, "attempt %d", i+1)
	}

	var count int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM consent_events`).Scan(&count))
	assert.Equal(t, 2, count)
}

// TestChaos_ConsentReadYourWrites proves consent reaches Redis within 2s (M6.3).
func TestChaos_ConsentReadYourWrites(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	ctx := context.Background()
	secret := []byte("consent-ryw-secret")
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	rdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		ConsentHMACSecret:    config.Secret(secret),
		ConsentUpdateChannel: "test:consent:update",
	}
	svc := newBareService(t, pool, []redis.UniversalClient{rdb}, cfg)
	store := ads.NewConsentStore(rdb)
	store.StartWatch(ctx, rdb, cfg.ConsentUpdateChannel)
	worker := NewOutboxWorker(svc)

	require.NoError(t, svc.RecordConsent(ctx, ConsentRecordInput{
		UserID:   "ryw-user",
		Purposes: ads.ConsentPurposeAdStorage | ads.ConsentPurposeAnalytics,
		Source:   "web",
	}))
	require.NoError(t, worker.ProcessOutbox(ctx))

	want := ads.ConsentPurposeAdStorage | ads.ConsentPurposeAnalytics
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if store.PurposesForUser("ryw-user") == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("chaos_proof fault=consent_read_your_writes consent not visible within 2s")
}

// TestChaos_ErasurePartialShardFailure proves erasure completes when one Redis shard is dead.
func TestChaos_ErasurePartialShardFailure(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos integration test")
	}
	ctx := context.Background()
	pool, cleanupDB := database.SetupTestDB(t)
	defer cleanupDB()
	okRdb, cleanupRedis := database.SetupTestRedis(t)
	defer cleanupRedis()
	badRdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})

	cfg := &config.Config{ConsentUpdateChannel: "test:erasure"}
	svc := newBareService(t, pool, []redis.UniversalClient{okRdb, badRdb}, cfg)

	userID := "erasure-user"
	hashHex := ads.HashUserIDHex(userID)
	require.NoError(t, okRdb.Set(ctx, ads.ConsentRedisKeyPrefix+hashHex, "3", 0).Err())

	reqID, err := svc.CreatePrivacyErasureRequest(ctx, userID)
	require.NoError(t, err)
	require.NoError(t, svc.ProcessPrivacyErasureTick(ctx))
	require.NoError(t, NewOutboxWorker(svc).ProcessOutbox(ctx))

	var status string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status::text FROM privacy_erasure_requests WHERE id = $1`, ads.ToUUID(reqID)).Scan(&status))
	assert.Equal(t, "REDIS_PURGED", status)
}
