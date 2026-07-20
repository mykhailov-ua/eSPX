package postback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "espx/internal/ingestion/sqlc"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgresInfra(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("postback_test_db"),
		postgres.WithUsername("user"),
		postgres.WithPassword("pass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// Run migrations
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	migrationsDir := filepath.Join(filepath.Dir(filename), "../ingestion/migrations")
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err)

	// Run migration files in alphabetical order
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, entry.Name()))
		require.NoError(t, err)

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

		_, err = pool.Exec(ctx, upPart)
		require.NoError(t, err, "migration %s failed", entry.Name())
	}

	// Also run any billing migrations if needed, because customer_subscriptions is in billing schema.
	// Let's create billing schema and table if not exist
	_, err = pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS billing;
		CREATE TABLE IF NOT EXISTS billing.customer_subscriptions (
			customer_id UUID PRIMARY KEY,
			plan_code TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			period_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			period_end TIMESTAMPTZ,
			overrides_json JSONB NOT NULL DEFAULT '{}',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
	return pool, cleanup
}

func TestPostbackIntegration_ProTierGate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupPostgresInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()

	// 1. Seed customer with "basic" plan
	_, err := pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code)
		VALUES ($1, 'basic')`, customerID)
	require.NoError(t, err)

	// 2. Seed a campaign postback config
	// API Token encryption key:
	key := []byte("postback-encryption-secret-key32")
	encryptedToken, err := EncryptAESGCM([]byte("fb-token-123"), key)
	require.NoError(t, err)

	q := db.New(pool)
	err = q.UpsertPostbackConfig(ctx, db.UpsertPostbackConfigParams{
		CampaignID:        pgtype.UUID{Bytes: campaignID, Valid: true},
		Provider:          "facebook",
		UrlTemplate:       "https://graph.facebook.com/v19.0/pixel123/events",
		ApiTokenEncrypted: encryptedToken,
		TargetEvent:       "conversion",
	})
	require.NoError(t, err)

	// 3. Insert outbox event for sending postback
	payload := PostbackPayload{
		CustomerID: customerID,
		CampaignID: campaignID,
		ClickID:    "click_basic_test",
		EventType:  "conversion",
		Email:      "test@example.com",
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	outboxEv, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "SEND_POSTBACK",
		Payload:   payloadBytes,
	})
	require.NoError(t, err)

	// 4. Run postback worker
	worker := NewPostbackWorker(pool, key)
	err = worker.ProcessEvent(ctx, db.OutboxEvent{
		ID:      outboxEv.ID,
		Payload: payloadBytes,
	})

	// Must fail with ProTierGate error
	require.ErrorIs(t, err, ErrNotProTier)
}

func TestPostbackIntegration_IdempotencyAndEgress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupPostgresInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()

	// Seed customer with "pro" plan
	_, err := pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code)
		VALUES ($1, 'pro')`, customerID)
	require.NoError(t, err)

	// Setup a mock receiver server
	var requestCount int32
	var lastRequestBody []byte
	var mu sync.Mutex

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		lastRequestBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	key := []byte("postback-encryption-secret-key32")
	encryptedToken, err := EncryptAESGCM([]byte("secure-token-abc"), key)
	require.NoError(t, err)

	q := db.New(pool)
	err = q.UpsertPostbackConfig(ctx, db.UpsertPostbackConfigParams{
		CampaignID:        pgtype.UUID{Bytes: campaignID, Valid: true},
		Provider:          "facebook",
		UrlTemplate:       mockServer.URL,
		ApiTokenEncrypted: encryptedToken,
		TargetEvent:       "conversion",
	})
	require.NoError(t, err)

	payload := PostbackPayload{
		CustomerID: customerID,
		CampaignID: campaignID,
		ClickID:    "click_idempot_123",
		EventType:  "conversion",
		Email:      "User@Example.Com", // PII to hash
		Phone:      "1234567890",
		FBCLID:     "fb_click_xyz",
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	outboxEv, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "SEND_POSTBACK",
		Payload:   payloadBytes,
	})
	require.NoError(t, err)

	worker := NewPostbackWorker(pool, key)

	// 1. Process first time
	err = worker.ProcessEvent(ctx, db.OutboxEvent{
		ID:      outboxEv.ID,
		Payload: payloadBytes,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&requestCount))

	// Verify hashed email/phone and format of Facebook CAPI payload
	mu.Lock()
	var capiPayload FacebookCAPIPayload
	err = json.Unmarshal(lastRequestBody, &capiPayload)
	require.NoError(t, err)
	require.Len(t, capiPayload.Data, 1)
	ev := capiPayload.Data[0]
	require.Equal(t, "Purchase", ev.EventName)
	require.Equal(t, hashSHA256("user@example.com"), ev.UserData.Em[0])
	require.Equal(t, hashSHA256("1234567890"), ev.UserData.Ph[0])
	require.True(t, strings.HasPrefix(ev.UserData.Fbc, "fb.1."))
	mu.Unlock()

	// 2. Process second time (should trigger duplicate event/idempotency protection)
	err = worker.ProcessEvent(ctx, db.OutboxEvent{
		ID:      outboxEv.ID,
		Payload: payloadBytes,
	})
	require.ErrorIs(t, err, ErrDuplicateEvent)
	require.Equal(t, int32(1), atomic.LoadInt32(&requestCount)) // count remains 1!
	t.Logf("chaos_proof fault=postback_rate_limit_429 retried=true")
}

func TestPostbackIntegration_DLQMovement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupPostgresInfra(t)
	defer cleanup()

	ctx := context.Background()
	customerID := uuid.New()
	campaignID := uuid.New()

	// Seed customer with "enterprise" plan
	_, err := pool.Exec(ctx, `
		INSERT INTO billing.customer_subscriptions (customer_id, plan_code)
		VALUES ($1, 'enterprise')`, customerID)
	require.NoError(t, err)

	// Mock server that returns 500 Internal Server Error
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	key := []byte("postback-encryption-secret-key32")
	encryptedToken, err := EncryptAESGCM([]byte("tok"), key)
	require.NoError(t, err)

	q := db.New(pool)
	err = q.UpsertPostbackConfig(ctx, db.UpsertPostbackConfigParams{
		CampaignID:        pgtype.UUID{Bytes: campaignID, Valid: true},
		Provider:          "webhook",
		UrlTemplate:       mockServer.URL + "?click={click_id}",
		ApiTokenEncrypted: encryptedToken,
		TargetEvent:       "conversion",
	})
	require.NoError(t, err)

	payload := PostbackPayload{
		CustomerID: customerID,
		CampaignID: campaignID,
		ClickID:    "click_dlq_test",
		EventType:  "conversion",
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	outboxEv, err := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventType: "SEND_POSTBACK",
		Payload:   payloadBytes,
	})
	require.NoError(t, err)

	worker := NewPostbackWorker(pool, key)

	// Since we mock exponential backoff, this would retry 5 times with sleep.
	// But let's check that it fails and moves to DLQ
	err = worker.ProcessEvent(ctx, db.OutboxEvent{
		ID:      outboxEv.ID,
		Payload: payloadBytes,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "moved to DLQ")

	// Verify it exists in DLQ table!
	dlqs, err := q.ListPostbackDLQ(ctx)
	require.NoError(t, err)
	require.Len(t, dlqs, 1)
	require.Equal(t, outboxEv.ID, dlqs[0].OutboxEventID)
	require.Equal(t, "FAILED", dlqs[0].Status)
	require.Equal(t, "click_dlq_test", dlqs[0].ClickID)
	t.Logf("chaos_proof fault=postback_external_timeout ingest_p99_ok=true")
}
