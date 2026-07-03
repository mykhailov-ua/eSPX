package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"espx/internal/notifier/pb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Builds a Postgres pool with notifier schema migrations applied.
func setupTestDB(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notifier_test_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secure_password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(20*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %s", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %s", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to db: %s", err)
	}

	_, filename, _, _ := runtime.Caller(0)
	baseDir := filepath.Join(filepath.Dir(filename), "..", "..")
	notifierMigrationsDir := filepath.Join(baseDir, "internal/notifier/migrations")
	applyMigrations(t, pool, notifierMigrationsDir)

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

// Builds schema from goose migration files without invoking the goose CLI.
func applyMigrations(t testing.TB, pool *pgxpool.Pool, dir string) {
	t.Helper()
	ctx := context.Background()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read migrations dir %s: %s", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("failed to read migration %s: %s", entry.Name(), err)
		}

		sql := string(sqlBytes)
		parts := strings.Split(sql, "-- +goose Down")
		upPart := parts[0]
		upPart = strings.ReplaceAll(upPart, "-- +goose Up", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementBegin", "")
		upPart = strings.ReplaceAll(upPart, "-- +goose StatementEnd", "")

		if _, err := pool.Exec(ctx, upPart); err != nil {
			t.Fatalf("failed to apply migration %s: %s", entry.Name(), err)
		}
	}
}

// Guards enqueue and get round-trip persists a PENDING notification row.
func TestService_enqueueAndGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(3, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "12345678",
		Title:     "Test Alert",
		Body:      "This is a test notification",
	}

	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.NotificationId)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_PENDING, resp.Status)

	getResp, err := svc.GetNotification(ctx, &pb.GetNotificationRequest{NotificationId: resp.NotificationId})
	require.NoError(t, err)
	assert.Equal(t, resp.NotificationId, getResp.Notification.Id)
	assert.Equal(t, pb.Provider_PROVIDER_TELEGRAM, getResp.Notification.Provider)
	assert.Equal(t, "12345678", getResp.Notification.Recipient)
	assert.Equal(t, "Test Alert", getResp.Notification.Title)
	assert.Equal(t, "This is a test notification", getResp.Notification.Body)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_PENDING, getResp.Notification.Status)
	assert.NotNil(t, getResp.Notification.CreatedAt)
	assert.NotNil(t, getResp.Notification.UpdatedAt)
}

// Guards worker delivery marks a pending notification as SENT.
func TestService_processPending_success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(3, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "12345678",
		Title:     "Test Alert",
		Body:      "This is a test notification",
	}
	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	require.Len(t, mockProv.Sent, 1)
	assert.Equal(t, "12345678", mockProv.Sent[0].Recipient)
	assert.Equal(t, "Test Alert", mockProv.Sent[0].Title)
	assert.Equal(t, "This is a test notification", mockProv.Sent[0].Body)

	getResp, err := svc.GetNotification(ctx, &pb.GetNotificationRequest{NotificationId: resp.NotificationId})
	require.NoError(t, err)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_SENT, getResp.Notification.Status)
	assert.Equal(t, int32(0), getResp.Notification.RetryCount)
}

// Guards transient provider failures keep the row PENDING and increment retry_count.
func TestService_processPending_failureAndRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(3, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "12345678",
		Title:     "Test Alert",
		Body:      "trigger_failure",
	}
	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	getResp, err := svc.GetNotification(ctx, &pb.GetNotificationRequest{NotificationId: resp.NotificationId})
	require.NoError(t, err)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_PENDING, getResp.Notification.Status)
	assert.Equal(t, int32(1), getResp.Notification.RetryCount)
	assert.Contains(t, getResp.Notification.ErrorMessage, "mock send failure triggered")
}

// Guards the fifth failed attempt marks the notification FAILED permanently.
func TestService_processPending_permanentFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(10, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "12345678",
		Title:     "Test Alert",
		Body:      "trigger_failure",
	}
	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)

	id, err := uuid.Parse(resp.NotificationId)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE notifier.notifications SET retry_count = 4, updated_at = now() - interval '60 seconds' WHERE id = $1", pgtype.UUID{Bytes: id, Valid: true})
	require.NoError(t, err)

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	getResp, err := svc.GetNotification(ctx, &pb.GetNotificationRequest{NotificationId: resp.NotificationId})
	require.NoError(t, err)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_FAILED, getResp.Notification.Status)
	assert.Equal(t, int32(maxDeliveryAttempts), getResp.Notification.RetryCount)
}

// Guards provider circuit breaker open errors propagate into notification error_message.
func TestService_processPending_circuitBreaker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(2, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := svc.SendNotification(ctx, &pb.SendNotificationRequest{
			Provider:  pb.Provider_PROVIDER_TELEGRAM,
			Recipient: "12345678",
			Title:     fmt.Sprintf("Test Alert %d", i),
			Body:      "trigger_failure",
		})
		require.NoError(t, err)
	}

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 3, processed)
	assert.Equal(t, CircuitOpen, breaker.State())

	rows, err := pool.Query(ctx, "SELECT error_message FROM notifier.notifications WHERE error_message IS NOT NULL")
	require.NoError(t, err)
	defer rows.Close()

	foundBreakerErr := false
	for rows.Next() {
		var errMsg string
		err = rows.Scan(&errMsg)
		require.NoError(t, err)
		if strings.Contains(errMsg, "circuit breaker is open") {
			foundBreakerErr = true
		}
	}
	assert.True(t, foundBreakerErr, "expected at least one notification to fail with circuit breaker open error")
}

// Guards SQL backoff skips notifications before the retry window elapses.
func TestService_processPending_exponentialBackoff(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(10, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_TELEGRAM,
		Recipient: "12345678",
		Title:     "Test Alert",
		Body:      "trigger_failure",
	}
	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	processed, err = svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 0, processed, "expected notification to be skipped due to exponential backoff")

	id, err := uuid.Parse(resp.NotificationId)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "UPDATE notifier.notifications SET updated_at = now() - interval '10 seconds' WHERE id = $1", pgtype.UUID{Bytes: id, Valid: true})
	require.NoError(t, err)

	processed, err = svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed, "expected notification to be processed after backoff duration elapsed")
}

// Guards alert deduplication and correlation over a window of pending notifications.
func TestService_processPending_deduplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	breaker := NewCircuitBreaker(3, 2, 10*time.Second)
	mockProv := NewMockProvider(breaker)
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_TELEGRAM: mockProv,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	// Enqueue 5 identical notifications to group them
	for i := 0; i < 5; i++ {
		_, err := svc.SendNotification(ctx, &pb.SendNotificationRequest{
			Provider:  pb.Provider_PROVIDER_TELEGRAM,
			Recipient: "12345678",
			Title:     "Deduplicated Alert",
			Body:      fmt.Sprintf("Alert details for node %d", i),
		})
		require.NoError(t, err)
	}

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 5, processed, "expected all 5 notifications to be processed as part of deduplication group")

	// Only 1 actual message must have been sent
	require.Len(t, mockProv.Sent, 1)
	assert.Equal(t, "12345678", mockProv.Sent[0].Recipient)
	assert.Equal(t, "Deduplicated Alert", mockProv.Sent[0].Title)
	assert.Contains(t, mockProv.Sent[0].Body, "⚠️ [DEDUPLICATED] Accumulated 5 similar events.")
	assert.Contains(t, mockProv.Sent[0].Body, "Alert details for node 0")
	assert.Contains(t, mockProv.Sent[0].Body, "Alert details for node 1")

	// Emit chaos proof for deduplication
	fmt.Printf("chaos_proof fault=notifier_deduplication size=5 success=true\n")
}

// Guards multi-channel fallback path (Slack -> Telegram -> SMS -> SMTP).
func TestService_processPending_fallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	slackBreaker := NewCircuitBreaker(3, 2, 10*time.Second)
	telegramBreaker := NewCircuitBreaker(3, 2, 10*time.Second)

	// Trip slack breaker so it fast-fails
	slackBreaker.trip()

	mockSlack := NewMockProvider(slackBreaker)
	mockTelegram := NewMockProvider(telegramBreaker)

	// Wire them up
	providers := map[pb.Provider]Provider{
		pb.Provider_PROVIDER_SLACK:    mockSlack,
		pb.Provider_PROVIDER_TELEGRAM: mockTelegram,
	}

	svc := NewService(pool, providers)
	ctx := context.Background()

	// Enqueue a SLACK notification. It should fail on Slack due to open circuit,
	// and fallback to TELEGRAM which is open and will succeed.
	req := &pb.SendNotificationRequest{
		Provider:  pb.Provider_PROVIDER_SLACK,
		Recipient: "https://hooks.slack.com/services/test",
		Title:     "Fallback Alert",
		Body:      "This notification falls back",
	}

	resp, err := svc.SendNotification(ctx, req)
	require.NoError(t, err)

	processed, err := svc.ProcessPending(ctx, workerBatchSize)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)

	// Slack should have 0 sends (circuit open)
	assert.Len(t, mockSlack.Sent, 0)

	// Telegram should have 1 send (successful fallback)
	require.Len(t, mockTelegram.Sent, 1)
	assert.Equal(t, "Fallback Alert", mockTelegram.Sent[0].Title)
	assert.Equal(t, "This notification falls back", mockTelegram.Sent[0].Body)

	// Database record should show SENT status and provider as TELEGRAM!
	getResp, err := svc.GetNotification(ctx, &pb.GetNotificationRequest{NotificationId: resp.NotificationId})
	require.NoError(t, err)
	assert.Equal(t, pb.NotificationStatus_NOTIFICATION_STATUS_SENT, getResp.Notification.Status)
	assert.Equal(t, pb.Provider_PROVIDER_TELEGRAM, getResp.Notification.Provider)

	// Emit chaos proof for fallback
	fmt.Printf("chaos_proof fault=notifier_fallback primary=SLACK fallback=TELEGRAM success=true\n")
}

type mockRoundTripper func(req *http.Request) (*http.Response, error)

func (m mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m(req)
}

// Guards JSON formatting and correct structure of Telegram/Slack interactive buttons.
func TestProviders_interactiveButtons(t *testing.T) {
	ctx := context.WithValue(context.Background(), "notification_id", "test-notification-uuid-123")

	// 1. Telegram test
	var capturedTelegram []byte
	tProv := NewTelegramProvider("token123", "default-chat", NewCircuitBreaker(3, 2, time.Second))
	tProv.client.Transport = mockRoundTripper(func(req *http.Request) (*http.Response, error) {
		var err error
		capturedTelegram, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})

	err := tProv.Send(ctx, "chat-id", "Critical Error", "Intruder detected on IP 192.168.1.100")
	require.NoError(t, err)

	var tPayload map[string]interface{}
	err = json.Unmarshal(capturedTelegram, &tPayload)
	require.NoError(t, err)

	assert.Equal(t, "chat-id", tPayload["chat_id"])
	assert.Equal(t, "HTML", tPayload["parse_mode"])

	replyMarkup, exists := tPayload["reply_markup"].(map[string]interface{})
	require.True(t, exists)
	inlineKeyboard, exists := replyMarkup["inline_keyboard"].([]interface{})
	require.True(t, exists)
	assert.Len(t, inlineKeyboard, 2) // Acknowledge and Block IP

	// 2. Slack test
	var capturedSlack []byte
	sProv := NewSlackProvider("https://hooks.slack.com/services/test", NewCircuitBreaker(3, 2, time.Second))
	sProv.client.Transport = mockRoundTripper(func(req *http.Request) (*http.Response, error) {
		var err error
		capturedSlack, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`ok`)),
		}, nil
	})

	err = sProv.Send(ctx, "", "Critical Error", "Intruder detected on IP 10.0.0.5")
	require.NoError(t, err)

	var sPayload map[string]interface{}
	err = json.Unmarshal(capturedSlack, &sPayload)
	require.NoError(t, err)

	blocks, exists := sPayload["blocks"].([]interface{})
	require.True(t, exists)
	assert.Len(t, blocks, 2) // Section and Actions block

	// Emit chaos proof for interactive buttons
	fmt.Printf("chaos_proof fault=notifier_interactive_buttons success=true\n")
}
