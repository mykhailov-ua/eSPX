package payment

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"espx/internal/ads"
	ads_db "espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/management"
	"espx/internal/management/pb"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
)

// setupTestDB provisions Postgres with ads and payment schemas for end-to-end settlement tests.
func setupTestDB(t testing.TB) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("payment_test_db"),
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

	adsMigrationsDir := filepath.Join(baseDir, "internal/ads/migrations")
	applyMigrations(t, pool, adsMigrationsDir)

	paymentMigrationsDir := filepath.Join(baseDir, "internal/payment/migrations")
	applyMigrations(t, pool, paymentMigrationsDir)

	return pool, func() {
		pool.Close()
		_ = pgContainer.Terminate(ctx)
	}
}

// applyMigrations runs goose Up sections in filename order without pulling goose as a test dependency.
func applyMigrations(t testing.TB, pool *pgxpool.Pool, dir string) {
	t.Helper()
	ctx := context.Background()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read migrations dir %s: %s", dir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

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

// setupTestRedis provides the Redis shard management needs for settlement side effects.
func setupTestRedis(t testing.TB) (redis.UniversalClient, func()) {
	ctx := context.Background()

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get redis endpoint: %s", err)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{endpoint},
	})

	return rdb, func() {
		_ = rdb.Close()
		_ = redisContainer.Terminate(ctx)
	}
}

// TestPaymentService_Integration covers intent creation, webhook ingestion, outbox delivery, and ledger credit.
// Requires a customer row in ads.customers because settlement credits the management ledger.
func TestPaymentService_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers integration test in short mode")
	}

	pool, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	rdb, cleanupRedis := setupTestRedis(t)
	defer cleanupRedis()

	cfg := &config.Config{
		PaymentInternalToken:    "payment_secret_token",
		SettlementInternalToken: "settlement_secret_token",
		StripeWebhookSecret:     "stripe_wh_secret",
		MaxRetries:              3,
	}
	cfg.Lifecycle.ShutdownTimeoutMs = 1000

	prov := NewMockProvider()
	svc := NewService(pool, prov, cfg)

	ctx := context.Background()

	customerID := uuid.New()
	qAds := ads_db.New(pool)
	_, err := qAds.CreateCustomer(ctx, ads_db.CreateCustomerParams{
		ID:       ads.ToUUID(customerID),
		Name:     "Test Payment Customer",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)

	idempotencyKey := "idempotency_key_test_123"
	amountMicro := int64(50000000)

	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", idempotencyKey, map[string]string{"foo": "bar"})
	require.NoError(t, err)
	intent := result.Intent
	assert.Equal(t, db.PaymentPaymentIntentStatusPENDINGPROVIDER, intent.Status)
	assert.Equal(t, amountMicro, intent.AmountMicro)
	assert.NotEmpty(t, result.CheckoutURL)

	result2, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", idempotencyKey, map[string]string{"foo": "bar"})
	require.NoError(t, err)
	intent2 := result2.Intent
	assert.Equal(t, intent.ID, intent2.ID)

	_, err = svc.CreatePaymentIntent(ctx, customerID, amountMicro+10, "USD", idempotencyKey, map[string]string{"foo": "bar"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "idempotency key conflict")

	providerRef := intent.ProviderRef.String
	stripeCents, err := MicroToStripeAmount(amountMicro)
	require.NoError(t, err)
	stripePayload := fmt.Sprintf(`{
		"id": "evt_stripe_test_999",
		"type": "payment_intent.succeeded",
		"data": {
			"object": {
				"id": "%s",
				"amount": %d
			}
		}
	}`, providerRef, stripeCents)

	err = svc.ProcessStripeWebhook(ctx, "evt_stripe_test_999", "payment_intent.succeeded", []byte(stripePayload), providerRef, amountMicro, stripePayload)
	require.NoError(t, err)

	intentUpdated, err := svc.GetPaymentIntent(ctx, uuid.UUID(intent.ID.Bytes))
	require.NoError(t, err)
	assert.Equal(t, db.PaymentPaymentIntentStatusSUCCEEDED, intentUpdated.Status)

	qPayment := db.New(pool)
	outboxEvents, err := qPayment.GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	require.Len(t, outboxEvents, 1)
	assert.Equal(t, "SETTLE_BALANCE", outboxEvents[0].EventType)

	rdbs := []redis.UniversalClient{rdb}
	mgmtSvc := management.NewService(pool, rdbs, ads.NewStaticSlotSharder(len(rdbs)), cfg)
	settleHandler := management.NewSettlementHandler(mgmtSvc, cfg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	cfg.SettlementServerHost = "127.0.0.1"
	cfg.SettlementServerPort = portStr

	grpcServer := grpc.NewServer()
	pb.RegisterSettlementServiceServer(grpcServer, settleHandler)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	outboxWorker := NewOutboxWorker(pool, cfg)
	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	go outboxWorker.Start(ctxCancel, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		events, err := db.New(pool).GetPendingOutboxEventsForUpdate(ctx, 10)
		return err == nil && len(events) == 0
	}, 5*time.Second, 100*time.Millisecond)

	customer, err := qAds.GetCustomerForUpdate(ctx, ads.ToUUID(customerID))
	require.NoError(t, err)
	assert.Equal(t, amountMicro, customer.Balance)

	ledgerRows, err := qAds.ListCustomerLedger(ctx, ads_db.ListCustomerLedgerParams{
		CustomerID: ads.ToUUID(customerID),
		Limit:      10,
		Offset:     0,
	})
	require.NoError(t, err)
	require.Len(t, ledgerRows, 1)
	assert.Equal(t, amountMicro, ledgerRows[0].Amount)
	assert.Equal(t, ads_db.LedgerType("PAYMENT_TOPUP"), ledgerRows[0].Type)
	assert.Equal(t, "payment:"+uuid.UUID(intent.ID.Bytes).String(), ledgerRows[0].IdempotencyHash.String)
	assert.Equal(t, ads.ToUUID(uuid.UUID(intent.ID.Bytes)), ledgerRows[0].PaymentIntentID)
}
