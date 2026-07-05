package payment

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"espx/internal/ads"
	ads_db "espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/management"
	mgmt_pb "espx/internal/management/pb"
	"espx/internal/payment/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const paymentContainerStopTimeout = 10 * time.Second

type paymentChaosInfra struct {
	Pool           *pgxpool.Pool
	Redis          redis.UniversalClient
	PGContainer    *postgres.PostgresContainer
	RedisContainer testcontainers.Container
	Cfg            *config.Config
	MgmtSvc        *management.Service
	SettlementLis  net.Listener
	SettlementGRPC *grpc.Server
}

type seededPayment struct {
	CustomerID  uuid.UUID
	IntentID    uuid.UUID
	AmountMicro int64
	ProviderRef string
	OutboxID    int64
}

// setupPaymentChaosInfra boots Postgres, Redis, and a live settlement gRPC server for fault injection tests.
func setupPaymentChaosInfra(t *testing.T) (*paymentChaosInfra, func()) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("payment_chaos_db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("secure_password"),
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

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	baseDir := filepath.Join(filepath.Dir(filename), "..", "..")
	applyMigrations(t, pool, filepath.Join(baseDir, "internal/ads/migrations"))
	applyMigrations(t, pool, filepath.Join(baseDir, "internal/payment/migrations"))

	redisContainer, err := rediscontainer.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	endpoint, err := redisContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{endpoint}})
	require.NoError(t, rdb.Ping(ctx).Err())

	cfg := &config.Config{
		PaymentInternalToken:    "payment_chaos_token",
		SettlementInternalToken: "settlement_chaos_token",
		StripeWebhookSecret:     "stripe_chaos_wh_secret",
		MaxRetries:              3,
	}

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
	mgmt_pb.RegisterSettlementServiceServer(grpcServer, settleHandler)
	go func() { _ = grpcServer.Serve(lis) }()

	infra := &paymentChaosInfra{
		Pool:           pool,
		Redis:          rdb,
		PGContainer:    pgContainer,
		RedisContainer: redisContainer,
		Cfg:            cfg,
		MgmtSvc:        mgmtSvc,
		SettlementLis:  lis,
		SettlementGRPC: grpcServer,
	}

	cleanup := func() {
		grpcServer.Stop()
		_ = rdb.Close()
		pool.Close()
		_ = redisContainer.Terminate(ctx)
		_ = pgContainer.Terminate(ctx)
	}
	return infra, cleanup
}

// stopPaymentContainer pauses Postgres to simulate an unreachable database during outbox work.
func stopPaymentContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	timeout := paymentContainerStopTimeout
	require.NoError(t, c.Stop(context.Background(), &timeout))
}

// startPaymentContainer resumes a stopped Postgres container for recovery scenarios.
func startPaymentContainer(t *testing.T, c testcontainers.Container) {
	t.Helper()
	require.NoError(t, c.Start(context.Background()))
}

// refreshPGPool recreates the pgx pool and management service pool after Postgres restarts.
func (infra *paymentChaosInfra) refreshPGPool(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	infra.Pool.Close()
	connStr, err := infra.PGContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	infra.Pool = pool
	infra.MgmtSvc.SetPool(pool)
	require.Eventually(t, func() bool {
		return pool.Ping(ctx) == nil
	}, 30*time.Second, 200*time.Millisecond)
}

// restartSettlementGRPC rebinds settlement on the same port after grpc.Stop closes the listener.
func (infra *paymentChaosInfra) restartSettlementGRPC(t *testing.T) {
	t.Helper()
	addr := infra.SettlementLis.Addr().String()
	if infra.SettlementGRPC != nil {
		infra.SettlementGRPC.Stop()
	}
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	infra.SettlementLis = lis

	settleHandler := management.NewSettlementHandler(infra.MgmtSvc, infra.Cfg)
	grpcServer := grpc.NewServer()
	mgmt_pb.RegisterSettlementServiceServer(grpcServer, settleHandler)
	go func() { _ = grpcServer.Serve(lis) }()
	infra.SettlementGRPC = grpcServer

	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond)
}

// requirePaymentFaultActive blocks until the injected fault is observable, avoiding false negatives.
func requirePaymentFaultActive(t *testing.T, faultActive func() bool, msg string) {
	t.Helper()
	require.Eventually(t, faultActive, 10*time.Second, 100*time.Millisecond, msg)
}

// seedCustomer inserts a billing account row required before intent or settlement tests run.
func seedCustomer(t *testing.T, pool *pgxpool.Pool, customerID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	_, err := ads_db.New(pool).CreateCustomer(ctx, ads_db.CreateCustomerParams{
		ID:       ads.ToUUID(customerID),
		Name:     "chaos customer",
		Balance:  0,
		Currency: "USD",
	})
	require.NoError(t, err)
}

// seedSucceededIntentWithOutbox drives checkout plus webhook so chaos tests start with a pending outbox row.
func seedSucceededIntentWithOutbox(t *testing.T, infra *paymentChaosInfra, customerID uuid.UUID, amountMicro int64, idempotencyKey string) seededPayment {
	t.Helper()
	ctx := context.Background()
	seedCustomer(t, infra.Pool, customerID)

	svc := NewService(infra.Pool, NewMockProvider(), infra.Cfg)
	result, err := svc.CreatePaymentIntent(ctx, customerID, amountMicro, "USD", idempotencyKey, nil)
	require.NoError(t, err)
	intent := result.Intent

	providerRef := intent.ProviderRef.String
	payload := fmt.Sprintf(`{"id":"evt_%s","type":"payment_intent.succeeded","data":{"object":{"id":"%s","amount":%d}}}`,
		idempotencyKey, providerRef, amountMicro)
	err = svc.ProcessStripeWebhook(ctx, "evt_"+idempotencyKey, "payment_intent.succeeded", []byte(payload), providerRef, amountMicro, payload)
	require.NoError(t, err)

	outboxRows, err := db.New(infra.Pool).GetPendingOutboxEventsForUpdate(ctx, 10)
	require.NoError(t, err)
	require.Len(t, outboxRows, 1)

	return seededPayment{
		CustomerID:  customerID,
		IntentID:    uuid.UUID(intent.ID.Bytes),
		AmountMicro: amountMicro,
		ProviderRef: providerRef,
		OutboxID:    outboxRows[0].ID,
	}
}

// seedSettledIntent completes top-up settlement so refund chaos tests start from credited balance.
func seedSettledIntent(t *testing.T, infra *paymentChaosInfra, customerID uuid.UUID, amountMicro int64, idempotencyKey string) seededPayment {
	t.Helper()
	seed := seedSucceededIntentWithOutbox(t, infra, customerID, amountMicro, idempotencyKey)
	worker := newOutboxWorkerForChaos(infra)
	n, err := worker.ProcessOutbox(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "PROCESSED", paymentOutboxStatus(t, infra.Pool, seed.OutboxID))
	assertPaymentChaosInvariants(t, infra.Pool, seed, seed.AmountMicro, 1)
	return seed
}

// processRefundWebhook simulates a Stripe refund.created webhook for chaos and integration tests.
func processRefundWebhook(t *testing.T, pool *pgxpool.Pool, svc *Service, eventID string, providerRef string, refundID string, refundAmountMicro int64) int64 {
	t.Helper()
	stripeCents, err := MicroToStripeAmount(refundAmountMicro)
	require.NoError(t, err)
	payload := fmt.Sprintf(`{"id":"%s","type":"refund.created","data":{"object":{"id":"%s","amount":%d,"payment_intent":"%s","status":"succeeded"}}}`,
		eventID, refundID, stripeCents, providerRef)
	err = svc.ProcessStripeRefundWebhook(context.Background(), eventID, "refund.created", []byte(payload), refundID, providerRef, refundAmountMicro, "succeeded")
	require.NoError(t, err)

	var outboxID int64
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT id FROM payment.payment_outbox
		WHERE event_type = $1 AND status = 'PENDING'
		ORDER BY created_at DESC LIMIT 1`, OutboxEventReverseBalance).Scan(&outboxID))
	return outboxID
}

// ledgerRefundCountForIntent counts PAYMENT_REFUND rows tied to one intent.
func ledgerRefundCountForIntent(t *testing.T, pool *pgxpool.Pool, intentID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM balance_ledger
		WHERE payment_intent_id = $1 AND type = 'PAYMENT_REFUND'`, ads.ToUUID(intentID)).Scan(&n)
	require.NoError(t, err)
	return n
}

// assertPaymentRefundInvariants checks balance and refund ledger rows after a payback scenario.
func assertPaymentRefundInvariants(t *testing.T, pool *pgxpool.Pool, seed seededPayment, wantBalance int64, wantRefundRows int) {
	t.Helper()
	require.Equal(t, wantBalance, customerBalance(t, pool, seed.CustomerID))
	require.Equal(t, wantRefundRows, ledgerRefundCountForIntent(t, pool, seed.IntentID))
}

// processDisputeWebhook simulates Stripe charge.dispute.* webhooks for chaos tests.
func processDisputeWebhook(t *testing.T, pool *pgxpool.Pool, svc *Service, eventID, eventType, providerRef, disputeID string, amountMicro int64, stripeStatus string) {
	t.Helper()
	stripeCents, err := MicroToStripeAmount(amountMicro)
	require.NoError(t, err)
	payload := fmt.Sprintf(`{"id":"%s","type":"%s","data":{"object":{"id":"%s","amount":%d,"payment_intent":"%s","status":"%s"}}}`,
		eventID, eventType, disputeID, stripeCents, providerRef, stripeStatus)
	err = svc.ProcessStripeDisputeWebhook(context.Background(), eventID, eventType, []byte(payload), disputeID, providerRef, amountMicro, stripeStatus)
	require.NoError(t, err)
}

// latestOutboxIDByType returns the newest pending outbox row id for one event type.
func latestOutboxIDByType(t *testing.T, pool *pgxpool.Pool, eventType string) int64 {
	t.Helper()
	var outboxID int64
	err := pool.QueryRow(context.Background(), `
		SELECT id FROM payment.payment_outbox
		WHERE event_type = $1 AND status = 'PENDING'
		ORDER BY created_at DESC LIMIT 1`, eventType).Scan(&outboxID)
	require.NoError(t, err)
	return outboxID
}

// ledgerChargebackCountForIntent counts PAYMENT_CHARGEBACK rows for one intent.
func ledgerChargebackCountForIntent(t *testing.T, pool *pgxpool.Pool, intentID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM balance_ledger
		WHERE payment_intent_id = $1 AND type = 'PAYMENT_CHARGEBACK'`, ads.ToUUID(intentID)).Scan(&n)
	require.NoError(t, err)
	return n
}

// ledgerChargebackReversalCountForIntent counts PAYMENT_CHARGEBACK_REVERSAL rows for one intent.
func ledgerChargebackReversalCountForIntent(t *testing.T, pool *pgxpool.Pool, intentID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM balance_ledger
		WHERE payment_intent_id = $1 AND type = 'PAYMENT_CHARGEBACK_REVERSAL'`, ads.ToUUID(intentID)).Scan(&n)
	require.NoError(t, err)
	return n
}

// assertPaymentChargebackInvariants checks balance and chargeback ledger rows.
func assertPaymentChargebackInvariants(t *testing.T, pool *pgxpool.Pool, seed seededPayment, wantBalance int64, wantChargebackRows, wantReversalRows int) {
	t.Helper()
	require.Equal(t, wantBalance, customerBalance(t, pool, seed.CustomerID))
	require.Equal(t, wantChargebackRows, ledgerChargebackCountForIntent(t, pool, seed.IntentID))
	require.Equal(t, wantReversalRows, ledgerChargebackReversalCountForIntent(t, pool, seed.IntentID))
}

// customerBalance reads the ads customer row balance used as the money invariant baseline.
func customerBalance(t *testing.T, pool *pgxpool.Pool, customerID uuid.UUID) int64 {
	t.Helper()
	ctx := context.Background()
	cust, err := ads_db.New(pool).GetCustomerForUpdate(ctx, ads.ToUUID(customerID))
	require.NoError(t, err)
	return cust.Balance
}

// ledgerCountForIntent counts PAYMENT_TOPUP rows tied to one intent for double-credit detection.
func ledgerCountForIntent(t *testing.T, pool *pgxpool.Pool, intentID uuid.UUID) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM balance_ledger
		WHERE payment_intent_id = $1 AND type = 'PAYMENT_TOPUP'`, ads.ToUUID(intentID)).Scan(&n)
	require.NoError(t, err)
	return n
}

// paymentOutboxStatus reads the outbox row state for lease and settlement progress assertions.
func paymentOutboxStatus(t *testing.T, pool *pgxpool.Pool, outboxID int64) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(), `
		SELECT status FROM payment.payment_outbox WHERE id = $1`, outboxID).Scan(&status)
	require.NoError(t, err)
	return status
}

// assertPaymentChaosInvariants checks balance and ledger row count after a fault scenario.
func assertPaymentChaosInvariants(t *testing.T, pool *pgxpool.Pool, seed seededPayment, wantBalance int64, wantLedgerRows int) {
	t.Helper()
	require.Equal(t, wantBalance, customerBalance(t, pool, seed.CustomerID))
	require.Equal(t, wantLedgerRows, ledgerCountForIntent(t, pool, seed.IntentID))
}

// itoaPaymentChaos formats integers for chaos_proof log lines without strconv in every test.
func itoaPaymentChaos(n int) string {
	return strconv.Itoa(n)
}

// newOutboxWorkerForChaos builds an outbox worker pre-connected to the infra settlement server.
func newOutboxWorkerForChaos(infra *paymentChaosInfra) *OutboxWorker {
	w := NewOutboxWorker(infra.Pool, infra.Cfg)
	target := "127.0.0.1:" + infra.Cfg.SettlementServerPort
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		w.conn = conn
		w.client = mgmt_pb.NewSettlementServiceClient(conn)
	}
	return w
}
