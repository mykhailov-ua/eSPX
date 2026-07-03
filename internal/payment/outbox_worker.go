package payment

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"espx/internal/config"
	"espx/internal/management/pb"
	"espx/internal/payment/db"
	"espx/pkg/cold"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// PostSettlementMarkHook runs after a successful settlement gRPC call and before the outbox row
// is marked PROCESSED. Used by chaos tests to simulate the credit/mark-processed gap.
var PostSettlementMarkHook func(ctx context.Context, ev db.PaymentPaymentOutbox) error

// OutboxWorker delivers SETTLE_BALANCE events to management without blocking webhook HTTP responses.
type OutboxWorker struct {
	pool *pgxpool.Pool
	cfg  *config.Config

	clientMu sync.Mutex
	client   pb.SettlementServiceClient
	conn     *grpc.ClientConn

	wg sync.WaitGroup
}

// NewOutboxWorker decouples ledger credit from webhook commit latency via async settlement delivery.
func NewOutboxWorker(pool *pgxpool.Pool, cfg *config.Config) *OutboxWorker {
	return &OutboxWorker{
		pool: pool,
		cfg:  cfg,
	}
}

// SettleBalancePayload is the outbox JSON contract shared with management ApplyPaymentCredit.
type SettleBalancePayload struct {
	CustomerID           string `json:"customer_id"`
	AmountMicro          int64  `json:"amount_micro"`
	LedgerIdempotencyKey string `json:"ledger_idempotency_key"`
	PaymentIntentID      string `json:"payment_intent_id"`
	Provider             string `json:"provider"`
	ProviderRef          string `json:"provider_ref"`
}

// Start polls the outbox until shutdown because settlement gRPC may be temporarily unreachable at boot.
func (worker *OutboxWorker) Start(ctx context.Context, interval time.Duration) {
	worker.wg.Add(1)
	defer worker.wg.Done()

	if err := worker.ensureSettlementClient(); err != nil {
		slog.Error("outbox worker failed to connect to management settlement server", "error", err)
	}

	slog.Info("payment outbox worker starting polling loop", "interval", interval)

	pollTimer := time.NewTimer(interval)
	defer pollTimer.Stop()

	recoveryTicker := time.NewTicker(interval * 5)
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			worker.resetSettlementClient()
			return
		case <-recoveryTicker.C:
			worker.reclaimStaleProcessing(ctx)
		case <-pollTimer.C:
			worker.refreshOutboxPendingGauge(ctx)
			processed, err := worker.ProcessOutbox(ctx, 100)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("payment outbox processing iteration failed, retrying in 2s", "error", err)
				pollTimer.Reset(2 * time.Second)
				continue
			}

			if processed > 0 {
				pollTimer.Reset(0)
				continue
			}

			pollTimer.Reset(interval)
		}
	}
}

// Wait blocks shutdown until the poll loop exits so in-flight settlements are not cut off mid-batch.
func (w *OutboxWorker) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// refreshOutboxPendingGauge exposes backlog depth for alerting when settlement falls behind webhooks.
func (w *OutboxWorker) refreshOutboxPendingGauge(ctx context.Context) {
	var pending int64
	err := w.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox
		WHERE status IN ('PENDING', 'PROCESSING')`).Scan(&pending)
	if err == nil {
		OutboxPending.Set(float64(pending))
	}
}

// reclaimStaleProcessing recovers rows leased by crashed workers so settlement does not stall indefinitely.
func (w *OutboxWorker) reclaimStaleProcessing(ctx context.Context) {
	_, err := w.pool.Exec(ctx, `
		UPDATE payment.payment_outbox
		SET status = 'PENDING', lease_until = NULL
		WHERE status = 'PROCESSING'
		  AND lease_until IS NOT NULL
		  AND lease_until < now()`)
	if err != nil && ctx.Err() == nil && !strings.Contains(err.Error(), "closed pool") {
		slog.Error("failed to reclaim stale payment outbox events", "error", err)
	}
}

// ProcessOutbox leases a batch before gRPC calls so duplicate workers cannot credit the same event twice.
func (worker *OutboxWorker) ProcessOutbox(ctx context.Context, limit int32) (int, error) {
	if err := worker.ensureSettlementClient(); err != nil {
		return 0, err
	}

	var events []db.PaymentPaymentOutbox
	leaseDuration := 30 * time.Second
	leaseUntil := time.Now().Add(leaseDuration)

	err := pgx.BeginFunc(ctx, worker.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)
		var err error
		events, err = txQueries.GetPendingOutboxEventsForUpdate(ctx, limit)
		if err != nil || len(events) == 0 {
			return err
		}

		ids := make([]int64, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}

		err = txQueries.LeaseOutboxEvents(ctx, db.LeaseOutboxEventsParams{
			Column1:    ids,
			LeaseUntil: pgtype.Timestamptz{Time: leaseUntil, Valid: true},
		})
		return err
	})

	if err != nil || len(events) == 0 {
		return 0, err
	}

	successCount := 0
	for _, ev := range events {
		if err := worker.handleOutboxEvent(ctx, ev); err != nil {
			slog.Error("failed to handle outbox event", "id", ev.ID, "error", err)
			SettlementErrorsTotal.Inc()
			if isSettlementTransientGRPC(err) {
				worker.resetSettlementClient()
			}
			worker.markOutboxEventRetryable(ctx, ev, err)
			continue
		}
		if PostSettlementMarkHook != nil {
			if hookErr := PostSettlementMarkHook(ctx, ev); hookErr != nil {
				slog.Error("post-settlement hook failed", "id", ev.ID, "error", hookErr)
				SettlementErrorsTotal.Inc()
				worker.markOutboxEventRetryable(ctx, ev, hookErr)
				continue
			}
		}
		if err := worker.markOutboxProcessedWithRetry(ctx, ev.ID); err != nil {
			slog.Error("failed to mark outbox event processed", "id", ev.ID, "error", err)
			continue
		}
		successCount++
	}

	worker.refreshOutboxPendingGauge(ctx)
	return successCount, nil
}

// ensureSettlementClient dials management lazily because payment may start before settlement gRPC is listening.
func (w *OutboxWorker) ensureSettlementClient() error {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()

	if w.client != nil {
		return nil
	}
	target := "127.0.0.1:" + w.cfg.SettlementServerPort
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("gRPC client not connected to %s: %w", target, err)
	}
	w.conn = conn
	w.client = pb.NewSettlementServiceClient(conn)
	return nil
}

// resetSettlementClient drops a dead connection so the next batch opens a fresh settlement channel.
func (w *OutboxWorker) resetSettlementClient() {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()

	if w.conn != nil {
		_ = w.conn.Close()
	}
	w.conn = nil
	w.client = nil
}

// getSettlementClient returns the cached client under lock because gRPC conn is shared across poll iterations.
func (w *OutboxWorker) getSettlementClient() pb.SettlementServiceClient {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	return w.client
}

// isSettlementTransientGRPC distinguishes retryable transport errors from permanent settlement rejection.
func isSettlementTransientGRPC(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "transport is closing")
}

// markOutboxProcessedWithRetry tolerates brief DB blips after ledger credit already succeeded.
func (w *OutboxWorker) markOutboxProcessedWithRetry(ctx context.Context, outboxID int64) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		lastErr = db.New(w.pool).MarkOutboxEventProcessed(ctx, outboxID)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return lastErr
		}
		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
	}
	return lastErr
}

// markOutboxEventRetryable records retry or terminal failure; NotFound also marks the intent SETTLEMENT_FAILED
// so ops can see money collected but ledger credit rejected.
func (worker *OutboxWorker) markOutboxEventRetryable(ctx context.Context, ev db.PaymentPaymentOutbox, cause error) {
	var lastErrText pgtype.Text
	lastErrText.String = cause.Error()
	lastErrText.Valid = true

	isFatal := false
	st, ok := status.FromError(cause)
	if ok && st.Code() == codes.NotFound {
		isFatal = true
	}

	innerErr := pgx.BeginFunc(ctx, worker.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)
		if isFatal {
			if err := txQueries.MarkOutboxEventFailed(ctx, db.MarkOutboxEventFailedParams{
				ID:        ev.ID,
				Attempts:  0,
				LastError: lastErrText,
			}); err != nil {
				return err
			}

			payload := cold.UnmarshalLenient[SettleBalancePayload](ev.Payload)
			if payload.PaymentIntentID != "" {
				intentUUID, _ := uuid.Parse(payload.PaymentIntentID)
				_, _ = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
					ID:          pgtype.UUID{Bytes: intentUUID, Valid: true},
					Status:      db.PaymentPaymentIntentStatusSETTLEMENTFAILED,
					ProviderRef: pgtype.Text{String: payload.ProviderRef, Valid: true},
				})
			}
		} else {
			maxAttempts := int32(worker.cfg.MaxRetries)
			if maxAttempts <= 0 {
				maxAttempts = 5
			}
			if err := txQueries.MarkOutboxEventFailed(ctx, db.MarkOutboxEventFailedParams{
				ID:        ev.ID,
				Attempts:  maxAttempts,
				LastError: lastErrText,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if innerErr != nil {
		slog.Error("failed to update outbox event failure status", "id", ev.ID, "error", innerErr)
	}
}

// handleOutboxEvent forwards one row to management; ledger idempotency key prevents double credit on retry.
func (worker *OutboxWorker) handleOutboxEvent(ctx context.Context, ev db.PaymentPaymentOutbox) error {
	if ev.EventType != "SETTLE_BALANCE" {
		slog.Warn("skipping unrecognized payment outbox event type", "type", ev.EventType)
		return nil
	}

	payload, err := cold.UnmarshalStrict[SettleBalancePayload](ev.Payload)
	if err != nil {
		return err
	}

	client := worker.getSettlementClient()
	if client == nil {
		return fmt.Errorf("settlement client not connected")
	}

	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(worker.cfg.SettlementInternalToken))

	_, err = client.ApplyPaymentCredit(grpcCtx, &pb.ApplyPaymentCreditRequest{
		CustomerId:           payload.CustomerID,
		AmountMicro:          payload.AmountMicro,
		LedgerIdempotencyKey: payload.LedgerIdempotencyKey,
		PaymentIntentId:      payload.PaymentIntentID,
		Provider:             payload.Provider,
		ProviderRef:          payload.ProviderRef,
	})
	if err != nil {
		return fmt.Errorf("management SettlementService call failed: %w", err)
	}

	return nil
}
