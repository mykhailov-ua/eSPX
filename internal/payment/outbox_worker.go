package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"espx/internal/config"
	"espx/internal/management/pb"
	"espx/internal/payment/db"

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
var PostSettlementMarkHook func(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error

// OutboxWorker delivers SETTLE_BALANCE outboxEventents to management without blocking webhook HTTP responses.
type OutboxWorker struct {
	pool *pgxpool.Pool
	cfg  *config.Config

	clientMu sync.Mutex
	client   pb.SettlementServiceClient
	conn     *grpc.ClientConn

	settlementAlerter *SettlementFailedAlerter

	wg sync.WaitGroup
}

// SetSettlementFailedAlerter wires ops notifications for terminal settlement failures.
func (outboxWorker *OutboxWorker) SetSettlementFailedAlerter(alerter *SettlementFailedAlerter) {
	outboxWorker.settlementAlerter = alerter
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
func (outboxWorker *OutboxWorker) Start(ctx context.Context, interval time.Duration) {
	outboxWorker.wg.Add(1)
	defer outboxWorker.wg.Done()

	if err := outboxWorker.ensureSettlementClient(); err != nil {
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
			outboxWorker.resetSettlementClient()
			return
		case <-recoveryTicker.C:
			outboxWorker.reclaimStaleProcessing(ctx)
		case <-pollTimer.C:
			outboxWorker.refreshOutboxPendingGauge(ctx)
			processed, err := outboxWorker.ProcessOutbox(ctx, 100)
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
func (outboxWorker *OutboxWorker) Wait() {
	outboxWorker.wg.Wait()
}

// refreshOutboxPendingGauge exposes backlog depth for alerting when settlement falls behind webhooks.
func (outboxWorker *OutboxWorker) refreshOutboxPendingGauge(ctx context.Context) {
	var pending int64
	err := outboxWorker.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM payment.payment_outbox
		WHERE status IN ('PENDING', 'PROCESSING')`).Scan(&pending)
	if err == nil {
		OutboxPending.Set(float64(pending))
	}
}

// reclaimStaleProcessing recovers rows leased by crashed workers so settlement does not stall indefinitely.
func (outboxWorker *OutboxWorker) reclaimStaleProcessing(ctx context.Context) {
	_, err := outboxWorker.pool.Exec(ctx, `
		UPDATE payment.payment_outbox
		SET status = 'PENDING', lease_until = NULL
		WHERE status = 'PROCESSING'
		  AND lease_until IS NOT NULL
		  AND lease_until < now()`)
	if err != nil && ctx.Err() == nil && !strings.Contains(err.Error(), "closed pool") {
		slog.Error("failed to reclaim stale payment outbox outboxEventents", "error", err)
	}
}

// ProcessOutbox leases a batch before gRPC calls so duplicate workers cannot credit the same outboxEventent twice.
func (outboxWorker *OutboxWorker) ProcessOutbox(ctx context.Context, limit int32) (int, error) {
	if err := outboxWorker.ensureSettlementClient(); err != nil {
		return 0, err
	}

	var outboxEventents []db.PaymentPaymentOutbox
	leaseDuration := 30 * time.Second
	leaseUntil := time.Now().Add(leaseDuration)

	err := pgx.BeginFunc(ctx, outboxWorker.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)
		var err error
		outboxEventents, err = txQueries.GetPendingOutboxEventsForUpdate(ctx, limit)
		if err != nil || len(outboxEventents) == 0 {
			return err
		}

		ids := make([]int64, len(outboxEventents))
		for i, outboxEvent := range outboxEventents {
			ids[i] = outboxEvent.ID
		}

		err = txQueries.LeaseOutboxEvents(ctx, db.LeaseOutboxEventsParams{
			Column1:    ids,
			LeaseUntil: pgtype.Timestamptz{Time: leaseUntil, Valid: true},
		})
		return err
	})

	if err != nil || len(outboxEventents) == 0 {
		return 0, err
	}

	successCount := 0
	for _, outboxEvent := range outboxEventents {
		if err := outboxWorker.handleOutboxEvent(ctx, outboxEvent); err != nil {
			slog.Error("failed to handle outbox outboxEventent", "id", outboxEvent.ID, "error", err)
			SettlementErrorsTotal.Inc()
			if isSettlementTransientGRPC(err) {
				outboxWorker.resetSettlementClient()
			}
			outboxWorker.markOutboxEventRetryable(ctx, outboxEvent, err)
			continue
		}
		if PostSettlementMarkHook != nil {
			if hookErr := PostSettlementMarkHook(ctx, outboxEvent); hookErr != nil {
				slog.Error("post-settlement hook failed", "id", outboxEvent.ID, "error", hookErr)
				SettlementErrorsTotal.Inc()
				outboxWorker.markOutboxEventRetryable(ctx, outboxEvent, hookErr)
				continue
			}
		}
		if err := outboxWorker.markOutboxProcessedWithRetry(ctx, outboxEvent.ID); err != nil {
			slog.Error("failed to mark outbox outboxEventent processed", "id", outboxEvent.ID, "error", err)
			continue
		}
		successCount++
	}

	outboxWorker.refreshOutboxPendingGauge(ctx)
	return successCount, nil
}

// ensureSettlementClient dials management lazily because payment may start before settlement gRPC is listening.
func (outboxWorker *OutboxWorker) ensureSettlementClient() error {
	outboxWorker.clientMu.Lock()
	defer outboxWorker.clientMu.Unlock()

	if outboxWorker.client != nil {
		return nil
	}
	target := outboxWorker.cfg.SettlementServerHost + ":" + outboxWorker.cfg.SettlementServerPort
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("gRPC client not connected to %s: %w", target, err)
	}
	outboxWorker.conn = conn
	outboxWorker.client = pb.NewSettlementServiceClient(conn)
	return nil
}

// resetSettlementClient drops a dead connection so the next batch opens a fresh settlement channel.
func (outboxWorker *OutboxWorker) resetSettlementClient() {
	outboxWorker.clientMu.Lock()
	defer outboxWorker.clientMu.Unlock()

	if outboxWorker.conn != nil {
		_ = outboxWorker.conn.Close()
	}
	outboxWorker.conn = nil
	outboxWorker.client = nil
}

// getSettlementClient returns the cached client under lock because gRPC conn is shared across poll iterations.
func (outboxWorker *OutboxWorker) getSettlementClient() pb.SettlementServiceClient {
	outboxWorker.clientMu.Lock()
	defer outboxWorker.clientMu.Unlock()
	return outboxWorker.client
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
func (outboxWorker *OutboxWorker) markOutboxProcessedWithRetry(ctx context.Context, outboxID int64) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		lastErr = db.New(outboxWorker.pool).MarkOutboxEventProcessed(ctx, outboxID)
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
func (outboxWorker *OutboxWorker) markOutboxEventRetryable(ctx context.Context, outboxEvent db.PaymentPaymentOutbox, cause error) {
	var lastErrText pgtype.Text
	lastErrText.String = cause.Error()
	lastErrText.Valid = true

	isFatal := false
	st, ok := status.FromError(cause)
	if ok && st.Code() == codes.NotFound {
		isFatal = true
	}

	maxAttempts := int32(outboxWorker.cfg.MaxRetries)
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	permanent := isFatal || outboxEvent.Attempts+1 >= maxAttempts

	innerErr := pgx.BeginFunc(ctx, outboxWorker.pool, func(tx pgx.Tx) error {
		txQueries := db.New(tx)
		if isFatal {
			if err := txQueries.MarkOutboxEventFailed(ctx, db.MarkOutboxEventFailedParams{
				ID:        outboxEvent.ID,
				Attempts:  0,
				LastError: lastErrText,
			}); err != nil {
				return err
			}

			var payload SettleBalancePayload
			if outboxEvent.EventType == "SETTLE_BALANCE" {
				if err := json.Unmarshal(outboxEvent.Payload, &payload); err == nil {
					intentUUID, _ := uuid.Parse(payload.PaymentIntentID)
					_, _ = txQueries.UpdatePaymentIntentStatus(ctx, db.UpdatePaymentIntentStatusParams{
						ID:          pgtype.UUID{Bytes: intentUUID, Valid: true},
						Status:      db.PaymentPaymentIntentStatusSETTLEMENTFAILED,
						ProviderRef: pgtype.Text{String: payload.ProviderRef, Valid: true},
					})
				}
			}
		} else {
			if err := txQueries.MarkOutboxEventFailed(ctx, db.MarkOutboxEventFailedParams{
				ID:        outboxEvent.ID,
				Attempts:  maxAttempts,
				LastError: lastErrText,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if innerErr != nil {
		slog.Error("failed to update outbox outboxEventent failure status", "id", outboxEvent.ID, "error", innerErr)
		return
	}
	if permanent && outboxWorker.settlementAlerter != nil {
		outboxWorker.settlementAlerter.AlertPermanentFailure(outboxEvent, cause)
	}
}

// handleOutboxEvent forwards one row to management; ledger idempotency key prevents double credit on retry.
func (outboxWorker *OutboxWorker) handleOutboxEvent(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error {
	switch outboxEvent.EventType {
	case "SETTLE_BALANCE":
		return outboxWorker.handleSettleBalance(ctx, outboxEvent)
	case OutboxEventReverseBalance:
		return outboxWorker.handleReverseBalance(ctx, outboxEvent)
	case OutboxEventApplyChargeback:
		return outboxWorker.handleApplyChargeback(ctx, outboxEvent)
	case OutboxEventReverseChargeback:
		return outboxWorker.handleReverseChargeback(ctx, outboxEvent)
	default:
		slog.Warn("skipping unrecognized payment outbox outboxEventent type", "type", outboxEvent.EventType)
		return nil
	}
}

func (outboxWorker *OutboxWorker) handleSettleBalance(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error {

	var payload SettleBalancePayload
	if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal outbox payload: %w", err)
	}

	client := outboxWorker.getSettlementClient()
	if client == nil {
		return fmt.Errorf("settlement client not connected")
	}

	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(outboxWorker.cfg.SettlementInternalToken))

	_, err := client.ApplyPaymentCredit(grpcCtx, &pb.ApplyPaymentCreditRequest{
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

func (outboxWorker *OutboxWorker) handleReverseBalance(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error {
	var payload ReverseBalancePayload
	if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal reverse balance payload: %w", err)
	}

	client := outboxWorker.getSettlementClient()
	if client == nil {
		return fmt.Errorf("settlement client not connected")
	}

	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(outboxWorker.cfg.SettlementInternalToken))

	_, err := client.ApplyPaymentRefund(grpcCtx, &pb.ApplyPaymentRefundRequest{
		CustomerId:           payload.CustomerID,
		AmountMicro:          payload.AmountMicro,
		LedgerIdempotencyKey: payload.LedgerIdempotencyKey,
		PaymentIntentId:      payload.PaymentIntentID,
		Provider:             payload.Provider,
		ProviderRefundId:     payload.ProviderRefundID,
	})
	if err != nil {
		return fmt.Errorf("management SettlementService refund call failed: %w", err)
	}

	return nil
}

func (outboxWorker *OutboxWorker) handleApplyChargeback(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error {
	var payload ApplyChargebackPayload
	if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal apply chargeback payload: %w", err)
	}

	client := outboxWorker.getSettlementClient()
	if client == nil {
		return fmt.Errorf("settlement client not connected")
	}

	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(outboxWorker.cfg.SettlementInternalToken))

	_, err := client.ApplyPaymentChargeback(grpcCtx, &pb.ApplyPaymentChargebackRequest{
		CustomerId:           payload.CustomerID,
		AmountMicro:          payload.AmountMicro,
		LedgerIdempotencyKey: payload.LedgerIdempotencyKey,
		PaymentIntentId:      payload.PaymentIntentID,
		Provider:             payload.Provider,
		ProviderDisputeId:    payload.ProviderDisputeID,
	})
	if err != nil {
		return fmt.Errorf("management SettlementService chargeback call failed: %w", err)
	}
	return nil
}

func (outboxWorker *OutboxWorker) handleReverseChargeback(ctx context.Context, outboxEvent db.PaymentPaymentOutbox) error {
	var payload ReverseChargebackPayload
	if err := json.Unmarshal(outboxEvent.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal reverse chargeback payload: %w", err)
	}

	client := outboxWorker.getSettlementClient()
	if client == nil {
		return fmt.Errorf("settlement client not connected")
	}

	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-internal-token", string(outboxWorker.cfg.SettlementInternalToken))

	_, err := client.ApplyPaymentChargebackReversal(grpcCtx, &pb.ApplyPaymentChargebackReversalRequest{
		CustomerId:           payload.CustomerID,
		AmountMicro:          payload.AmountMicro,
		LedgerIdempotencyKey: payload.LedgerIdempotencyKey,
		PaymentIntentId:      payload.PaymentIntentID,
		Provider:             payload.Provider,
		ProviderDisputeId:    payload.ProviderDisputeID,
	})
	if err != nil {
		return fmt.Errorf("management SettlementService chargeback reversal call failed: %w", err)
	}
	return nil
}
