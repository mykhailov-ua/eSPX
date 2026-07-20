package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/config"
	"espx/internal/ingestion"
	db "espx/internal/ingestion/sqlc"
	"espx/internal/metrics"
	"espx/pkg/coldpath"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Service coordinates management business logic, background workers, and hot-path propagation via outbox.
type Service struct {
	pool        *pgxpool.Pool
	rdbs        []redis.UniversalClient
	sharder     ingestion.Sharder
	cfg         *config.Config
	alerter     *OpsAlerter
	ch          driver.Conn
	paymentPool *pgxpool.Pool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	workerMu    sync.Mutex
	closed      atomic.Bool
	locCache    sync.Map
}

// StartBackgroundWorker launches an auxiliary goroutine tracked for graceful shutdown.
func (s *Service) StartBackgroundWorker(fn func()) {
	s.startWorker(fn)
}

// startWorker launches a background goroutine tracked for graceful shutdown.
func (s *Service) startWorker(fn func()) {
	s.workerMu.Lock()
	if s.closed.Load() {
		s.workerMu.Unlock()
		return
	}
	s.wg.Add(1)
	s.workerMu.Unlock()

	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// NewService constructs the management service and starts core background workers.
func NewService(pool *pgxpool.Pool, rdbs []redis.UniversalClient, sharder ingestion.Sharder, cfg *config.Config) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		pool:    pool,
		rdbs:    rdbs,
		sharder: sharder,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	s.startWorker(func() {
		NewOutboxWorker(s).Start(ctx, 20*time.Millisecond)
	})
	s.startWorker(func() {
		NewCampaignDrainWorker(s).Start(ctx, 20*time.Millisecond)
	})
	s.startWorker(func() {
		NewCreditScoringWorker(s).Start(ctx, 24*time.Hour)
	})
	s.startWorker(func() {
		NewScheduleWorker(s).Start(ctx)
	})
	s.startWorker(func() {
		NewTLSImpersonationWorker(s).Start(ctx, 1*time.Hour)
	})
	s.startWorker(func() {
		s.RunSystemStateSyncer(ctx)
	})
	return s
}

// StartReconWorker starts periodic ledger reconciliation on the given interval.
func (s *Service) StartReconWorker(interval time.Duration) {
	s.startWorker(func() {
		NewReconWorker(s, interval).Start(s.ctx)
	})
}

// StartAuditCleaner deletes audit rows older than the configured retention window.
func (s *Service) StartAuditCleaner(retention Days) {
	s.startWorker(func() {
		s.RunAuditCleaner(s.ctx, retention)
	})
}

// StartBlacklistJanitor evicts expired temporary blacklist entries from Postgres and Redis.
func (s *Service) StartBlacklistJanitor(interval time.Duration) {
	s.startWorker(func() {
		NewBlacklistJanitor(s, interval).Start(s.ctx)
	})
}

// GetPool exposes the Postgres pool for tests and auxiliary workers.
func (s *Service) GetPool() *pgxpool.Pool {
	return s.pool
}

// SetPool replaces the Postgres pool after reconnect (e.g. DB failover).
func (s *Service) SetPool(pool *pgxpool.Pool) {
	s.pool = pool
}

// SetOpsAlerter attaches the notifier-backed operator alert sender.
func (s *Service) SetOpsAlerter(alerter *OpsAlerter) {
	s.alerter = alerter
}

// Close cancels background workers and waits for them to exit.
func (s *Service) Close() {
	s.closed.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// StartPacingController starts the closed-loop pacing worker with budget sync dependencies.
func (s *Service) StartPacingController(syncWorkers []*ingestion.SyncWorker, interval time.Duration) {
	s.startWorker(func() {
		NewPacingControllerWorker(s, syncWorkers).Start(s.ctx, interval)
	})
}

// StartAutoscaleBudgetWorker starts CTR-based budget shifting when interval is positive.
func (s *Service) StartAutoscaleBudgetWorker(syncWorkers []*ingestion.SyncWorker, interval time.Duration) {
	s.startWorker(func() {
		NewAutoscaleBudgetWorker(s, syncWorkers).Start(s.ctx, interval)
	})
}

// StartDeliveryOptimizerWorker runs the unified M5.0 delivery pass (pacing, autoscale, MAB, bid floors).
func (s *Service) StartDeliveryOptimizerWorker(syncWorkers []*ingestion.SyncWorker, interval time.Duration) {
	s.startWorker(func() {
		NewDeliveryOptimizerWorker(s, syncWorkers).Start(s.ctx, interval)
	})
}

// GetCampaign loads the full campaign row for internal authorization and lifecycle checks.
func (s *Service) GetCampaign(ctx context.Context, id uuid.UUID) (db.Campaign, error) {
	return db.New(s.pool).GetCampaignFull(ctx, ingestion.ToUUID(id))
}

// CreateCustomer registers a new billing account with an optional opening balance.
func (s *Service) CreateCustomer(ctx context.Context, id uuid.UUID, name string, balance int64, currency string) error {
	_, err := db.New(s.pool).CreateCustomer(ctx, db.CreateCustomerParams{
		ID:       ingestion.ToUUID(id),
		Name:     name,
		Balance:  balance,
		Currency: currency,
	})
	if err == nil {
		s.AuditLog(ctx, nil, uuid.Nil, "CREATE_CUSTOMER", "customer", &id, map[string]any{"name": name, "balance": balance}, nil)
	}
	return err
}

// GenerateIdempotencyHash derives a stable key from customer identity and request payload for safe retries.
func (s *Service) GenerateIdempotencyHash(customerID uuid.UUID, params any) (string, error) {
	b, err := coldpath.MarshalJSON(params)
	if err != nil {
		return "", fmt.Errorf("idempotency hash params: %w", err)
	}
	h := sha256.New()
	h.Write([]byte(customerID.String()))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// TopUpBalance credits a customer account idempotently and records the ledger entry.
func (s *Service) TopUpBalance(ctx context.Context, customerID uuid.UUID, amount int64, idempotencyKey string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: idempotencyKey, Valid: true})
		if err == nil {
			return nil
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ingestion.ToUUID(customerID),
			Balance: amount,
		})
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ingestion.ToUUID(customerID),
			Amount:          amount,
			Type:            db.LedgerTypeTOPUP,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
			PaymentIntentID: pgtype.UUID{},
		})
		if err == nil {
			metrics.BalanceTopupsTotal.WithLabelValues("USD").Add(float64(amount) / ingestion.MicroUnitFactor)
			s.AuditLog(ctx, q, uuid.Nil, "TOPUP_BALANCE", "customer", &customerID, map[string]any{"amount": amount}, map[string]any{"idempotency_key": idempotencyKey})
		}
		return err
	})
}

// ApplyPaymentCredit credits a customer account idempotently and records the PAYMENT_TOPUP ledger entry.
func (s *Service) ApplyPaymentCredit(ctx context.Context, customerID uuid.UUID, amount int64, ledgerIdempotencyKey string, paymentIntentID uuid.UUID, provider string, providerRef string) (bool, int64, error) {
	var ledgerEntryID int64
	var applied bool

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)

		existingPI, err := q.GetLedgerByPaymentIntentForUpdate(ctx, ingestion.ToUUID(paymentIntentID))
		if err == nil {
			ledgerEntryID = existingPI.ID
			applied = false
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("payment intent idempotency check failed: %w", err)
		}

		existing, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true})
		if err == nil {
			ledgerEntryID = existing.ID
			applied = false
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("idempotency check failed: %w", err)
		}

		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ingestion.ToUUID(customerID),
			Balance: amount,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrCustomerNotFound
			}
			return fmt.Errorf("failed to update balance: %w", err)
		}

		row, err := q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ingestion.ToUUID(customerID),
			Amount:          amount,
			Type:            db.LedgerType("PAYMENT_TOPUP"),
			IdempotencyHash: pgtype.Text{String: ledgerIdempotencyKey, Valid: true},
			PaymentIntentID: ingestion.ToUUID(paymentIntentID),
		})
		if err != nil {
			if isPgUniqueViolation(err) {
				if existingPI, lookupErr := q.GetLedgerByPaymentIntentForUpdate(ctx, ingestion.ToUUID(paymentIntentID)); lookupErr == nil {
					ledgerEntryID = existingPI.ID
					applied = false
					return nil
				}
				if existing, lookupErr := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true}); lookupErr == nil {
					ledgerEntryID = existing.ID
					applied = false
					return nil
				}
			}
			return fmt.Errorf("failed to create ledger entry: %w", err)
		}

		ledgerEntryID = row.ID
		applied = true

		metrics.BalanceTopupsTotal.WithLabelValues("USD").Add(float64(amount) / ingestion.MicroUnitFactor)
		s.AuditLog(ctx, q, uuid.Nil, "PAYMENT_SETTLEMENT", "customer", &customerID, map[string]any{"amount": amount, "payment_intent_id": paymentIntentID.String(), "provider": provider, "provider_ref": providerRef}, map[string]any{"idempotency_key": ledgerIdempotencyKey})
		return nil
	})

	return applied, ledgerEntryID, err
}

// ApplyPaymentRefund debits a customer account idempotently after a Stripe refund webhook.
func (s *Service) ApplyPaymentRefund(ctx context.Context, customerID uuid.UUID, amountMicro int64, ledgerIdempotencyKey string, paymentIntentID uuid.UUID, provider string, providerRefundID string) (bool, int64, error) {
	if amountMicro <= 0 {
		return false, 0, errValidation("refund amount must be positive")
	}

	var ledgerEntryID int64
	var applied bool

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)

		existing, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true})
		if err == nil {
			ledgerEntryID = existing.ID
			applied = false
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("refund idempotency check failed: %w", err)
		}

		topup, err := q.GetLedgerByPaymentIntentForUpdate(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrPaymentTopupNotFound
			}
			return fmt.Errorf("payment topup lookup failed: %w", err)
		}

		refundedSoFar, err := q.SumPaymentRefundAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			return fmt.Errorf("sum payment refunds failed: %w", err)
		}
		if refundedSoFar+amountMicro > topup.Amount {
			return ErrRefundExceedsTopup
		}

		debitAmount := -amountMicro
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ingestion.ToUUID(customerID),
			Balance: debitAmount,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrCustomerNotFound
			}
			return fmt.Errorf("failed to debit balance: %w", err)
		}

		row, err := q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ingestion.ToUUID(customerID),
			Amount:          debitAmount,
			Type:            db.LedgerType("PAYMENT_REFUND"),
			IdempotencyHash: pgtype.Text{String: ledgerIdempotencyKey, Valid: true},
			PaymentIntentID: ingestion.ToUUID(paymentIntentID),
		})
		if err != nil {
			if isPgUniqueViolation(err) {
				if existing, lookupErr := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true}); lookupErr == nil {
					ledgerEntryID = existing.ID
					applied = false
					return nil
				}
			}
			return fmt.Errorf("failed to create refund ledger entry: %w", err)
		}

		ledgerEntryID = row.ID
		applied = true

		s.AuditLog(ctx, q, uuid.Nil, "PAYMENT_REFUND", "customer", &customerID,
			map[string]any{"amount": amountMicro, "payment_intent_id": paymentIntentID.String(), "provider": provider, "provider_refund_id": providerRefundID},
			map[string]any{"idempotency_key": ledgerIdempotencyKey})
		return nil
	})

	return applied, ledgerEntryID, err
}

// ApplyPaymentChargeback debits a customer account when Stripe withdraws disputed funds.
func (s *Service) ApplyPaymentChargeback(ctx context.Context, customerID uuid.UUID, amountMicro int64, ledgerIdempotencyKey string, paymentIntentID uuid.UUID, provider string, providerDisputeID string) (bool, int64, error) {
	return s.applyPaymentChargebackMovement(ctx, customerID, amountMicro, ledgerIdempotencyKey, paymentIntentID, provider, providerDisputeID, "PAYMENT_CHARGEBACK", true)
}

// ApplyPaymentChargebackReversal credits a customer account when Stripe reinstates won dispute funds.
func (s *Service) ApplyPaymentChargebackReversal(ctx context.Context, customerID uuid.UUID, amountMicro int64, ledgerIdempotencyKey string, paymentIntentID uuid.UUID, provider string, providerDisputeID string) (bool, int64, error) {
	return s.applyPaymentChargebackMovement(ctx, customerID, amountMicro, ledgerIdempotencyKey, paymentIntentID, provider, providerDisputeID, "PAYMENT_CHARGEBACK_REVERSAL", false)
}

func (s *Service) applyPaymentChargebackMovement(
	ctx context.Context,
	customerID uuid.UUID,
	amountMicro int64,
	ledgerIdempotencyKey string,
	paymentIntentID uuid.UUID,
	provider string,
	providerDisputeID string,
	ledgerType string,
	isDebit bool,
) (bool, int64, error) {
	if amountMicro <= 0 {
		return false, 0, errValidation("chargeback amount must be positive")
	}

	var ledgerEntryID int64
	var applied bool

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)

		existing, err := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true})
		if err == nil {
			ledgerEntryID = existing.ID
			applied = false
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("chargeback idempotency check failed: %w", err)
		}

		topup, err := q.GetLedgerByPaymentIntentForUpdate(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrPaymentTopupNotFound
			}
			return fmt.Errorf("payment topup lookup failed: %w", err)
		}

		refundedSoFar, err := q.SumPaymentRefundAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			return fmt.Errorf("sum payment refunds failed: %w", err)
		}
		chargebackSoFar, err := q.SumPaymentChargebackAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			return fmt.Errorf("sum payment chargebacks failed: %w", err)
		}
		reversalSoFar, err := q.SumPaymentChargebackReversalAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
		if err != nil {
			return fmt.Errorf("sum payment chargeback reversals failed: %w", err)
		}

		netChargeback := chargebackSoFar - reversalSoFar
		if isDebit {
			if refundedSoFar+netChargeback+amountMicro > topup.Amount {
				return ErrChargebackExceedsTopup
			}
		} else if amountMicro > netChargeback {
			return ErrChargebackReversalExceedsWithdrawn
		}

		balanceDelta := amountMicro
		ledgerAmount := amountMicro
		if isDebit {
			balanceDelta = -amountMicro
			ledgerAmount = -amountMicro
		}

		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ingestion.ToUUID(customerID),
			Balance: balanceDelta,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrCustomerNotFound
			}
			return fmt.Errorf("failed to update balance for chargeback: %w", err)
		}

		row, err := q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ingestion.ToUUID(customerID),
			Amount:          ledgerAmount,
			Type:            db.LedgerType(ledgerType),
			IdempotencyHash: pgtype.Text{String: ledgerIdempotencyKey, Valid: true},
			PaymentIntentID: ingestion.ToUUID(paymentIntentID),
		})
		if err != nil {
			if isPgUniqueViolation(err) {
				if existing, lookupErr := q.GetLedgerByHashForUpdate(ctx, pgtype.Text{String: ledgerIdempotencyKey, Valid: true}); lookupErr == nil {
					ledgerEntryID = existing.ID
					applied = false
					return nil
				}
			}
			return fmt.Errorf("failed to create chargeback ledger entry: %w", err)
		}

		ledgerEntryID = row.ID
		applied = true

		action := "PAYMENT_CHARGEBACK"
		if !isDebit {
			action = "PAYMENT_CHARGEBACK_REVERSAL"
		}
		s.AuditLog(ctx, q, uuid.Nil, action, "customer", &customerID,
			map[string]any{"amount": amountMicro, "payment_intent_id": paymentIntentID.String(), "provider": provider, "provider_dispute_id": providerDisputeID},
			map[string]any{"idempotency_key": ledgerIdempotencyKey})
		return nil
	})

	return applied, ledgerEntryID, err
}

// CancelCampaign marks a campaign draining so the hot path can finish in-flight bids before refund.
func (s *Service) CancelCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignForUpdate(ctx, ingestion.ToUUID(campaignID))
		if err != nil {
			return err
		}
		if camp.Status == db.CampaignStatusTypeDELETED || camp.Status == db.CampaignStatusTypeDRAINING {
			return nil
		}
		_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
			ID:     ingestion.ToUUID(campaignID),
			Status: db.CampaignStatusTypeDRAINING,
		})
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ingestion.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: camp.Status, Valid: true},
			NewStatus:  db.CampaignStatusTypeDRAINING,
			Reason:     pgtype.Text{String: reason, Valid: true},
		})
		if err == nil {
			payloadBytes, marshalErr := coldpath.MarshalJSON(CampaignPayload{CampaignID: campaignID.String()})
			if marshalErr != nil {
				return fmt.Errorf("marshal cancel campaign outbox payload: %w", marshalErr)
			}
			_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{EventType: "CANCEL_CAMPAIGN", Payload: payloadBytes})
		}
		return err
	})
}

// FinalizeCancelledCampaign completes refund and deletion for one draining campaign under row lock.
func (s *Service) FinalizeCancelledCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		var camp db.Campaign
		err := tx.QueryRow(ctx, `
			SELECT status, budget_limit, current_spend, customer_id 
			FROM campaigns 
			WHERE id = $1 
			FOR UPDATE`, ingestion.ToUUID(campaignID)).Scan(&camp.Status, &camp.BudgetLimit, &camp.CurrentSpend, &camp.CustomerID)
		if err != nil {
			return err
		}
		return s.finalizeDrainingCampaign(ctx, q, campaignID, camp, reason)
	})
}

// finalizeDrainingCampaign releases remaining budget, collects fees, and soft-deletes a draining campaign.
func (s *Service) finalizeDrainingCampaign(ctx context.Context, q db.Querier, campaignID uuid.UUID, camp db.Campaign, reason string) error {
	if camp.Status != db.CampaignStatusTypeDRAINING {
		return nil
	}
	totalBudget := camp.BudgetLimit
	currentSpend := camp.CurrentSpend
	remaining := totalBudget - currentSpend
	if remaining < 0 {
		remaining = 0
	}
	feePercent := 0.0
	if s.cfg != nil {
		feePercent = s.cfg.Management.CancellationFeePercent
	}
	fee := int64(float64(remaining) * (feePercent / 100.0))
	refund := remaining - fee
	if refund > 0 {
		_, err := q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      camp.CustomerID,
			Balance: refund,
		})
		if err != nil {
			return err
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      camp.CustomerID,
			CampaignID:      ingestion.ToUUID(campaignID),
			Amount:          refund,
			Type:            db.LedgerTypeRELEASE,
			PaymentIntentID: pgtype.UUID{},
		})
		if err != nil {
			return err
		}
	}
	if fee > 0 {
		_, err := q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      camp.CustomerID,
			CampaignID:      ingestion.ToUUID(campaignID),
			Amount:          fee,
			Type:            db.LedgerTypeFEE,
			PaymentIntentID: pgtype.UUID{},
		})
		if err != nil {
			return err
		}
		metrics.CommissionsCollectedTotal.Add(float64(fee) / ingestion.MicroUnitFactor)
	}
	if err := q.SoftDeleteCampaign(ctx, ingestion.ToUUID(campaignID)); err != nil {
		return err
	}
	if err := q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
		CampaignID: ingestion.ToUUID(campaignID),
		OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeDRAINING, Valid: true},
		NewStatus:  db.CampaignStatusTypeDELETED,
		Reason:     pgtype.Text{String: "Finalized", Valid: true},
	}); err != nil {
		return err
	}
	s.AuditLog(ctx, q, uuid.Nil, "CANCEL_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)
	return nil
}

// campaignUpdateChannel returns the Redis pubsub channel used to invalidate hot-path campaign caches.
func (s *Service) campaignUpdateChannel() string {
	if s.cfg != nil && s.cfg.CampaignUpdateChannel != "" {
		return s.cfg.CampaignUpdateChannel
	}
	return "campaigns:update"
}

// getPubSubRDB returns shard 0, the sole Redis instance trackers subscribe on for campaigns:update.
func (s *Service) getPubSubRDB() redis.UniversalClient {
	if len(s.rdbs) == 0 {
		return nil
	}
	return s.rdbs[0]
}

// publishCampaignUpdate notifies trackers via pub/sub on shard 0 regardless of campaign key placement.
func (s *Service) publishCampaignUpdate(ctx context.Context, campaignID string) error {
	rdb := s.getPubSubRDB()
	if rdb == nil {
		return fmt.Errorf("no redis pubsub client available")
	}
	return rdb.Publish(ctx, s.campaignUpdateChannel(), campaignID).Err()
}

// getRDB selects the Redis shard that owns a campaign's budget and settings keys.
func (s *Service) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(s.rdbs) == 0 {
		return nil
	}
	if len(s.rdbs) == 1 {
		return s.rdbs[0]
	}
	idx := s.sharder.GetShard(campaignID)
	return s.rdbs[idx%len(s.rdbs)]
}

// ListAuditLogs returns paginated admin audit entries for compliance review.
func (s *Service) ListAuditLogs(ctx context.Context, limit, offset int32) ([]db.AdminAuditLog, int64, error) {
	q := db.New(s.pool)
	return coldpath.PaginatedQuery(
		func() (int64, error) { return q.CountAuditLogs(ctx) },
		func() ([]db.AdminAuditLog, error) {
			return q.ListAuditPaginated(ctx, db.ListAuditPaginatedParams{
				Limit:  limit,
				Offset: offset,
			})
		},
	)
}

// GetLedgerEntry returns PAYMENT_TOPUP ledger state and related totals for a payment intent.
func (s *Service) GetLedgerEntry(ctx context.Context, paymentIntentID uuid.UUID) (found bool, entry db.BalanceLedger, refundTotal, chargebackTotal, reversalTotal int64, err error) {
	q := db.New(s.pool)
	entry, err = q.GetLedgerByPaymentIntent(ctx, ingestion.ToUUID(paymentIntentID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			entry = db.BalanceLedger{}
		} else {
			return false, db.BalanceLedger{}, 0, 0, 0, err
		}
	} else {
		found = true
	}
	refundTotal, err = q.SumPaymentRefundAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
	if err != nil {
		return false, db.BalanceLedger{}, 0, 0, 0, err
	}
	chargebackTotal, err = q.SumPaymentChargebackAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
	if err != nil {
		return false, db.BalanceLedger{}, 0, 0, 0, err
	}
	reversalTotal, err = q.SumPaymentChargebackReversalAmountForIntent(ctx, ingestion.ToUUID(paymentIntentID))
	if err != nil {
		return false, db.BalanceLedger{}, 0, 0, 0, err
	}
	return found, entry, refundTotal, chargebackTotal, reversalTotal, nil
}

// UpdateOverdraft adjusts credit limits and suspends campaigns when reduced overdraft would overcommit balance.
func (s *Service) UpdateOverdraft(ctx context.Context, id uuid.UUID, newOverdraft int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		cust, err := q.GetCustomerForUpdate(ctx, ingestion.ToUUID(id))
		if err != nil {
			return fmt.Errorf("failed to fetch customer for overdraft update: %w", err)
		}

		prevOverdraft := cust.AllowedOverdraft
		if newOverdraft == prevOverdraft {
			return nil
		}

		if newOverdraft < prevOverdraft {
			availableLimit := cust.Balance + newOverdraft
			if availableLimit < 0 {
				camps, err := q.ListCampaigns(ctx, db.ListCampaignsParams{
					Limit:      10000,
					Offset:     0,
					CustomerID: ingestion.ToUUID(id),
					Status:     pgtype.Text{String: string(db.CampaignStatusTypeACTIVE), Valid: true},
				})
				if err != nil {
					return fmt.Errorf("failed to list active campaigns for overdraft decrease: %w", err)
				}

				for _, c := range camps {
					if availableLimit >= 0 {
						break
					}

					locked, err := q.GetCampaignForUpdate(ctx, c.ID)
					if err != nil {
						return fmt.Errorf("failed to lock campaign for overdraft suspend: %w", err)
					}
					if locked.Status != db.CampaignStatusTypeACTIVE {
						continue
					}

					_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
						ID:     locked.ID,
						Status: db.CampaignStatusTypePAUSED,
					})
					if err != nil {
						return fmt.Errorf("failed to pause campaign: %w", err)
					}

					err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
						CampaignID: locked.ID,
						OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeACTIVE, Valid: true},
						NewStatus:  db.CampaignStatusTypePAUSED,
						Reason:     pgtype.Text{String: "Overdraft reduced, campaign suspended", Valid: true},
					})
					if err != nil {
						return fmt.Errorf("failed to write status history: %w", err)
					}

					budgetLimit := locked.BudgetLimit
					currentSpend := locked.CurrentSpend
					remaining := budgetLimit - currentSpend
					if remaining < 0 {
						remaining = 0
					}

					if remaining > 0 {
						_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
							ID:      ingestion.ToUUID(id),
							Balance: remaining,
						})
						if err != nil {
							return fmt.Errorf("failed to refund balance for suspended campaign: %w", err)
						}

						_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
							CustomerID:      ingestion.ToUUID(id),
							CampaignID:      locked.ID,
							Amount:          remaining,
							Type:            db.LedgerTypeRELEASE,
							PaymentIntentID: pgtype.UUID{},
						})
						if err != nil {
							return fmt.Errorf("failed to record release ledger entry: %w", err)
						}

						availableLimit = availableLimit + remaining
					}

					payloadBytes, marshalErr := coldpath.MarshalJSON(CampaignPayload{CampaignID: uuid.UUID(locked.ID.Bytes).String()})
					if marshalErr != nil {
						return fmt.Errorf("marshal pause campaign outbox payload: %w", marshalErr)
					}
					_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
						EventType: "PAUSE_CAMPAIGN",
						Payload:   payloadBytes,
					})
					if err != nil {
						return fmt.Errorf("failed to emit outbox event for paused campaign: %w", err)
					}

					campID := uuid.UUID(locked.ID.Bytes)
					s.AuditLog(ctx, q, uuid.Nil, "SUSPEND_CAMPAIGN", "campaign", &campID, map[string]any{"reason": "overdraft_reduced"}, nil)
				}
			}
		}

		_, err = q.UpdateCustomerOverdraft(ctx, db.UpdateCustomerOverdraftParams{
			ID:               ingestion.ToUUID(id),
			AllowedOverdraft: newOverdraft,
		})
		if err != nil {
			return err
		}

		s.AuditLog(ctx, q, uuid.Nil, "UPDATE_CUSTOMER_OVERDRAFT", "customer", &id, map[string]any{
			"old_overdraft": fmt.Sprintf("%.2f", float64(prevOverdraft)/ingestion.MicroUnitFactor),
			"new_overdraft": fmt.Sprintf("%.2f", float64(newOverdraft)/ingestion.MicroUnitFactor),
		}, nil)
		return nil
	})
}
