package management

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads/db"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mykhailov-ua/ad-event-processor/internal/ads"
	"github.com/mykhailov-ua/ad-event-processor/internal/config"
	"github.com/mykhailov-ua/ad-event-processor/internal/metrics"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type Service struct {
	pool    *pgxpool.Pool
	rdbs    []redis.UniversalClient
	sharder ads.Sharder
	cfg     *config.Config
}

func NewService(pool *pgxpool.Pool, rdbs []redis.UniversalClient, sharder ads.Sharder, cfg *config.Config) *Service {
	return &Service{
		pool:    pool,
		rdbs:    rdbs,
		sharder: sharder,
		cfg:     cfg,
	}
}

func (s *Service) CreateCustomer(ctx context.Context, id uuid.UUID, name string, balance decimal.Decimal, currency string) error {
	_, err := db.New(s.pool).CreateCustomer(ctx, db.CreateCustomerParams{
		ID:       ads.ToUUID(id),
		Name:     name,
		Balance:  ads.ToNumeric(balance),
		Currency: currency,
	})
	if err == nil {
		s.AuditLog(ctx, nil, uuid.Nil, "CREATE_CUSTOMER", "customer", &id, map[string]any{"name": name, "balance": balance}, nil)
	}
	return err
}

func (s *Service) GenerateIdempotencyHash(customerID uuid.UUID, params any) string {
	b, _ := json.Marshal(params)
	h := sha256.New()
	h.Write([]byte(customerID.String()))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Service) TopUpBalance(ctx context.Context, customerID uuid.UUID, amount decimal.Decimal, idempotencyKey string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		_, err := q.GetLedgerByHash(ctx, pgtype.Text{String: idempotencyKey, Valid: true})
		if err == nil {
			return nil
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: ads.ToNumeric(amount),
		})
		if err != nil {
			return fmt.Errorf("failed to update balance: %w", err)
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			Amount:          ads.ToNumeric(amount),
			Type:            db.LedgerTypeTOPUP,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
		})
		if err == nil {
			metrics.BalanceTopupsTotal.WithLabelValues("USD").Add(amount.InexactFloat64())
			s.AuditLog(ctx, q, uuid.Nil, "TOPUP_BALANCE", "customer", &customerID, map[string]any{"amount": amount}, map[string]any{"idempotency_key": idempotencyKey})
		}
		return err
	})
}

func (s *Service) CreateCampaign(ctx context.Context, customerID uuid.UUID, name string, budgetLimit decimal.Decimal, idempotencyKey string) (uuid.UUID, error) {
	campaignID, _ := uuid.NewV7()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		if existing, err := q.GetLedgerByHash(ctx, pgtype.Text{String: idempotencyKey, Valid: true}); err == nil {
			if existing.CampaignID.Valid {
				campaignID = uuid.UUID(existing.CampaignID.Bytes)
				return nil
			}
		}
		cust, err := q.GetCustomerForUpdate(ctx, ads.ToUUID(customerID))
		if err != nil {
			return fmt.Errorf("customer not found: %w", err)
		}
		if ads.FromNumeric(cust.Balance).LessThan(budgetLimit) {
			return fmt.Errorf("insufficient balance")
		}
		_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
			ID:      ads.ToUUID(customerID),
			Balance: ads.ToNumeric(budgetLimit.Neg()),
		})
		if err != nil {
			return err
		}
		_, err = q.CreateCampaign(ctx, db.CreateCampaignParams{
			ID:          ads.ToUUID(campaignID),
			Name:        name,
			BudgetLimit: ads.ToNumeric(budgetLimit),
			Status:      db.CampaignStatusTypeACTIVE,
			CustomerID:  ads.ToUUID(customerID),
		})
		if err != nil {
			return err
		}
		_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
			CustomerID:      ads.ToUUID(customerID),
			CampaignID:      ads.ToUUID(campaignID),
			Amount:          ads.ToNumeric(budgetLimit),
			Type:            db.LedgerTypeFREEZE,
			IdempotencyHash: pgtype.Text{String: idempotencyKey, Valid: true},
		})
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			NewStatus:  db.CampaignStatusTypeACTIVE,
			Reason:     pgtype.Text{String: "db.Campaign Creation", Valid: true},
		})
		if err == nil {
			s.AuditLog(ctx, q, uuid.Nil, "CREATE_CAMPAIGN", "campaign", &campaignID, map[string]any{"name": name, "budget_limit": budgetLimit, "customer_id": customerID}, map[string]any{"idempotency_key": idempotencyKey})
		}
		return err
	})
	if err != nil {
		return uuid.Nil, err
	}
	rdb := s.getRDB(campaignID)
	if rdb != nil {
		rdb.Set(ctx, fmt.Sprintf("budget:campaign:%s", campaignID), budgetLimit.StringFixed(2), 24*time.Hour)
	} else {
		return uuid.Nil, fmt.Errorf("target redis shard is unavailable")
	}
	s.notifyUpdate(ctx, campaignID)
	return campaignID, nil
}

func (s *Service) CancelCampaign(ctx context.Context, campaignID uuid.UUID, reason string) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignFull(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		if camp.Status == db.CampaignStatusTypeDELETED || camp.Status == db.CampaignStatusTypeDRAINING {
			return nil
		}
		_, err = q.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
			ID:     ads.ToUUID(campaignID),
			Status: db.CampaignStatusTypeDRAINING,
		})
		if err != nil {
			return err
		}
		return q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: camp.Status, Valid: true},
			NewStatus:  db.CampaignStatusTypeDRAINING,
			Reason:     pgtype.Text{String: reason, Valid: true},
		})
	})
	if err != nil {
		return err
	}
	rdb := s.getRDB(campaignID)
	if rdb != nil {
		rdb.Del(ctx, fmt.Sprintf("budget:campaign:%s", campaignID))
	}
	s.notifyUpdate(ctx, campaignID)
	time.Sleep(time.Duration(s.cfg.Lifecycle.WaitTimeoutMs) * time.Millisecond)
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := db.New(tx)
		camp, err := q.GetCampaignFull(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		totalBudget := ads.FromNumeric(camp.BudgetLimit)
		currentSpend := ads.FromNumeric(camp.CurrentSpend)
		remaining := totalBudget.Sub(currentSpend)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
		feePercent := decimal.NewFromFloat(s.cfg.Management.CancellationFeePercent).Div(decimal.NewFromInt(100))
		fee := remaining.Mul(feePercent).Round(2)
		refund := remaining.Sub(fee)
		if refund.IsPositive() {
			_, err = q.UpdateCustomerBalanceManagement(ctx, db.UpdateCustomerBalanceManagementParams{
				ID:      camp.CustomerID,
				Balance: ads.ToNumeric(refund),
			})
			if err != nil {
				return err
			}
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     ads.ToNumeric(refund),
				Type:       db.LedgerTypeRELEASE,
			})
			if err != nil {
				return err
			}
		}
		if fee.IsPositive() {
			_, err = q.CreateLedgerEntry(ctx, db.CreateLedgerEntryParams{
				CustomerID: camp.CustomerID,
				CampaignID: ads.ToUUID(campaignID),
				Amount:     ads.ToNumeric(fee),
				Type:       db.LedgerTypeFEE,
			})
			if err != nil {
				return err
			}
			metrics.CommissionsCollectedTotal.Add(fee.InexactFloat64())
		}
		err = q.SoftDeleteCampaign(ctx, ads.ToUUID(campaignID))
		if err != nil {
			return err
		}
		err = q.CreateStatusHistory(ctx, db.CreateStatusHistoryParams{
			CampaignID: ads.ToUUID(campaignID),
			OldStatus:  db.NullCampaignStatusType{CampaignStatusType: db.CampaignStatusTypeDRAINING, Valid: true},
			NewStatus:  db.CampaignStatusTypeDELETED,
			Reason:     pgtype.Text{String: "Finalized", Valid: true},
		})
		if err != nil {
			return err
		}
		s.AuditLog(ctx, q, uuid.Nil, "CANCEL_CAMPAIGN", "campaign", &campaignID, map[string]any{"reason": reason}, nil)
		return nil
	})
	return err
}

func (s *Service) notifyUpdate(ctx context.Context, id uuid.UUID) {
	for _, rdb := range s.rdbs {
		if rdb != nil {
			rdb.Publish(ctx, s.cfg.CampaignUpdateChannel, id.String())
		}
	}
}

func (s *Service) getRDB(campaignID uuid.UUID) redis.UniversalClient {
	if len(s.rdbs) <= 1 {
		return s.rdbs[0]
	}
	idx := s.sharder.GetShard(campaignID)
	return s.rdbs[idx%len(s.rdbs)]
}

func (s *Service) UpdateSettings(ctx context.Context, settings map[string]string) error {
	// 1. Log to Audit
	s.AuditLog(ctx, nil, uuid.Nil, "UPDATE_SETTINGS", "system", nil, settings, nil)

	// 2. Update Redis
	// We use the first Redis shard as the source of truth for global config
	rdb := s.rdbs[0]

	_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		if len(settings) > 0 {
			pipe.HSet(ctx, "config:values", settings)
		}
		pipe.Incr(ctx, "config:version")
		return nil
	})

	return err
}

func (s *Service) BlockIP(ctx context.Context, ip string, source string) error {
	s.AuditLog(ctx, nil, uuid.Nil, "BLOCK_IP", "system", nil, map[string]string{"ip": ip, "source": source}, nil)

	rdb := s.rdbs[0]
	key := "blacklist:" + source
	if source == "" {
		key = "blacklist:manual"
	}

	return rdb.SAdd(ctx, key, ip).Err()
}

func (s *Service) UnblockIP(ctx context.Context, ip string, source string) error {
	s.AuditLog(ctx, nil, uuid.Nil, "UNBLOCK_IP", "system", nil, map[string]string{"ip": ip, "source": source}, nil)

	rdb := s.rdbs[0]
	key := "blacklist:" + source
	if source == "" {
		key = "blacklist:manual"
	}

	return rdb.SRem(ctx, key, ip).Err()
}

func (s *Service) ListAuditLogs(ctx context.Context, limit, offset int32) ([]db.AdminAuditLog, error) {
	return db.New(s.pool).ListAuditLogs(ctx, db.ListAuditLogsParams{
		Limit:  limit,
		Offset: offset,
	})
}
