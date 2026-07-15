package ingestion

import (
	"context"
	"time"

	"espx/internal/campaignmodel"
	"espx/internal/config"
	"espx/internal/ingestion/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ToUUID wraps a uuid.UUID for pgtype query parameters.
func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

func campaignFromDBRow(row db.Campaign) *campaignmodel.Campaign {
	id := uuid.UUID(row.ID.Bytes)
	customerID := uuid.UUID(row.CustomerID.Bytes)

	loc, err := time.LoadLocation(row.Timezone)
	if err != nil {
		loc = time.UTC
	}

	var brandIDPtr *uuid.UUID
	if row.BrandID.Valid {
		brandID := uuid.UUID(row.BrandID.Bytes)
		brandIDPtr = &brandID
	}

	idStr := id.String()
	customerIDStr := customerID.String()
	dailyBudgetMicro := row.DailyBudget

	var fcapPrefix string
	if row.BrandFcapKey != "" {
		fcapPrefix = row.BrandFcapKey + ":u:"
	} else {
		fcapPrefix = "fcap:c:" + idStr + ":u:"
	}

	return &campaignmodel.Campaign{
		ID:                     id,
		IDStr:                  idStr,
		IDStrAny:               idStr,
		CustomerID:             customerID,
		CustomerIDStr:          customerIDStr,
		CustomerIDStrAny:       customerIDStr,
		BrandID:                brandIDPtr,
		BrandFcapKey:           row.BrandFcapKey,
		Name:                   row.Name,
		Status:                 campaignmodel.CampaignStatus(row.Status),
		PacingMode:             campaignmodel.PacingMode(row.PacingMode),
		BudgetLimit:            row.BudgetLimit,
		CurrentSpend:           row.CurrentSpend,
		DailyBudget:            row.DailyBudget,
		DailyBudgetMicro:       dailyBudgetMicro,
		DailyBudgetMicroAny:    dailyBudgetMicro,
		Timezone:               row.Timezone,
		Location:               loc,
		FreqLimit:              row.FreqLimit.Int32,
		FreqLimitAny:           row.FreqLimit.Int32,
		FreqWindow:             row.FreqWindow.Int32,
		FreqWindowAny:          row.FreqWindow.Int32,
		TargetCountries:        SliceToMap(row.TargetCountries),
		BudgetCampaignKey:      "budget:campaign:" + idStr,
		CampaignSyncKey:        "budget:sync:campaign:" + idStr,
		CustomerSyncKey:        "budget:sync:customer:" + customerIDStr,
		FcapKeyPrefix:          fcapPrefix,
		DailySpendKeyPrefix:    "budget:daily_spent:campaign:" + idStr + ":",
		FraudThresholdPass:     uint8(row.FraudThresholdPass),
		FraudThresholdSuspect:  uint8(row.FraudThresholdSuspect),
		FraudThresholdIVT:      uint8(row.FraudThresholdIvt),
		FraudThresholdBlock:    uint8(row.FraudThresholdBlock),
		GhostIVTEnabled:        row.GhostIvtEnabled,
		BehaviorFlags:          campaignmodel.BehaviorFlags(row.BehaviorFlags),
		RequireConsentPurposes: row.RequireConsentPurposes,
	}
}

// dbQuerier exposes the backing DBTX so sync workers can commit spend with sync_idempotency.
type dbQuerier struct {
	db.Querier
	dbtx db.DBTX
}

func (q *dbQuerier) DB() db.DBTX {
	return q.dbtx
}

// NewCampaignRepoWithDB wraps querier with a DB handle for idempotent sync flushes.
func NewCampaignRepoWithDB(dbtx db.DBTX, queries db.Querier) *CampaignRepo {
	return &CampaignRepo{queries: &dbQuerier{Querier: queries, dbtx: dbtx}}
}

// CampaignRepo loads campaigns and applies idempotent budget sync updates from Redis.
type CampaignRepo struct {
	queries db.Querier
}

func NewCampaignRepo(queries db.Querier) *CampaignRepo {
	return &CampaignRepo{queries: queries}
}

// GetByID loads full campaign fields for budget cache reload paths.
func (r *CampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*campaignmodel.Campaign, error) {
	row, err := r.queries.GetCampaignFull(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}
	return campaignFromDBRow(row), nil
}

func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status campaignmodel.CampaignStatus) error {
	_, err := r.queries.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		Status: db.CampaignStatusType(status),
	})
	return err
}

// UpdateSpend applies a Redis sync delta exactly once per sync transaction id.
func (r *CampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
			ID:           pgtype.UUID{Bytes: id, Valid: true},
			CurrentSpend: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		CurrentSpend: amount,
	})
	if err != nil {
		return err
	}

	// Phase 1.5.1: decrease reserved_amount proportionally to spend flushed
	sharder := NewStaticSlotSharder(config.ExpectedRedisShardCount)
	shardID := int16(sharder.GetShard(id))
	_ = q.DecreaseCampaignQuotaReserved(ctx, db.DecreaseCampaignQuotaReservedParams{
		ShardID:        shardID,
		CampaignID:     pgtype.UUID{Bytes: id, Valid: true},
		ReservedAmount: amount,
	})

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}

// ListActive returns all active campaigns for reconciliation and admin paths.
func (r *CampaignRepo) ListActive(ctx context.Context) ([]*campaignmodel.Campaign, error) {
	rows, err := r.queries.ListActiveCampaigns(ctx)
	if err != nil {
		return nil, err
	}

	campaigns := make([]*campaignmodel.Campaign, len(rows))
	for i, row := range rows {
		campaigns[i] = campaignFromDBRow(row)
	}
	return campaigns, nil
}

// CustomerRepo loads customers and applies idempotent balance sync updates from Redis.
type CustomerRepo struct {
	queries db.Querier
}

func NewCustomerRepo(queries db.Querier) *CustomerRepo {
	return &CustomerRepo{queries: queries}
}

// NewCustomerRepoWithDB wraps querier with a DB handle for idempotent sync flushes.
func NewCustomerRepoWithDB(dbtx db.DBTX, queries db.Querier) *CustomerRepo {
	return &CustomerRepo{queries: &dbQuerier{Querier: queries, dbtx: dbtx}}
}

func (r *CustomerRepo) GetByID(ctx context.Context, id uuid.UUID) (*campaignmodel.Customer, error) {
	row, err := r.queries.GetCustomerByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	return &campaignmodel.Customer{
		ID:       id,
		Name:     row.Name,
		Balance:  row.Balance,
		Currency: row.Currency,
	}, nil
}

// UpdateBalance applies a Redis sync delta exactly once per sync transaction id.
func (r *CustomerRepo) UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
			ID:      pgtype.UUID{Bytes: id, Valid: true},
			Balance: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
		ID:      pgtype.UUID{Bytes: id, Valid: true},
		Balance: amount,
	})
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}
