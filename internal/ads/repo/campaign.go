package repo

import (
	"context"

	"espx/internal/ads/db"
	"espx/internal/ads/sharding"
	"espx/internal/config"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querierWithDB exposes the backing pool for transactional sync_idempotency writes.
type querierWithDB struct {
	db.Querier
	dbtx db.DBTX
}

func (q *querierWithDB) DB() db.DBTX {
	return q.dbtx
}

// CampaignRepo loads campaigns and applies idempotent budget sync updates from Redis.
type CampaignRepo struct {
	queries db.Querier
}

// NewCampaignRepo wraps sqlc queries for campaign persistence.
func NewCampaignRepo(queries db.Querier) *CampaignRepo {
	return &CampaignRepo{queries: queries}
}

// NewCampaignRepoFromPool creates a repo that records budget sync in sync_idempotency.
func NewCampaignRepoFromPool(pool *pgxpool.Pool) *CampaignRepo {
	return NewCampaignRepo(&querierWithDB{Querier: db.New(pool), dbtx: pool})
}

// GetByID loads full campaign fields for budget cache reload paths.
func (r *CampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	row, err := r.queries.GetCampaignFull(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}
	return campaignFromDBRow(row), nil
}

// UpdateStatus changes campaign lifecycle state in Postgres.
func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
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

	sharder := sharding.NewStaticSlotSharder(config.ExpectedRedisShardCount)
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
func (r *CampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	rows, err := r.queries.ListActiveCampaigns(ctx)
	if err != nil {
		return nil, err
	}

	campaigns := make([]*domain.Campaign, len(rows))
	for i, row := range rows {
		campaigns[i] = campaignFromDBRow(row)
	}
	return campaigns, nil
}
