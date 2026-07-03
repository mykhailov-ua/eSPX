package repo

import (
	"context"

	"espx/internal/ads/db"
	"espx/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CustomerRepo loads customers and applies idempotent balance sync updates from Redis.
type CustomerRepo struct {
	queries db.Querier
}

// NewCustomerRepo wraps sqlc queries for customer persistence.
func NewCustomerRepo(queries db.Querier) *CustomerRepo {
	return &CustomerRepo{queries: queries}
}

// NewCustomerRepoFromPool creates a repo that records balance sync in sync_idempotency.
func NewCustomerRepoFromPool(pool *pgxpool.Pool) *CustomerRepo {
	return NewCustomerRepo(&querierWithDB{Querier: db.New(pool), dbtx: pool})
}

// GetByID loads a customer record by primary key.
func (r *CustomerRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Customer, error) {
	row, err := r.queries.GetCustomerByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	return &domain.Customer{
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
