package domain

import (
	"context"
	"github.com/google/uuid"
)

// Customer is the billing account aggregate shared by management, ledger, and campaign reservation flows.
type Customer struct {
	ID       uuid.UUID
	Name     string
	Balance  int64
	Currency string
}

// CustomerRepository isolates balance reads and mutations from sqlc and pool wiring in upper layers.
type CustomerRepository interface {
	GetByID(ctx context.Context, id uuid.UUID) (*Customer, error)
	UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error
}
