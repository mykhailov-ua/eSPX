package campaignmodel

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
	// GetByID loads the billing account snapshot before reservations and ledger writes proceed.
	GetByID(ctx context.Context, id uuid.UUID) (*Customer, error)
	// UpdateBalance applies idempotent balance deltas so retried settlements do not double-credit accounts.
	UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error
}
