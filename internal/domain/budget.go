package domain

import (
	"context"
	"github.com/google/uuid"
)

// BudgetManager is the hot-path spend gate so ingestion can deduct click cost without importing storage details.
type BudgetManager interface {
	CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount int64) (bool, error)
}
