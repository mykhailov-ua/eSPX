package campaignmodel

import (
	"context"
	"github.com/google/uuid"
)

// BudgetManager is the hot-path spend gate so ingestion can deduct click cost without importing storage details.
type BudgetManager interface {
	// CheckAndSpend atomically reserves spend so parallel clicks cannot overdraw customer or campaign budgets.
	CheckAndSpend(ctx context.Context, customerID, campaignID uuid.UUID, clickID string, amount int64) (bool, error)
}
