package ingestion

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// RewarmCampaignBudgetKeys seeds authoritative budget keys from Postgres on the target shard.
// PG re-warm is the cutover source of truth for budget counters; COPY handles ephemeral keys.
func RewarmCampaignBudgetKeys(
	ctx context.Context,
	pool *pgxpool.Pool,
	dst redis.Cmdable,
	campaignIDs []uuid.UUID,
) error {
	if pool == nil || dst == nil || len(campaignIDs) == 0 {
		return nil
	}
	for _, id := range campaignIDs {
		var budgetLimit, currentSpend int64
		err := pool.QueryRow(ctx,
			`SELECT budget_limit, current_spend FROM campaigns WHERE id = $1`, ToUUID(id),
		).Scan(&budgetLimit, &currentSpend)
		if err != nil {
			return fmt.Errorf("rewarm read campaign %s: %w", id, err)
		}
		remaining := budgetLimit - currentSpend
		if remaining < 0 {
			remaining = 0
		}
		key := budgetCampaignKey(id)
		if err := dst.Set(ctx, key, remaining, budgetKeyTTL).Err(); err != nil {
			return fmt.Errorf("rewarm set %q: %w", key, err)
		}
	}
	return nil
}
