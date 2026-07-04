package ads

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const budgetInvariantToleranceMicro = int64(1)

// BudgetInvariantSnapshot captures Redis budget, sync delta, and Postgres spend for one campaign.
type BudgetInvariantSnapshot struct {
	CampaignID     uuid.UUID
	BudgetLimit    int64
	RedisRemaining int64
	SyncDelta      int64
	PGCurrentSpend int64
}

// ReadBudgetInvariant loads budget_limit/current_spend from Postgres and hot-path Redis keys.
func ReadBudgetInvariant(ctx context.Context, pool *pgxpool.Pool, rdb redis.Cmdable, campaignID uuid.UUID) (BudgetInvariantSnapshot, error) {
	var snap BudgetInvariantSnapshot
	snap.CampaignID = campaignID

	err := pool.QueryRow(ctx,
		`SELECT budget_limit, current_spend FROM campaigns WHERE id = $1`, ToUUID(campaignID),
	).Scan(&snap.BudgetLimit, &snap.PGCurrentSpend)
	if err != nil {
		return snap, fmt.Errorf("read campaign spend from postgres: %w", err)
	}

	budgetKey := "budget:campaign:" + campaignID.String()
	syncKey := "budget:sync:campaign:" + campaignID.String()

	syncDelta, err := rdb.Get(ctx, syncKey).Int64()
	if err == redis.Nil {
		syncDelta = 0
	} else if err != nil {
		return snap, fmt.Errorf("read %s: %w", syncKey, err)
	}
	snap.SyncDelta = syncDelta

	remaining, err := rdb.Get(ctx, budgetKey).Int64()
	if err == redis.Nil {
		// Unwarmed key: treat as PG-aligned remaining (not zero spend).
		remaining = snap.BudgetLimit - snap.PGCurrentSpend - snap.SyncDelta
		if remaining < 0 {
			remaining = 0
		}
	} else if err != nil {
		return snap, fmt.Errorf("read %s: %w", budgetKey, err)
	}
	snap.RedisRemaining = remaining

	return snap, nil
}

// AssertBudgetInvariant verifies GUIDE_CHAOS_RELIABILITY R5: (budget_limit - redis_remaining) = pg_current_spend + sync_delta.
func AssertBudgetInvariant(t testing.TB, ctx context.Context, pool *pgxpool.Pool, rdb redis.Cmdable, campaignID uuid.UUID) {
	t.Helper()

	snap, err := ReadBudgetInvariant(ctx, pool, rdb, campaignID)
	if err != nil {
		t.Fatalf("read budget invariant: %v", err)
	}

	redisSpend := snap.BudgetLimit - snap.RedisRemaining
	pgPlusSync := snap.PGCurrentSpend + snap.SyncDelta
	diff := redisSpend - pgPlusSync

	if diff < -budgetInvariantToleranceMicro || diff > budgetInvariantToleranceMicro {
		t.Fatalf(
			"budget invariant violated for campaign %s: budget_limit=%d redis_remaining=%d sync_delta=%d pg_current_spend=%d redis_spend=%d pg_plus_sync=%d diff=%d (tolerance<=%d)",
			campaignID,
			snap.BudgetLimit,
			snap.RedisRemaining,
			snap.SyncDelta,
			snap.PGCurrentSpend,
			redisSpend,
			pgPlusSync,
			diff,
			budgetInvariantToleranceMicro,
		)
	}
}
