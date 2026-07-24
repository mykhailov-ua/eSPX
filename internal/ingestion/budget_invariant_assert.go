package ingestion

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

	budgetKey := budgetCampaignKey(campaignID)
	syncKey := campaignSyncKey(campaignID)

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

// VerifyBudgetInvariant returns nil when R5 holds within tolerance for one campaign.
func VerifyBudgetInvariant(ctx context.Context, pool *pgxpool.Pool, rdb redis.Cmdable, campaignID uuid.UUID) error {
	snap, err := ReadBudgetInvariant(ctx, pool, rdb, campaignID)
	if err != nil {
		return err
	}
	redisSpend := snap.BudgetLimit - snap.RedisRemaining
	pgPlusSync := snap.PGCurrentSpend + snap.SyncDelta
	diff := redisSpend - pgPlusSync
	if diff < -budgetInvariantToleranceMicro || diff > budgetInvariantToleranceMicro {
		return fmt.Errorf(
			"budget invariant violated for campaign %s: diff=%d tolerance=%d",
			campaignID, diff, budgetInvariantToleranceMicro,
		)
	}
	return nil
}

// AssertBudgetInvariant verifies CHAOS.md R5: (budget_limit - redis_remaining) = pg_current_spend + sync_delta.
func AssertBudgetInvariant(t testing.TB, ctx context.Context, pool *pgxpool.Pool, rdb redis.Cmdable, campaignID uuid.UUID) {
	t.Helper()

	if err := VerifyBudgetInvariant(ctx, pool, rdb, campaignID); err != nil {
		t.Fatal(err)
	}
}
