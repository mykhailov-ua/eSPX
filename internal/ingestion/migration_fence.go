package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	redis "github.com/redis/go-redis/v9"
)

const (
	// MigrationFenceKeyPrefix blocks debits on a draining source shard during slot copy.
	MigrationFenceKeyPrefix = "budget:migration_fence:"
	// BudgetFrozenKeyPrefix blocks debits when management freezes spend via outbox.
	BudgetFrozenKeyPrefix = "budget:frozen:"
	migrationFenceTTL     = 24 * time.Hour
)

// MigrationFenceRedisKey returns the Redis key that rejects Lua debits when present.
func MigrationFenceRedisKey(campaignID uuid.UUID) string {
	return MigrationFenceKeyPrefix + campaignID.String()
}

// BudgetFrozenRedisKey returns the Redis key set by BUDGET_FREEZE outbox events.
func BudgetFrozenRedisKey(campaignID uuid.UUID) string {
	return BudgetFrozenKeyPrefix + campaignID.String()
}

// BumpMigrationFences increments Postgres migration_gen and sets fence keys on the source shard.
func BumpMigrationFences(
	ctx context.Context,
	pool *pgxpool.Pool,
	src redis.Cmdable,
	campaignIDs []uuid.UUID,
) error {
	if pool == nil || src == nil || len(campaignIDs) == 0 {
		return nil
	}
	pgIDs := make([]uuid.UUID, len(campaignIDs))
	copy(pgIDs, campaignIDs)

	rows, err := pool.Query(ctx, `
		UPDATE campaigns
		SET migration_gen = migration_gen + 1
		WHERE id = ANY($1::uuid[])
		RETURNING id, migration_gen`,
		pgIDs,
	)
	if err != nil {
		return fmt.Errorf("bump migration_gen: %w", err)
	}
	defer rows.Close()

	type fenceRow struct {
		id  uuid.UUID
		gen int64
	}
	var bumped []fenceRow
	for rows.Next() {
		var r fenceRow
		if err := rows.Scan(&r.id, &r.gen); err != nil {
			return err
		}
		bumped = append(bumped, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	pipe := src.Pipeline()
	for _, r := range bumped {
		key := MigrationFenceRedisKey(r.id)
		pipe.Set(ctx, key, r.gen, migrationFenceTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("set migration fence keys: %w", err)
	}
	return nil
}

// SetBudgetFrozen marks a campaign shard as spend-frozen until the key is deleted.
func SetBudgetFrozen(ctx context.Context, rdb redis.Cmdable, campaignID uuid.UUID) error {
	if rdb == nil {
		return fmt.Errorf("nil redis client")
	}
	return rdb.Set(ctx, BudgetFrozenRedisKey(campaignID), "1", 0).Err()
}

// ClearBudgetFrozen removes the spend-freeze marker for a campaign.
func ClearBudgetFrozen(ctx context.Context, rdb redis.Cmdable, campaignID uuid.UUID) error {
	if rdb == nil {
		return fmt.Errorf("nil redis client")
	}
	return rdb.Del(ctx, BudgetFrozenRedisKey(campaignID)).Err()
}
