package ivtdetector

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const idempotencyPrefix = "ivt:block:"

// IdempotencyStore guards exactly-once blacklist enqueue via sync_idempotency.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

// NewIdempotencyStore binds Postgres for sync_idempotency claims.
func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

// TryClaim inserts an idempotency key and reports whether this caller won the race.
func (store *IdempotencyStore) TryClaim(ctx context.Context, ip string) (bool, error) {
	if store == nil || store.pool == nil {
		return false, fmt.Errorf("idempotency store: nil pool")
	}
	if ip == "" {
		return false, ErrInvalidIP
	}

	tag, err := store.pool.Exec(ctx,
		"INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING",
		idempotencyPrefix+ip,
	)
	if err != nil {
		return false, fmt.Errorf("claim idempotency key: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Release removes a claim so a failed management call can be retried on the next cycle.
func (store *IdempotencyStore) Release(ctx context.Context, ip string) error {
	if store == nil || store.pool == nil {
		return fmt.Errorf("idempotency store: nil pool")
	}
	if ip == "" {
		return ErrInvalidIP
	}
	_, err := store.pool.Exec(ctx, "DELETE FROM sync_idempotency WHERE id = $1", idempotencyPrefix+ip)
	if err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	return nil
}

// TryClaimFraudEnforcement inserts an idempotency key in ml_enforcement_idempotency and reports whether this caller won the race.
func (store *IdempotencyStore) TryClaimFraudEnforcement(ctx context.Context, ip string, modelVersion string, reason string) (bool, error) {
	if store == nil || store.pool == nil {
		return false, fmt.Errorf("idempotency store: nil pool")
	}
	if ip == "" {
		return false, ErrInvalidIP
	}

	tag, err := store.pool.Exec(ctx,
		"INSERT INTO ml_enforcement_idempotency (ip, model_version, reason) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
		ip, modelVersion, reason,
	)
	if err != nil {
		return false, fmt.Errorf("claim ml idempotency key: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ReleaseFraudEnforcement removes a claim from ml_enforcement_idempotency.
func (store *IdempotencyStore) ReleaseFraudEnforcement(ctx context.Context, ip string, modelVersion string, reason string) error {
	if store == nil || store.pool == nil {
		return fmt.Errorf("idempotency store: nil pool")
	}
	if ip == "" {
		return ErrInvalidIP
	}
	_, err := store.pool.Exec(ctx,
		"DELETE FROM ml_enforcement_idempotency WHERE ip = $1 AND model_version = $2 AND reason = $3",
		ip, modelVersion, reason,
	)
	if err != nil {
		return fmt.Errorf("release ml idempotency key: %w", err)
	}
	return nil
}

// HasClaim reports whether an IP was already flagged by a prior detector cycle.
func (store *IdempotencyStore) HasClaim(ctx context.Context, ip string) (bool, error) {
	if store == nil || store.pool == nil {
		return false, fmt.Errorf("idempotency store: nil pool")
	}
	var exists bool
	err := store.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM sync_idempotency WHERE id = $1)",
		idempotencyPrefix+ip,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check idempotency key: %w", err)
	}
	return exists, nil
}
