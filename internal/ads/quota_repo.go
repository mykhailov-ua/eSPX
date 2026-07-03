package ads

import (
	"context"
	"errors"
	"fmt"

	"espx/internal/ads/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrQuotaBudgetExceeded is returned when current_spend + reserved + chunk exceeds budget_limit.
	ErrQuotaBudgetExceeded = errors.New("quota reserve exceeds budget_limit")
	// ErrQuotaInvalidChunk is returned when chunk_size is not positive.
	ErrQuotaInvalidChunk = errors.New("chunk_size must be positive")
)

const quotaIdempotencyPrefix = "quota:"

// CampaignShardID returns the shard index for a campaign via StaticSlot routing (no user_id).
func CampaignShardID(sharder Sharder, campaignID uuid.UUID) int {
	return sharder.GetShard(campaignID)
}

// QuotaRepo reserves Postgres quota chunks for Distributed Quotas (Phase 1.1).
type QuotaRepo struct {
	pool *pgxpool.Pool
}

// NewQuotaRepo constructs a quota repository backed by a pgx pool for transactional reserve.
func NewQuotaRepo(pool *pgxpool.Pool) *QuotaRepo {
	return &QuotaRepo{pool: pool}
}

// ReserveChunkResult is the outcome of a successful quota chunk reservation.
type ReserveChunkResult struct {
	ShardID        int16
	CampaignID     uuid.UUID
	ReservedAmount int64
	ChunkSize      int64
	AlreadyApplied bool
}

// ReserveChunk atomically reserves chunk_size micro-units against campaigns.budget_limit.
// Idempotency keys are stored in sync_idempotency with a quota: prefix; retries with the
// same key return AlreadyApplied without increasing reserved_amount.
func (r *QuotaRepo) ReserveChunk(
	ctx context.Context,
	sharder Sharder,
	campaignID uuid.UUID,
	chunkSize int64,
	idempotencyKey string,
) (ReserveChunkResult, error) {
	var zero ReserveChunkResult
	if chunkSize <= 0 {
		return zero, ErrQuotaInvalidChunk
	}
	if r.pool == nil {
		return zero, fmt.Errorf("quota repo: nil pool")
	}

	shardID := int16(CampaignShardID(sharder, campaignID))
	pgCampaignID := ToUUID(campaignID)

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return zero, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	idemID := quotaIdempotencyPrefix + idempotencyKey
	tag, err := tx.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", idemID)
	if err != nil {
		return zero, err
	}
	if tag.RowsAffected() == 0 {
		q := db.New(tx)
		row, qerr := q.GetCampaignQuota(ctx, db.GetCampaignQuotaParams{
			ShardID:    shardID,
			CampaignID: pgCampaignID,
		})
		if qerr != nil {
			if errors.Is(qerr, pgx.ErrNoRows) {
				return ReserveChunkResult{
					ShardID:        shardID,
					CampaignID:     campaignID,
					ReservedAmount: 0,
					ChunkSize:      chunkSize,
					AlreadyApplied: true,
				}, nil
			}
			return zero, qerr
		}
		if err := tx.Commit(ctx); err != nil {
			return zero, err
		}
		return ReserveChunkResult{
			ShardID:        row.ShardID,
			CampaignID:     campaignID,
			ReservedAmount: row.ReservedAmount,
			ChunkSize:      row.ChunkSize,
			AlreadyApplied: true,
		}, nil
	}

	q := db.New(tx)

	budget, err := q.LockCampaignBudgetForQuota(ctx, pgCampaignID)
	if err != nil {
		return zero, err
	}

	var reserved int64
	quotaExists := true
	quotaRow, err := q.LockCampaignQuota(ctx, db.LockCampaignQuotaParams{
		ShardID:    shardID,
		CampaignID: pgCampaignID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			quotaExists = false
		} else {
			return zero, err
		}
	} else {
		reserved = quotaRow.ReservedAmount
	}

	if budget.CurrentSpend+reserved+chunkSize > budget.BudgetLimit {
		return zero, ErrQuotaBudgetExceeded
	}

	newReserved := reserved + chunkSize
	if !quotaExists {
		err = q.InsertCampaignQuota(ctx, db.InsertCampaignQuotaParams{
			ShardID:        shardID,
			CampaignID:     pgCampaignID,
			ReservedAmount: chunkSize,
			ChunkSize:      chunkSize,
		})
	} else {
		err = q.IncreaseCampaignQuotaReserved(ctx, db.IncreaseCampaignQuotaReservedParams{
			ShardID:        shardID,
			CampaignID:     pgCampaignID,
			ReservedAmount: chunkSize,
			ChunkSize:      chunkSize,
		})
	}
	if err != nil {
		return zero, err
	}

	if err := tx.Commit(ctx); err != nil {
		return zero, err
	}

	return ReserveChunkResult{
		ShardID:        shardID,
		CampaignID:     campaignID,
		ReservedAmount: newReserved,
		ChunkSize:      chunkSize,
	}, nil
}

// GetQuota loads the campaign_quotas row for observability and reconciliation paths.
func (r *QuotaRepo) GetQuota(ctx context.Context, sharder Sharder, campaignID uuid.UUID) (db.CampaignQuota, error) {
	shardID := int16(CampaignShardID(sharder, campaignID))
	q := db.New(r.pool)
	return q.GetCampaignQuota(ctx, db.GetCampaignQuotaParams{
		ShardID:    shardID,
		CampaignID: ToUUID(campaignID),
	})
}

// ReleaseChunk decreases the reserved_amount in Postgres (e.g. on Redis refill failure or timeout).
func (r *QuotaRepo) ReleaseChunk(
	ctx context.Context,
	sharder Sharder,
	campaignID uuid.UUID,
	chunkSize int64,
) error {
	if r.pool == nil {
		return fmt.Errorf("quota repo: nil pool")
	}
	shardID := int16(CampaignShardID(sharder, campaignID))
	pgCampaignID := ToUUID(campaignID)

	q := db.New(r.pool)
	return q.IncreaseCampaignQuotaReserved(ctx, db.IncreaseCampaignQuotaReservedParams{
		ShardID:        shardID,
		CampaignID:     pgCampaignID,
		ReservedAmount: -chunkSize,
		ChunkSize:      chunkSize,
	})
}

// QuotaShardForCampaign exposes shard_id derivation for tests and QuotaManager wiring.
func QuotaShardForCampaign(sharder Sharder, campaignID uuid.UUID) int16 {
	return int16(CampaignShardID(sharder, campaignID))
}
