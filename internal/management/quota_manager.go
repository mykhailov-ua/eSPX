package management

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"espx/internal/ads"
	"espx/internal/ads/db"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// QuotaManager coordinates Distributed Quota refills and initial allocations (Phase 1.4).
type QuotaManager struct {
	svc          *Service
	quotaRepo    *ads.QuotaRepo
	pollInterval time.Duration
	chunkSize    int64
	thresholdPct int
}

// NewQuotaManager constructs a QuotaManager with config-driven chunk sizes and thresholds.
func NewQuotaManager(svc *Service) *QuotaManager {
	var chunkSize int64
	var thresholdPct int
	if svc.cfg != nil {
		chunkSize = svc.cfg.QuotaChunkSize
		thresholdPct = svc.cfg.QuotaRefillThresholdPct
	}
	if chunkSize <= 0 {
		chunkSize = 5000000 // default 5,000,000 micro-units ($5.00)
	}
	if thresholdPct <= 0 {
		thresholdPct = 20
	}
	return &QuotaManager{
		svc:          svc,
		quotaRepo:    ads.NewQuotaRepo(svc.GetPool()),
		pollInterval: 100 * time.Millisecond,
		chunkSize:    chunkSize,
		thresholdPct: thresholdPct,
	}
}

// Start runs the refill poll loop and periodic initial quota warming until context is cancelled.
func (qm *QuotaManager) Start(ctx context.Context) {
	ticker := time.NewTicker(qm.pollInterval)
	defer ticker.Stop()

	warmTicker := time.NewTicker(5 * time.Second)
	defer warmTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if qm.svc.cfg != nil && (qm.svc.cfg.QuotaMode == "shadow" || qm.svc.cfg.QuotaMode == "live") {
				qm.pollRefills(ctx)
			}
		case <-warmTicker.C:
			if qm.svc.cfg != nil && (qm.svc.cfg.QuotaMode == "shadow" || qm.svc.cfg.QuotaMode == "live") {
				qm.warmActiveCampaignQuotas(ctx)
			}
		}
	}
}

func (qm *QuotaManager) pollRefills(ctx context.Context) {
	for shardIdx, rdb := range qm.svc.rdbs {
		campaignIDs, err := rdb.SPopN(ctx, "budget:refill_needed", 100).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) {
				slog.Error("failed to pop from budget:refill_needed", "shard", shardIdx, "error", err)
			}
			continue
		}

		for _, idStr := range campaignIDs {
			campaignID, err := uuid.Parse(idStr)
			if err != nil {
				slog.Error("failed to parse campaign ID from refill_needed", "id", idStr, "error", err)
				continue
			}

			if err := qm.refillCampaign(ctx, campaignID, shardIdx, rdb); err != nil {
				slog.Error("failed to refill campaign", "campaign_id", campaignID, "error", err)
			}
		}
	}
}

func (qm *QuotaManager) refillCampaign(ctx context.Context, campaignID uuid.UUID, shardIdx int, rdb redis.UniversalClient) error {
	lockKey := fmt.Sprintf("budget:refill_lock:%s", campaignID)
	claimed, err := rdb.GetDel(ctx, lockKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return fmt.Errorf("claim budget:refill_lock: %w", err)
	}
	if claimed == "" {
		return nil
	}

	requeue := func() {
		_ = rdb.Set(ctx, lockKey, "1", 10*time.Second).Err()
		_ = rdb.SAdd(ctx, "budget:refill_needed", campaignID.String()).Err()
	}

	idempotencyKey := uuid.New().String()
	res, err := qm.quotaRepo.ReserveChunk(ctx, qm.svc.sharder, campaignID, qm.chunkSize, idempotencyKey)
	if err != nil {
		if errors.Is(err, ads.ErrQuotaBudgetExceeded) {
			slog.Warn("campaign budget exceeded during refill", "campaign_id", campaignID)
			return nil
		}
		requeue()
		return fmt.Errorf("failed to reserve chunk in Postgres: %w", err)
	}

	if res.AlreadyApplied {
		return nil
	}

	shadow := qm.svc.cfg != nil && qm.svc.cfg.QuotaMode == "shadow"
	if shadow {
		slog.Info("shadow quota refill reserved in Postgres only",
			"campaign_id", campaignID, "shard", shardIdx, "chunk_size", qm.chunkSize,
			"reserved_amount", res.ReservedAmount)
		return nil
	}

	quotaKey := fmt.Sprintf("budget:quota:%s", campaignID)
	_, err = rdb.IncrBy(ctx, quotaKey, qm.chunkSize).Result()
	if err != nil {
		slog.Error("failed to increment budget:quota in Redis, rolling back Postgres reservation", "campaign_id", campaignID, "error", err)
		if rollbackErr := qm.quotaRepo.ReleaseChunk(ctx, qm.svc.sharder, campaignID, qm.chunkSize); rollbackErr != nil {
			slog.Error("failed to rollback Postgres reservation", "campaign_id", campaignID, "error", rollbackErr)
		}
		requeue()
		return fmt.Errorf("failed to increment budget:quota in Redis: %w", err)
	}

	slog.Info("successfully refilled campaign quota", "campaign_id", campaignID, "shard", shardIdx, "chunk_size", qm.chunkSize)
	return nil
}

func (qm *QuotaManager) warmActiveCampaignQuotas(ctx context.Context) {
	campaignRepo := ads.NewCampaignRepo(db.New(qm.svc.GetPool()))
	campaigns, err := campaignRepo.ListActive(ctx)
	if err != nil {
		slog.Error("failed to list active campaigns for quota warming", "error", err)
		return
	}

	for _, camp := range campaigns {
		if camp == nil {
			continue
		}
		campaignID := camp.ID
		shardIdx := qm.svc.sharder.GetShard(campaignID)
		if shardIdx < 0 || shardIdx >= len(qm.svc.rdbs) {
			continue
		}
		rdb := qm.svc.rdbs[shardIdx]

		quotaKey := fmt.Sprintf("budget:quota:%s", campaignID)
		lockKey := fmt.Sprintf("budget:refill_lock:%s", campaignID)

		exists, err := rdb.Exists(ctx, quotaKey).Result()
		if err != nil {
			continue
		}

		shadow := qm.svc.cfg != nil && qm.svc.cfg.QuotaMode == "shadow"
		if exists == 0 {
			if shadow {
				pgQuota, qerr := qm.quotaRepo.GetQuota(ctx, qm.svc.sharder, campaignID)
				if qerr == nil && pgQuota.ReservedAmount > 0 {
					continue
				}
			}

			locked, err := rdb.SetNX(ctx, lockKey, "1", 10*time.Second).Result()
			if err != nil || !locked {
				continue
			}

			exists, err = rdb.Exists(ctx, quotaKey).Result()
			if err != nil || exists > 0 {
				_ = rdb.Del(ctx, lockKey).Err()
				continue
			}

			slog.Info("initializing quota for campaign", "campaign_id", campaignID, "shadow", shadow)
			idempotencyKey := fmt.Sprintf("init-quota-%s", campaignID)
			_, err = qm.quotaRepo.ReserveChunk(ctx, qm.svc.sharder, campaignID, qm.chunkSize, idempotencyKey)
			if err != nil {
				_ = rdb.Del(ctx, lockKey).Err()
				if !errors.Is(err, ads.ErrQuotaBudgetExceeded) {
					slog.Error("failed to reserve initial chunk in Postgres", "campaign_id", campaignID, "error", err)
				}
				continue
			}

			actualChunk := qm.chunkSize

			if shadow {
				_ = rdb.Del(ctx, lockKey).Err()
				slog.Info("shadow quota warm reserved in Postgres only",
					"campaign_id", campaignID, "shard", shardIdx, "chunk_size", actualChunk)
				continue
			}

			_, err = rdb.IncrBy(ctx, quotaKey, actualChunk).Result()
			if err != nil {
				slog.Error("failed to set initial budget:quota in Redis, rolling back Postgres", "campaign_id", campaignID, "error", err)
				_ = qm.quotaRepo.ReleaseChunk(ctx, qm.svc.sharder, campaignID, actualChunk)
				_ = rdb.Del(ctx, lockKey).Err()
				continue
			}

			_ = rdb.Del(ctx, lockKey).Err()
			slog.Info("successfully initialized campaign quota", "campaign_id", campaignID, "shard", shardIdx, "chunk_size", actualChunk)
		}
	}
}
