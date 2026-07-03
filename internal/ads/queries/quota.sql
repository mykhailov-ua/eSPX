-- quota.sql: sqlc queries for Distributed Quotas Postgres control plane (Phase 1.1).
-- ReserveChunk orchestration lives in QuotaRepo (transaction + sync_idempotency);
-- these queries provide row locks and DML primitives.

-- name: LockCampaignBudgetForQuota :one
SELECT budget_limit, current_spend
FROM campaigns
WHERE id = $1
FOR UPDATE;

-- name: LockCampaignQuota :one
SELECT shard_id, campaign_id, reserved_amount, chunk_size, updated_at
FROM campaign_quotas
WHERE shard_id = $1 AND campaign_id = $2
FOR UPDATE;

-- name: InsertCampaignQuota :exec
INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size)
VALUES ($1, $2, $3, $4);

-- name: IncreaseCampaignQuotaReserved :exec
UPDATE campaign_quotas
SET reserved_amount = reserved_amount + $3,
    chunk_size = $4,
    updated_at = NOW()
WHERE shard_id = $1 AND campaign_id = $2;

-- name: GetCampaignQuota :one
SELECT shard_id, campaign_id, reserved_amount, chunk_size, updated_at
FROM campaign_quotas
WHERE shard_id = $1 AND campaign_id = $2;

-- name: DecreaseCampaignQuotaReserved :exec
UPDATE campaign_quotas
SET reserved_amount = GREATEST(0, reserved_amount - $3),
    updated_at = NOW()
WHERE shard_id = $1 AND campaign_id = $2;

-- name: SumCampaignReservedByCampaign :one
SELECT COALESCE(reserved_amount, 0)::bigint AS reserved_amount
FROM campaign_quotas
WHERE campaign_id = $1;
