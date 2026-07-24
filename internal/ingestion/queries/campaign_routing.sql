-- name: UpsertCampaignRouting :one
INSERT INTO campaign_routing (
    campaign_id, home_slot, primary_a_shard, primary_b_shard, reserve_shard,
    routing_epoch, h_ema, c_ema, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (campaign_id) DO UPDATE SET
    home_slot = EXCLUDED.home_slot,
    primary_a_shard = EXCLUDED.primary_a_shard,
    primary_b_shard = EXCLUDED.primary_b_shard,
    reserve_shard = EXCLUDED.reserve_shard,
    routing_epoch = EXCLUDED.routing_epoch,
    h_ema = EXCLUDED.h_ema,
    c_ema = EXCLUDED.c_ema,
    updated_at = NOW()
RETURNING *;

-- name: GetCampaignRouting :one
SELECT * FROM campaign_routing
WHERE campaign_id = $1;

-- name: DeleteCampaignRouting :exec
DELETE FROM campaign_routing
WHERE campaign_id = $1;

-- name: ListCampaignRoutingByShard :many
SELECT * FROM campaign_routing
WHERE primary_a_shard = $1
ORDER BY c_ema DESC
LIMIT $2;

-- name: BumpGlobalRoutingEpoch :one
UPDATE redis_slot_map_meta
SET routing_epoch = routing_epoch + 1,
    updated_at = NOW()
WHERE id = 1
RETURNING routing_epoch, active_version;

-- name: GetGlobalRoutingEpoch :one
SELECT routing_epoch, active_version
FROM redis_slot_map_meta
WHERE id = 1;
