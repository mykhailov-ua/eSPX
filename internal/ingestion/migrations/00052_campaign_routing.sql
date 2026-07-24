-- 00052_campaign_routing.sql: M2 elastic triplets — per-campaign routing with routing_epoch.
-- Replaces campaign_shard_assignment as the canonical triplet home for hot campaigns.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS campaign_routing (
    campaign_id UUID PRIMARY KEY REFERENCES campaigns(id) ON DELETE CASCADE,
    home_slot SMALLINT NOT NULL CHECK (home_slot >= 0 AND home_slot <= 1023),
    primary_a_shard SMALLINT NOT NULL CHECK (primary_a_shard >= 0),
    primary_b_shard SMALLINT NOT NULL CHECK (primary_b_shard >= 0),
    reserve_shard SMALLINT NOT NULL CHECK (reserve_shard >= 0),
    routing_epoch BIGINT NOT NULL DEFAULT 0,
    h_ema DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    c_ema DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_campaign_routing_primary_a
    ON campaign_routing (primary_a_shard);

ALTER TABLE redis_slot_map_meta
    ADD COLUMN IF NOT EXISTS routing_epoch BIGINT NOT NULL DEFAULT 0;

-- Migrate existing triplet assignments from M1 table.
INSERT INTO campaign_routing (
    campaign_id, home_slot, primary_a_shard, primary_b_shard, reserve_shard, h_ema, c_ema
)
SELECT
    csa.campaign_id,
    (('x' || substr(replace(csa.campaign_id::text, '-', ''), 1, 8))::bit(32)::int & 1023)::SMALLINT,
    csa.primary_a_shard,
    csa.primary_b_shard,
    csa.reserve_shard,
    csa.h_ema,
    csa.c_ema
FROM campaign_shard_assignment csa
ON CONFLICT (campaign_id) DO NOTHING;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE redis_slot_map_meta DROP COLUMN IF EXISTS routing_epoch;
DROP TABLE IF EXISTS campaign_routing;
-- +goose StatementEnd
