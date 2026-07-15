-- 00027_campaign_quotas.sql: Postgres control-plane rows for Distributed Quotas (Phase 1.1).
-- reserved_amount tracks micro-units allocated to Redis budget:quota:{cid} but not yet
-- reflected in campaigns.current_spend. Invariant enforced at reserve time:
--   current_spend + reserved_amount + chunk <= budget_limit
-- shard_id must match StaticSlotSharder(campaign_id); no user_id routing.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE campaign_quotas (
    shard_id        SMALLINT NOT NULL,
    campaign_id     UUID NOT NULL,
    reserved_amount BIGINT NOT NULL DEFAULT 0 CHECK (reserved_amount >= 0),
    chunk_size      BIGINT NOT NULL DEFAULT 0 CHECK (chunk_size >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, campaign_id)
);

CREATE INDEX idx_campaign_quotas_campaign_id ON campaign_quotas (campaign_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS campaign_quotas;
-- +goose StatementEnd
