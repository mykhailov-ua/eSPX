-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS campaign_shard_assignment (
    campaign_id UUID PRIMARY KEY REFERENCES campaigns(id) ON DELETE CASCADE,
    primary_a_shard SMALLINT NOT NULL CHECK (primary_a_shard >= 0),
    primary_b_shard SMALLINT NOT NULL CHECK (primary_b_shard >= 0),
    reserve_shard SMALLINT NOT NULL CHECK (reserve_shard >= 0),
    h_ema DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    c_ema DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS campaign_shard_assignment;
-- +goose StatementEnd
