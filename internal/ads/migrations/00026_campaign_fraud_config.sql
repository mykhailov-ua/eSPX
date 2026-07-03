-- Per-campaign fraud score tier boundaries and behavioral filter toggles for the hot-path registry.

-- +goose Up
-- +goose StatementBegin
ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS fraud_threshold_pass SMALLINT NOT NULL DEFAULT 30
        CHECK (fraud_threshold_pass BETWEEN 0 AND 100),
    ADD COLUMN IF NOT EXISTS fraud_threshold_suspect SMALLINT NOT NULL DEFAULT 60
        CHECK (fraud_threshold_suspect BETWEEN 0 AND 100),
    ADD COLUMN IF NOT EXISTS fraud_threshold_ivt SMALLINT NOT NULL DEFAULT 80
        CHECK (fraud_threshold_ivt BETWEEN 0 AND 100),
    ADD COLUMN IF NOT EXISTS fraud_threshold_block SMALLINT NOT NULL DEFAULT 100
        CHECK (fraud_threshold_block BETWEEN 0 AND 100),
    ADD COLUMN IF NOT EXISTS ghost_ivt_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS behavior_flags INTEGER NOT NULL DEFAULT 0;

ALTER TABLE campaigns
    ADD CONSTRAINT campaigns_fraud_thresholds_ordered CHECK (
        fraud_threshold_pass <= fraud_threshold_suspect
        AND fraud_threshold_suspect <= fraud_threshold_ivt
        AND fraud_threshold_ivt <= fraud_threshold_block
    );
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP CONSTRAINT IF EXISTS campaigns_fraud_thresholds_ordered;
ALTER TABLE campaigns
    DROP COLUMN IF EXISTS behavior_flags,
    DROP COLUMN IF EXISTS ghost_ivt_enabled,
    DROP COLUMN IF EXISTS fraud_threshold_block,
    DROP COLUMN IF EXISTS fraud_threshold_ivt,
    DROP COLUMN IF EXISTS fraud_threshold_suspect,
    DROP COLUMN IF EXISTS fraud_threshold_pass;
-- +goose StatementEnd
