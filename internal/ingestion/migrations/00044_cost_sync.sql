-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS cost_sync_credentials (
    id BIGSERIAL PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(id),
    network TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT '',
    access_token_encrypted BYTEA,
    refresh_token_encrypted BYTEA,
    api_key_encrypted BYTEA,
    extra_config JSONB NOT NULL DEFAULT '{}',
    token_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, network)
);

CREATE INDEX idx_cost_sync_credentials_customer ON cost_sync_credentials(customer_id);

CREATE TABLE IF NOT EXISTS campaign_costs (
    id BIGSERIAL PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(id),
    campaign_id UUID NOT NULL REFERENCES campaigns(id),
    cost_date DATE NOT NULL,
    network TEXT NOT NULL,
    placement_id TEXT NOT NULL DEFAULT '',
    adset_id TEXT NOT NULL DEFAULT '',
    ad_id TEXT NOT NULL DEFAULT '',
    line_type TEXT NOT NULL DEFAULT 'spend',
    amount_micro BIGINT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    amount_usd_micro BIGINT NOT NULL,
    ingest_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, campaign_id, cost_date, network, placement_id)
);

CREATE INDEX idx_campaign_costs_customer_date ON campaign_costs(customer_id, cost_date);
CREATE INDEX idx_campaign_costs_campaign_date ON campaign_costs(campaign_id, cost_date);

CREATE TABLE IF NOT EXISTS cost_sync_runs (
    id BIGSERIAL PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(id),
    network TEXT NOT NULL,
    cost_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    rows_imported INT NOT NULL DEFAULT 0,
    total_amount_usd_micro BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    trigger_source TEXT NOT NULL DEFAULT 'cron',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_cost_sync_runs_customer ON cost_sync_runs(customer_id, started_at DESC);

CREATE TABLE IF NOT EXISTS cost_sync_ecb_rates (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    usd_per_unit_micro BIGINT NOT NULL,
    PRIMARY KEY (rate_date, currency)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS cost_sync_ecb_rates;
DROP TABLE IF EXISTS cost_sync_runs;
DROP TABLE IF EXISTS campaign_costs;
DROP TABLE IF EXISTS cost_sync_credentials;
-- +goose StatementEnd
