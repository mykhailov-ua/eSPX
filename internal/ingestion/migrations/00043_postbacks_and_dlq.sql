-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS postback_configs (
    campaign_id UUID PRIMARY KEY,
    provider TEXT NOT NULL,
    url_template TEXT NOT NULL,
    api_token_encrypted BYTEA NOT NULL,
    target_event TEXT NOT NULL DEFAULT 'conversion',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS postback_dispatches (
    id BIGSERIAL PRIMARY KEY,
    idempotency_hash TEXT NOT NULL UNIQUE,
    campaign_id UUID NOT NULL,
    click_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'SENT',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS postback_dlq (
    id BIGSERIAL PRIMARY KEY,
    outbox_event_id BIGINT NOT NULL,
    campaign_id UUID NOT NULL,
    click_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    failures_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    status TEXT NOT NULL DEFAULT 'FAILED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_postback_dlq_status ON postback_dlq(status);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS postback_dlq;
DROP TABLE IF EXISTS postback_dispatches;
DROP TABLE IF EXISTS postback_configs;
-- +goose StatementEnd
