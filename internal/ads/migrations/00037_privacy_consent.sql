-- +goose Up
-- +goose StatementBegin
-- M6: consent events, user consent state, campaign consent requirements, erasure requests.

CREATE TABLE IF NOT EXISTS consent_events (
    id              BIGSERIAL PRIMARY KEY,
    user_id_hash    BYTEA NOT NULL,
    purposes        SMALLINT NOT NULL CHECK (purposes >= 0 AND purposes <= 65535),
    source          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_consent_events_user_hash ON consent_events (user_id_hash, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_consent_events_created_at ON consent_events (created_at);

CREATE TABLE IF NOT EXISTS user_consent_state (
    user_id_hash        BYTEA PRIMARY KEY,
    ad_storage          BOOLEAN NOT NULL DEFAULT FALSE,
    analytics_storage   BOOLEAN NOT NULL DEFAULT FALSE,
    purposes            SMALLINT NOT NULL DEFAULT 0 CHECK (purposes >= 0 AND purposes <= 65535),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS require_consent_purposes SMALLINT NOT NULL DEFAULT 0
        CHECK (require_consent_purposes >= 0 AND require_consent_purposes <= 65535);

CREATE TYPE privacy_erasure_status AS ENUM (
    'PENDING',
    'PG_ANONYMIZED',
    'REDIS_PURGED',
    'CH_PURGED',
    'COMPLETED',
    'FAILED'
);

CREATE TABLE IF NOT EXISTS privacy_erasure_requests (
    id              UUID PRIMARY KEY,
    user_id_hash    BYTEA NOT NULL,
    subject_user_id TEXT NOT NULL DEFAULT '',
    status          privacy_erasure_status NOT NULL DEFAULT 'PENDING',
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_privacy_erasure_status ON privacy_erasure_requests (status, updated_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS privacy_erasure_requests;
DROP TYPE IF EXISTS privacy_erasure_status;
ALTER TABLE campaigns DROP COLUMN IF EXISTS require_consent_purposes;
DROP TABLE IF EXISTS user_consent_state;
DROP TABLE IF EXISTS consent_events;
-- +goose StatementEnd
