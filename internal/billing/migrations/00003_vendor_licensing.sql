-- +goose Up
-- +goose StatementBegin

CREATE SCHEMA IF NOT EXISTS vendor;

CREATE TABLE vendor.licenses (
    license_key    TEXT PRIMARY KEY,
    customer_name  TEXT NOT NULL,
    plan_code      TEXT NOT NULL, -- starter|growth|enterprise
    valid_from     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_until    TIMESTAMPTZ NOT NULL,
    grace_days     INT NOT NULL DEFAULT 7,
    limits_json    JSONB NOT NULL,
    features_json  JSONB NOT NULL,
    support_tier   TEXT NOT NULL DEFAULT 'standard',
    revoked        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE vendor.deployments (
    deployment_id  UUID PRIMARY KEY,
    license_key    TEXT NOT NULL REFERENCES vendor.licenses(license_key) ON DELETE CASCADE,
    fingerprint    TEXT NOT NULL,
    activated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE vendor.renewal_events (
    id             BIGSERIAL PRIMARY KEY,
    license_key    TEXT NOT NULL REFERENCES vendor.licenses(license_key) ON DELETE CASCADE,
    new_valid_until TIMESTAMPTZ NOT NULL,
    renewed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS vendor.renewal_events;
DROP TABLE IF EXISTS vendor.deployments;
DROP TABLE IF EXISTS vendor.licenses;
DROP SCHEMA IF EXISTS vendor;
-- +goose StatementEnd
