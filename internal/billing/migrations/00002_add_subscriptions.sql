-- +goose Up
-- +goose StatementBegin

CREATE TABLE billing.subscription_plans (
    code            TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    limits_json     JSONB NOT NULL,
    features_json   JSONB NOT NULL,
    base_fee_micro  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE billing.customer_subscriptions (
    customer_id     UUID PRIMARY KEY REFERENCES public.customers(id) ON DELETE CASCADE,
    plan_code       TEXT NOT NULL REFERENCES billing.subscription_plans(code),
    status          TEXT NOT NULL,
    period_start    DATE NOT NULL,
    period_end      DATE,
    overrides_json  JSONB,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE billing.usage_meters (
    customer_id     UUID NOT NULL REFERENCES public.customers(id) ON DELETE CASCADE,
    meter           TEXT NOT NULL,
    period          DATE NOT NULL,
    value           BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, meter, period)
);

CREATE TABLE billing.usage_daily (
    customer_id   UUID NOT NULL REFERENCES public.customers(id) ON DELETE CASCADE,
    usage_date    DATE NOT NULL,
    meter         TEXT NOT NULL,
    value         BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, usage_date, meter)
);

CREATE TABLE billing.license_status (
    deployment_id      UUID PRIMARY KEY,
    license_id         UUID NOT NULL,
    plan_code          TEXT NOT NULL,
    valid_until        TIMESTAMPTZ NOT NULL,
    state              TEXT NOT NULL,
    entitlements_json  JSONB NOT NULL,
    last_verified_at   TIMESTAMPTZ NOT NULL,
    last_refresh_error TEXT
);

-- Seed plans
INSERT INTO billing.subscription_plans (code, display_name, limits_json, features_json, base_fee_micro) VALUES
(
  'basic',
  'Basic Plan',
  '{"max_active_campaigns": 50, "max_rps": 10000, "max_requests_per_day": 500000, "max_events_per_month": 5000000, "max_regions": 1, "max_api_keys": 2, "max_export_chunk_bytes": 1048576, "quota_reset_timezone": "UTC"}'::jsonb,
  '{"rtb_live": false, "ml_fraud_boost": false, "multi_region": false, "slot_migration": false}'::jsonb,
  100000000 -- 10 units / month
),
(
  'pro',
  'Pro Plan',
  '{"max_active_campaigns": 500, "max_rps": 50000, "max_requests_per_day": 10000000, "max_events_per_month": 50000000, "max_regions": 1, "max_api_keys": 5, "max_export_chunk_bytes": 5242880, "quota_reset_timezone": "UTC"}'::jsonb,
  '{"rtb_live": false, "ml_fraud_boost": false, "multi_region": false, "slot_migration": false}'::jsonb,
  500000000 -- 50 units / month
),
(
  'enterprise',
  'Enterprise Plan',
  '{"max_active_campaigns": 999999, "max_rps": 200000, "max_requests_per_day": 999999999, "max_events_per_month": 9999999999, "max_regions": 5, "max_api_keys": 99, "max_export_chunk_bytes": 104857600, "quota_reset_timezone": "UTC"}'::jsonb,
  '{"rtb_live": true, "ml_fraud_boost": true, "multi_region": true, "slot_migration": true}'::jsonb,
  2000000000 -- 200 units / month
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS billing.license_status;
DROP TABLE IF EXISTS billing.usage_daily;
DROP TABLE IF EXISTS billing.usage_meters;
DROP TABLE IF EXISTS billing.customer_subscriptions;
DROP TABLE IF EXISTS billing.subscription_plans;
-- +goose StatementEnd
