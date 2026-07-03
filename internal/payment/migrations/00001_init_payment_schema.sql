-- +goose Up
-- +goose StatementBegin
-- payment schema isolates checkout state from ads ledger so webhook retries do not contend with RTB hot tables.
CREATE SCHEMA IF NOT EXISTS payment;

CREATE TYPE payment.payment_intent_status AS ENUM (
  'CREATED',
  'PENDING_PROVIDER',
  'PROCESSING',
  'SUCCEEDED',
  'FAILED',
  'CANCELLED',
  'REFUNDED',
  'SETTLEMENT_FAILED'
);

CREATE TYPE payment.webhook_event_status AS ENUM (
  'RECEIVED',
  'PROCESSED',
  'FAILED',
  'IGNORED'
);

CREATE TYPE payment.outbox_status AS ENUM (
  'PENDING',
  'PROCESSING',
  'PROCESSED',
  'DEAD'
);

CREATE TABLE payment.payment_intents (
  id                UUID PRIMARY KEY,
  customer_id       UUID NOT NULL,
  amount_micro      BIGINT NOT NULL CHECK (amount_micro > 0),
  currency          TEXT NOT NULL DEFAULT 'USD',
  status            payment.payment_intent_status NOT NULL DEFAULT 'CREATED',
  provider          TEXT NOT NULL DEFAULT 'stripe',
  provider_ref      TEXT,
  idempotency_key   TEXT NOT NULL,
  metadata          JSONB NOT NULL DEFAULT '{}',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_payment_intents_idempotency UNIQUE (idempotency_key)
);

CREATE INDEX idx_payment_intents_customer_created
  ON payment.payment_intents (customer_id, created_at DESC);

CREATE INDEX idx_payment_intents_provider_ref
  ON payment.payment_intents (provider, provider_ref)
  WHERE provider_ref IS NOT NULL;

CREATE INDEX idx_payment_intents_status
  ON payment.payment_intents (status)
  WHERE status IN ('PENDING_PROVIDER', 'PROCESSING', 'SUCCEEDED');

CREATE TABLE payment.webhook_events (
  id                 BIGSERIAL PRIMARY KEY,
  provider           TEXT NOT NULL,
  provider_event_id  TEXT NOT NULL,
  event_type         TEXT NOT NULL,
  payload_hash       BYTEA NOT NULL,
  payload_redacted   JSONB,
  status             payment.webhook_event_status NOT NULL DEFAULT 'RECEIVED',
  error_message      TEXT,
  received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at       TIMESTAMPTZ,

  CONSTRAINT uq_webhook_events_provider_event UNIQUE (provider, provider_event_id)
);

CREATE INDEX idx_webhook_events_status_received
  ON payment.webhook_events (status, received_at)
  WHERE status = 'RECEIVED';

CREATE TABLE payment.payment_outbox (
  id            BIGSERIAL PRIMARY KEY,
  event_type    TEXT NOT NULL,
  payload       JSONB NOT NULL,
  status        payment.outbox_status NOT NULL DEFAULT 'PENDING',
  lease_until   TIMESTAMPTZ,
  attempts      INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at  TIMESTAMPTZ
);

CREATE INDEX idx_payment_outbox_pending
  ON payment.payment_outbox (created_at)
  WHERE status = 'PENDING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS payment.payment_outbox;
DROP TABLE IF EXISTS payment.webhook_events;
DROP TABLE IF EXISTS payment.payment_intents;
DROP TYPE IF EXISTS payment.outbox_status;
DROP TYPE IF EXISTS payment.webhook_event_status;
DROP TYPE IF EXISTS payment.payment_intent_status;
DROP SCHEMA IF EXISTS payment;
-- +goose StatementEnd
