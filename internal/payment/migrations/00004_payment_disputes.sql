-- +goose Up
-- +goose StatementBegin
ALTER TYPE payment.payment_intent_status ADD VALUE IF NOT EXISTS 'DISPUTED';

CREATE TYPE payment.dispute_status AS ENUM (
  'OPEN',
  'FUNDS_WITHDRAWN',
  'FUNDS_REINSTATED',
  'WON',
  'LOST'
);

CREATE TABLE payment.payment_disputes (
  id                    UUID PRIMARY KEY,
  payment_intent_id     UUID NOT NULL,
  provider              TEXT NOT NULL DEFAULT 'stripe',
  provider_dispute_id   TEXT NOT NULL,
  amount_micro          BIGINT NOT NULL CHECK (amount_micro > 0),
  status                payment.dispute_status NOT NULL DEFAULT 'OPEN',
  withdrawn_amount_micro BIGINT NOT NULL DEFAULT 0,
  reinstated_amount_micro BIGINT NOT NULL DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_payment_disputes_provider_dispute UNIQUE (provider, provider_dispute_id)
);

CREATE INDEX idx_payment_disputes_intent
  ON payment.payment_disputes (payment_intent_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS payment.payment_disputes;
DROP TYPE IF EXISTS payment.dispute_status;
-- PostgreSQL does not support removing enum values from payment_intent_status.
-- +goose StatementEnd
