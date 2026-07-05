-- +goose Up
-- +goose StatementBegin
CREATE TYPE payment.refund_status AS ENUM (
  'PENDING',
  'SUCCEEDED',
  'FAILED'
);

ALTER TABLE payment.payment_intents
  ADD COLUMN IF NOT EXISTS refunded_amount_micro BIGINT NOT NULL DEFAULT 0;

ALTER TABLE payment.payment_intents
  ADD CONSTRAINT chk_refunded_not_exceed_amount
  CHECK (refunded_amount_micro >= 0 AND refunded_amount_micro <= amount_micro);

CREATE TABLE payment.payment_refunds (
  id                  UUID PRIMARY KEY,
  payment_intent_id   UUID NOT NULL,
  provider            TEXT NOT NULL DEFAULT 'stripe',
  provider_refund_id  TEXT NOT NULL,
  amount_micro        BIGINT NOT NULL CHECK (amount_micro > 0),
  status              payment.refund_status NOT NULL DEFAULT 'PENDING',
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_payment_refunds_provider_refund UNIQUE (provider, provider_refund_id)
);

CREATE INDEX idx_payment_refunds_intent
  ON payment.payment_refunds (payment_intent_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS payment.payment_refunds;
ALTER TABLE payment.payment_intents DROP CONSTRAINT IF EXISTS chk_refunded_not_exceed_amount;
ALTER TABLE payment.payment_intents DROP COLUMN IF EXISTS refunded_amount_micro;
DROP TYPE IF EXISTS payment.refund_status;
-- +goose StatementEnd
