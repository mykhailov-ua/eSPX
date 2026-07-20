-- +goose Up
-- +goose StatementBegin
CREATE TABLE payment.crypto_holds (
  id                  UUID PRIMARY KEY,
  payment_intent_id   UUID NOT NULL REFERENCES payment.payment_intents(id) ON DELETE CASCADE,
  customer_id         UUID NOT NULL,
  amount_micro        BIGINT NOT NULL CHECK (amount_micro > 0),
  currency            TEXT NOT NULL DEFAULT 'USDT',
  tx_hash             TEXT NOT NULL,
  status              TEXT NOT NULL DEFAULT 'HELD', -- 'HELD', 'RELEASED', 'FRAUD_BLOCKED'
  release_at          TIMESTAMPTZ NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_crypto_holds_status_release
  ON payment.crypto_holds (status, release_at)
  WHERE status = 'HELD';

CREATE INDEX idx_crypto_holds_customer
  ON payment.crypto_holds (customer_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS payment.crypto_holds;
-- +goose StatementEnd
