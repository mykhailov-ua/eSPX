-- +goose Up
-- +goose StatementBegin
ALTER TYPE ledger_type ADD VALUE IF NOT EXISTS 'PAYMENT_TOPUP';

ALTER TABLE balance_ledger
  ADD COLUMN IF NOT EXISTS payment_intent_id UUID;

CREATE INDEX IF NOT EXISTS idx_ledger_payment_intent
  ON balance_ledger (payment_intent_id)
  WHERE payment_intent_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ledger_payment_intent;
ALTER TABLE balance_ledger DROP COLUMN IF EXISTS payment_intent_id;
-- +goose StatementEnd
