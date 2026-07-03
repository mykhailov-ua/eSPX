-- +goose Up
-- +goose StatementBegin
CREATE UNIQUE INDEX idx_ledger_payment_topup_intent
  ON balance_ledger (payment_intent_id)
  WHERE type = 'PAYMENT_TOPUP' AND payment_intent_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ledger_payment_topup_intent;
-- +goose StatementEnd
