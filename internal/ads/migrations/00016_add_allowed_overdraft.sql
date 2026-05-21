-- +goose Up
-- +goose StatementBegin
ALTER TABLE customers ADD COLUMN allowed_overdraft NUMERIC(15,2) NOT NULL DEFAULT 0.00;
ALTER TABLE customers DROP CONSTRAINT IF EXISTS balance_non_negative;
ALTER TABLE customers ADD CONSTRAINT chk_allowed_balance CHECK (balance + allowed_overdraft >= 0.00);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE customers DROP CONSTRAINT IF EXISTS chk_allowed_balance;
ALTER TABLE customers DROP COLUMN IF EXISTS allowed_overdraft;
ALTER TABLE customers ADD CONSTRAINT balance_non_negative CHECK (balance >= 0.00);
-- +goose StatementEnd
