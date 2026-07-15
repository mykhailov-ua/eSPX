-- +goose Up
-- +goose StatementBegin
ALTER TYPE ledger_type ADD VALUE IF NOT EXISTS 'PAYMENT_REFUND';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- PostgreSQL does not support removing enum values.
-- +goose StatementEnd
