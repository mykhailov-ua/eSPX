-- +goose Up
-- +goose StatementBegin
ALTER TABLE customers ADD CONSTRAINT balance_non_negative CHECK (balance >= 0);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE customers DROP CONSTRAINT balance_non_negative;
-- +goose StatementEnd
