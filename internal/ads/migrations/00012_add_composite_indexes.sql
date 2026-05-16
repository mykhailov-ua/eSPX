-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_campaigns_cust_status ON campaigns(customer_id, status);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at DESC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_campaigns_cust_status;
DROP INDEX IF EXISTS idx_customers_created_at;
-- +goose StatementEnd
