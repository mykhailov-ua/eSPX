-- 00012_add_composite_indexes.sql: composite indexes for the management query patterns.
-- idx_campaigns_cust_status: covers LIST campaigns by customer filtered by status;
--   used by the management API when listing active/paused campaigns per customer.
-- idx_customers_created_at: covers customer list ordering by creation date DESC;
--   used by admin pagination queries in the management service.

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
