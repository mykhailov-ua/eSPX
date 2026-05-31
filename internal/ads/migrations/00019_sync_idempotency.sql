-- 00019_sync_idempotency.sql: idempotency store for SyncWorker budget commit operations.
-- Each SyncWorker commit generates a UUID txID; before updating current_spend or
-- customer balance the repository inserts the txID with ON CONFLICT DO NOTHING.
-- If the txID already exists (retry after crash between Redis commit and Postgres write)
-- the UPDATE is skipped, guaranteeing exactly-once spend persistence per sync cycle.
-- Rows are never purged; storage growth is bounded by the budget:dirty_* set cardinality.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE sync_idempotency (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE sync_idempotency;
-- +goose StatementEnd
