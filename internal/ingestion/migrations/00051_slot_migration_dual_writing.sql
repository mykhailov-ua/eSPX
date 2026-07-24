-- 00051_slot_migration_dual_writing.sql: dual-write catch-up state for M1-08 hot-slot cutover.

-- +goose Up
-- +goose StatementBegin
ALTER TYPE redis_slot_migration_state ADD VALUE IF NOT EXISTS 'dual_writing' AFTER 'copied';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- PostgreSQL cannot remove enum values; downgrade is a no-op.
-- +goose StatementEnd
