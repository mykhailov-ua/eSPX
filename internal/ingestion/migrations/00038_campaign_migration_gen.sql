-- 00038_campaign_migration_gen.sql: per-campaign fencing token for slot migration (M1).
-- migration_gen is bumped before copying keys; source shard gets budget:migration_fence:{id}.

-- +goose Up
-- +goose StatementBegin
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS migration_gen BIGINT NOT NULL DEFAULT 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN IF EXISTS migration_gen;
-- +goose StatementEnd
