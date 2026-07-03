-- 00029_redis_slot_migration.sql: per-slot migration progress for Phase 2.3 orchestrator.

-- +goose Up
-- +goose StatementBegin
CREATE TYPE redis_slot_migration_state AS ENUM (
    'pending',
    'copying',
    'copied',
    'draining',
    'done',
    'failed'
);

CREATE TABLE redis_slot_migration (
    version          INT NOT NULL,
    slot             SMALLINT NOT NULL CHECK (slot >= 0 AND slot <= 1023),
    source_shard     SMALLINT NOT NULL CHECK (source_shard >= 0),
    target_shard     SMALLINT NOT NULL CHECK (target_shard >= 0),
    state            redis_slot_migration_state NOT NULL DEFAULT 'pending',
    campaigns_total  INT NOT NULL DEFAULT 0,
    campaigns_copied INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (version, slot)
);

CREATE INDEX idx_redis_slot_migration_state ON redis_slot_migration (state);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS redis_slot_migration;
DROP TYPE IF EXISTS redis_slot_migration_state;
-- +goose StatementEnd
