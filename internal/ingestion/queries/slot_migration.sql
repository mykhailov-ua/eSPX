-- name: InsertSlotMigrationIfAbsent :exec
INSERT INTO redis_slot_migration (version, slot, source_shard, target_shard, state)
VALUES ($1, $2, $3, $4, 'pending')
ON CONFLICT (version, slot) DO NOTHING;

-- name: UpsertSlotMigration :exec
INSERT INTO redis_slot_migration (version, slot, source_shard, target_shard, state, campaigns_total, campaigns_copied, last_error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (version, slot) DO UPDATE SET
    source_shard = EXCLUDED.source_shard,
    target_shard = EXCLUDED.target_shard,
    state = EXCLUDED.state,
    campaigns_total = EXCLUDED.campaigns_total,
    campaigns_copied = EXCLUDED.campaigns_copied,
    last_error = EXCLUDED.last_error,
    updated_at = NOW();

-- name: GetSlotMigration :one
SELECT version, slot, source_shard, target_shard, state, campaigns_total, campaigns_copied, last_error, updated_at
FROM redis_slot_migration
WHERE version = $1 AND slot = $2;

-- name: ListSlotMigrationsByVersion :many
SELECT version, slot, source_shard, target_shard, state, campaigns_total, campaigns_copied, last_error, updated_at
FROM redis_slot_migration
WHERE version = $1
ORDER BY slot ASC;

-- name: UpdateSlotMigrationProgress :exec
UPDATE redis_slot_migration
SET campaigns_total = $3,
    campaigns_copied = $4,
    state = $5,
    last_error = $6,
    updated_at = NOW()
WHERE version = $1 AND slot = $2;

-- name: UpdateSlotMigrationState :exec
UPDATE redis_slot_migration
SET state = $3,
    last_error = $4,
    updated_at = NOW()
WHERE version = $1 AND slot = $2;

-- name: ListDrainingSlotMigrations :many
SELECT version, slot, source_shard, target_shard, state, campaigns_total, campaigns_copied, last_error, updated_at
FROM redis_slot_migration
WHERE state = 'draining'
ORDER BY version ASC, slot ASC;

-- name: ListSlotMigrationsByState :many
SELECT version, slot, source_shard, target_shard, state, campaigns_total, campaigns_copied, last_error, updated_at
FROM redis_slot_migration
WHERE state = ANY($1::redis_slot_migration_state[])
ORDER BY version ASC, slot ASC;

-- name: GetMaxDraftVersionWithMigrating :one
SELECT COALESCE(MAX(m.version), 0)::INT AS max_version
FROM redis_slot_map AS m
JOIN redis_slot_map_meta AS meta ON meta.id = 1
WHERE m.version > meta.active_version AND m.state = 'MIGRATING';
