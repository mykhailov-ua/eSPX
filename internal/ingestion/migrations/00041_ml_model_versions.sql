-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ml_model_versions (
    id TEXT PRIMARY KEY,
    artifact_hash TEXT NOT NULL,
    metrics_json JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'SYNCING', 'ACTIVE', 'RETIRED')) DEFAULT 'DRAFT',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ml_shard_sync_state (
    shard_id INT NOT NULL,
    model_version TEXT NOT NULL REFERENCES ml_model_versions(id) ON DELETE CASCADE,
    phase TEXT NOT NULL CHECK (phase IN ('ACTIVE', 'SYNC', 'ROLLBACK')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, model_version)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS ml_shard_sync_state;
DROP TABLE IF EXISTS ml_model_versions;
-- +goose StatementEnd
