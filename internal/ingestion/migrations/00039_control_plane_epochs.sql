-- +goose Up
CREATE TABLE IF NOT EXISTS control_plane_epochs (
    epoch_id BIGINT PRIMARY KEY,
    config_hash BYTEA NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_control_plane_epochs_created
    ON control_plane_epochs (created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_control_plane_epochs_created;
DROP TABLE IF EXISTS control_plane_epochs;
