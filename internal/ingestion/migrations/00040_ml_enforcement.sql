-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ml_enforcement_idempotency (
    ip TEXT NOT NULL,
    model_version TEXT NOT NULL,
    reason TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ip, model_version, reason)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS ml_enforcement_idempotency;
-- +goose StatementEnd
