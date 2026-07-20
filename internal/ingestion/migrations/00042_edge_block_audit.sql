-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS edge_block_audit (
    id BIGSERIAL PRIMARY KEY,
    ip TEXT NOT NULL,
    reason_id TEXT NOT NULL,
    ttl INT,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_edge_block_audit_ip ON edge_block_audit(ip);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS edge_block_audit;
-- +goose StatementEnd
