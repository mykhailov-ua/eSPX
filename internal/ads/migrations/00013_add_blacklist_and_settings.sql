-- +goose Up
-- +goose StatementBegin
CREATE TABLE ip_blacklist (
    id BIGSERIAL PRIMARY KEY,
    ip TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ip_blacklist_ip ON ip_blacklist(ip);

CREATE TABLE system_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS system_settings;
DROP TABLE IF EXISTS ip_blacklist;
-- +goose StatementEnd
