-- +goose Up
-- +goose StatementBegin
ALTER TABLE ip_blacklist ADD COLUMN expires_at TIMESTAMPTZ;

CREATE INDEX idx_ip_blacklist_expires_at ON ip_blacklist(expires_at)
    WHERE expires_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_ip_blacklist_expires_at;
ALTER TABLE ip_blacklist DROP COLUMN IF EXISTS expires_at;
-- +goose StatementEnd
