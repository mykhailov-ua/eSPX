-- +goose Up
-- +goose StatementBegin
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_lookup TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_key_lookup ON api_keys(key_lookup) WHERE key_lookup IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_api_keys_key_lookup;
ALTER TABLE api_keys DROP COLUMN IF EXISTS key_lookup;
-- +goose StatementEnd
