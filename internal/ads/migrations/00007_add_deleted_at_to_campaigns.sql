-- +goose Up
-- +goose StatementBegin
ALTER TABLE campaigns ADD COLUMN deleted_at TIMESTAMPTZ;
CREATE INDEX idx_campaigns_deleted_at ON campaigns(deleted_at) WHERE deleted_at IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE campaigns DROP COLUMN deleted_at;
-- +goose StatementEnd
