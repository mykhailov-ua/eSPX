-- +goose Up
-- +goose StatementBegin
ALTER TYPE campaign_status_type ADD VALUE IF NOT EXISTS 'DRAINING';
ALTER TYPE campaign_status_type ADD VALUE IF NOT EXISTS 'DELETED';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Enum values cannot be removed in PostgreSQL without recreating the type.
-- We keep them as is for backward compatibility.
-- +goose StatementEnd
