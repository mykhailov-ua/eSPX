-- +goose Up
ALTER TYPE notifier.notification_status ADD VALUE IF NOT EXISTS 'PROCESSING';

-- +goose Down
-- enum values cannot be removed safely; no-op
