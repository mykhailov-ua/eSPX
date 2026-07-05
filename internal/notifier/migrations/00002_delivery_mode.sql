-- +goose Up
-- +goose StatementBegin
CREATE TYPE notifier.delivery_mode AS ENUM (
  'FALLBACK',
  'BROADCAST'
);

ALTER TABLE notifier.notifications
  ADD COLUMN delivery_mode notifier.delivery_mode NOT NULL DEFAULT 'FALLBACK',
  ADD COLUMN broadcast_providers TEXT[];

CREATE INDEX idx_notifications_pending_broadcast
  ON notifier.notifications (status, delivery_mode, created_at)
  WHERE status = 'PENDING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS notifier.idx_notifications_pending_broadcast;

ALTER TABLE notifier.notifications
  DROP COLUMN IF EXISTS broadcast_providers,
  DROP COLUMN IF EXISTS delivery_mode;

DROP TYPE IF EXISTS notifier.delivery_mode;
-- +goose StatementEnd
