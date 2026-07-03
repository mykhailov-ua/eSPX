-- +goose Up
-- +goose StatementBegin
CREATE SCHEMA IF NOT EXISTS notifier;

CREATE TYPE notifier.provider AS ENUM (
  'TELEGRAM',
  'SLACK',
  'SMTP',
  'SMS'
);

CREATE TYPE notifier.notification_status AS ENUM (
  'PENDING',
  'SENT',
  'FAILED'
);

CREATE TABLE notifier.notifications (
  id             UUID PRIMARY KEY,
  provider       notifier.provider NOT NULL,
  recipient      TEXT NOT NULL,
  title          TEXT,
  body           TEXT NOT NULL,
  status         notifier.notification_status NOT NULL DEFAULT 'PENDING',
  retry_count    INT NOT NULL DEFAULT 0,
  error_message  TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_pending_created
  ON notifier.notifications (status, created_at)
  WHERE status = 'PENDING';

CREATE INDEX idx_notifications_created_at
  ON notifier.notifications (created_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS notifier.notifications;
DROP TYPE IF EXISTS notifier.notification_status;
DROP TYPE IF EXISTS notifier.provider;
DROP SCHEMA IF EXISTS notifier;
-- +goose StatementEnd
