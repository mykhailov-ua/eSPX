-- +goose Up
-- +goose StatementBegin
ALTER TABLE notifier.notifications
  ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS dedup_key TEXT;

CREATE INDEX IF NOT EXISTS idx_notifications_dedup_active
  ON notifier.notifications (dedup_key, created_at DESC)
  WHERE dedup_key IS NOT NULL AND status IN ('PENDING', 'PROCESSING');

CREATE INDEX IF NOT EXISTS idx_notifications_processing_stale
  ON notifier.notifications (claimed_at)
  WHERE status = 'PROCESSING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS notifier.idx_notifications_processing_stale;
DROP INDEX IF EXISTS notifier.idx_notifications_dedup_active;

ALTER TABLE notifier.notifications
  DROP COLUMN IF EXISTS dedup_key,
  DROP COLUMN IF EXISTS claimed_at;
-- +goose StatementEnd
