-- +goose Up
-- +goose StatementBegin
ALTER TABLE outbox_events ADD COLUMN processing_started_at TIMESTAMPTZ;

CREATE INDEX idx_outbox_processing_stale ON outbox_events (processing_started_at)
    WHERE status = 'PROCESSING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_outbox_processing_stale;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS processing_started_at;
-- +goose StatementEnd
