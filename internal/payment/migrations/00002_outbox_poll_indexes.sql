-- +goose Up
-- +goose StatementBegin
CREATE INDEX idx_payment_outbox_stale_processing
  ON payment.payment_outbox (lease_until, created_at)
  WHERE status = 'PROCESSING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS payment.idx_payment_outbox_stale_processing;
-- +goose StatementEnd
