-- Composite/partial indexes for ledger recon, credit scoring, and shard drain workers.
-- Targets seq scans flagged by EXPLAIN (ANALYZE, BUFFERS) audit on balance_ledger and campaigns.

-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_ledger_topup_recent
    ON balance_ledger (customer_id, created_at DESC)
    WHERE type IN ('TOPUP', 'PAYMENT_TOPUP');

CREATE INDEX IF NOT EXISTS idx_ledger_fee_created
    ON balance_ledger (created_at, campaign_id)
    WHERE type = 'FEE';

CREATE INDEX IF NOT EXISTS idx_campaigns_draining_updated
    ON campaigns (updated_at ASC)
    WHERE status = 'DRAINING';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_campaigns_draining_updated;
DROP INDEX IF EXISTS idx_ledger_fee_created;
DROP INDEX IF EXISTS idx_ledger_topup_recent;
-- +goose StatementEnd
