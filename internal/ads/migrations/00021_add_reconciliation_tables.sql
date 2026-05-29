-- +goose Up
-- +goose StatementBegin

-- Extend ledger enum to support automated financial corrections from reconciliation process.
-- RECONCILIATION_ADJUST allows the system to credit or debit customer balances when discrepancies
-- between ClickHouse raw events / Redis edge state and the authoritative PostgreSQL ledger are detected.
-- This is critical for financial integrity in a distributed AdTech billing system.
ALTER TYPE ledger_type ADD VALUE IF NOT EXISTS 'RECONCILIATION_ADJUST';

-- recon_runs tracks each execution of the reconciliation job for a closed time window.
-- Windows are deliberately chosen as "closed" (e.g. T-2h to T-1h) to avoid racing with
-- inflight events still being settled by the SyncWorker / Processor pool.
CREATE TABLE IF NOT EXISTS recon_runs (
    id BIGSERIAL PRIMARY KEY,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, COMPLETED, FAILED
    total_delta BIGINT NOT NULL DEFAULT 0,  -- net discrepancy in micro-units (positive = system overcharged)
    campaigns_checked INT NOT NULL DEFAULT 0,
    discrepancies_found INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_recon_runs_period ON recon_runs (period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_recon_runs_status ON recon_runs (status);

-- recon_discrepancies records per-campaign differences found during a run.
-- It serves as an immutable audit trail and allows operators to review / rollback adjustments.
-- delta = (ledger_charged - actual_from_source). Negative delta means customer was under-charged.
CREATE TABLE IF NOT EXISTS recon_discrepancies (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES recon_runs(id) ON DELETE CASCADE,
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    expected_spend BIGINT NOT NULL,  -- authoritative from source (CH aggregate or Redis sync)
    actual_spend BIGINT NOT NULL,    -- what was recorded in balance_ledger
    delta BIGINT NOT NULL,
    adjustment_ledger_id BIGINT,     -- FK to the corrective entry created in balance_ledger
    redis_adjusted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recon_disc_run ON recon_discrepancies (run_id);
CREATE INDEX IF NOT EXISTS idx_recon_disc_campaign ON recon_discrepancies (campaign_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS recon_discrepancies;
DROP TABLE IF EXISTS recon_runs;
-- Note: cannot easily remove enum value in Postgres without table rewrite.
-- +goose StatementEnd
