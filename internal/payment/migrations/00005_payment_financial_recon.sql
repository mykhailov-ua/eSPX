-- +goose Up
-- +goose StatementBegin
CREATE TYPE payment.financial_finding_kind AS ENUM (
  'MISSING_LEDGER_TOPUP',
  'TOPUP_AMOUNT_MISMATCH',
  'ORPHAN_LEDGER_TOPUP',
  'REFUND_LEDGER_DRIFT',
  'CHARGEBACK_LEDGER_DRIFT',
  'CHARGEBACK_REVERSAL_DRIFT',
  'DEAD_OUTBOX',
  'SETTLEMENT_FAILED_INTENT'
);

CREATE TYPE payment.financial_recon_status AS ENUM (
  'PENDING',
  'COMPLETED',
  'FAILED'
);

CREATE TABLE payment.financial_recon_runs (
  id                 BIGSERIAL PRIMARY KEY,
  period_start       TIMESTAMPTZ NOT NULL,
  period_end         TIMESTAMPTZ NOT NULL,
  status             payment.financial_recon_status NOT NULL DEFAULT 'PENDING',
  findings_count     INT NOT NULL DEFAULT 0,
  intents_checked    INT NOT NULL DEFAULT 0,
  error_message      TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at       TIMESTAMPTZ
);

CREATE INDEX idx_financial_recon_runs_created
  ON payment.financial_recon_runs (created_at DESC);

CREATE TABLE payment.financial_recon_findings (
  id                 BIGSERIAL PRIMARY KEY,
  run_id             BIGINT NOT NULL REFERENCES payment.financial_recon_runs(id) ON DELETE CASCADE,
  kind               payment.financial_finding_kind NOT NULL,
  payment_intent_id  UUID,
  customer_id        UUID,
  payment_amount_micro BIGINT NOT NULL DEFAULT 0,
  ledger_amount_micro  BIGINT NOT NULL DEFAULT 0,
  delta_micro        BIGINT NOT NULL DEFAULT 0,
  detail             JSONB NOT NULL DEFAULT '{}',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_financial_recon_findings_run
  ON payment.financial_recon_findings (run_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS payment.financial_recon_findings;
DROP TABLE IF EXISTS payment.financial_recon_runs;
DROP TYPE IF EXISTS payment.financial_recon_status;
DROP TYPE IF EXISTS payment.financial_finding_kind;
-- +goose StatementEnd
