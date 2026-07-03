-- +goose Up
-- +goose StatementBegin
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TYPE billing.tax_scheme AS ENUM (
  'NONE',
  'VAT',
  'SALES_TAX'
);

CREATE TABLE billing.customer_tax_profiles (
  customer_id   UUID PRIMARY KEY,
  country_code  CHAR(2) NOT NULL DEFAULT 'US',
  tax_region    TEXT,
  tax_scheme    billing.tax_scheme NOT NULL DEFAULT 'NONE',
  tax_rate_bps  INT NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE billing.invoice_status AS ENUM (
  'FINALIZED',
  'VOID'
);

CREATE TABLE billing.invoices (
  id               UUID PRIMARY KEY,
  customer_id      UUID NOT NULL,
  billing_month    DATE NOT NULL,
  subtotal_micro   BIGINT NOT NULL,
  tax_micro        BIGINT NOT NULL,
  total_micro      BIGINT NOT NULL,
  currency         TEXT NOT NULL DEFAULT 'USD',
  tax_scheme       billing.tax_scheme NOT NULL DEFAULT 'NONE',
  tax_rate_bps     INT NOT NULL DEFAULT 0,
  ledger_sum_micro BIGINT NOT NULL,
  status           billing.invoice_status NOT NULL DEFAULT 'FINALIZED',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (customer_id, billing_month)
);

CREATE INDEX idx_invoices_customer_created
  ON billing.invoices (customer_id, created_at DESC);

CREATE TABLE billing.invoice_lines (
  id            BIGSERIAL PRIMARY KEY,
  invoice_id    UUID NOT NULL REFERENCES billing.invoices(id) ON DELETE CASCADE,
  ledger_type   TEXT NOT NULL,
  amount_micro  BIGINT NOT NULL,
  entry_count   INT NOT NULL
);

CREATE INDEX idx_invoice_lines_invoice
  ON billing.invoice_lines (invoice_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS billing.invoice_lines;
DROP TABLE IF EXISTS billing.invoices;
DROP TYPE IF EXISTS billing.invoice_status;
DROP TABLE IF EXISTS billing.customer_tax_profiles;
DROP TYPE IF EXISTS billing.tax_scheme;
DROP SCHEMA IF EXISTS billing;
-- +goose StatementEnd
