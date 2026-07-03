-- name: GetCustomerBalance :one
SELECT balance, currency
FROM customers
WHERE id = $1;

-- name: SumCustomerLedgerTotal :one
SELECT COALESCE(SUM(amount), 0)::bigint AS total_micro
FROM balance_ledger
WHERE customer_id = $1;

-- name: SumCustomerSpendInWindow :one
SELECT COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0)::bigint AS spend_micro
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3
  AND type IN ('FEE', 'RECONCILIATION_ADJUST');

-- name: SumCustomerLedgerByTypeInWindow :many
SELECT type::text AS ledger_type,
       COALESCE(SUM(amount), 0)::bigint AS amount_micro,
       COUNT(*)::int AS entry_count
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3
GROUP BY type
ORDER BY type;

-- name: GetCustomerTaxProfile :one
SELECT *
FROM billing.customer_tax_profiles
WHERE customer_id = $1;

-- name: UpsertCustomerTaxProfile :one
INSERT INTO billing.customer_tax_profiles (
  customer_id, country_code, tax_region, tax_scheme, tax_rate_bps
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (customer_id) DO UPDATE
SET country_code = EXCLUDED.country_code,
    tax_region = EXCLUDED.tax_region,
    tax_scheme = EXCLUDED.tax_scheme,
    tax_rate_bps = EXCLUDED.tax_rate_bps,
    updated_at = now()
RETURNING *;

-- name: GetInvoiceByCustomerMonth :one
SELECT *
FROM billing.invoices
WHERE customer_id = $1 AND billing_month = $2;

-- name: GetInvoice :one
SELECT *
FROM billing.invoices
WHERE id = $1;

-- name: CountCustomerInvoices :one
SELECT COUNT(*)::bigint
FROM billing.invoices
WHERE customer_id = $1;

-- name: ListCustomerInvoices :many
SELECT *
FROM billing.invoices
WHERE customer_id = $1
ORDER BY billing_month DESC
LIMIT $2 OFFSET $3;

-- name: CreateInvoice :one
INSERT INTO billing.invoices (
  id, customer_id, billing_month, subtotal_micro, tax_micro, total_micro,
  currency, tax_scheme, tax_rate_bps, ledger_sum_micro
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING *;

-- name: CreateInvoiceLine :one
INSERT INTO billing.invoice_lines (invoice_id, ledger_type, amount_micro, entry_count)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: ListInvoiceLines :many
SELECT *
FROM billing.invoice_lines
WHERE invoice_id = $1
ORDER BY ledger_type;
