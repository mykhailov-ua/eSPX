-- name: GetCustomerBalance :one
SELECT balance, currency
FROM customers
WHERE id = $1;

-- name: SumCustomerLedgerTotal :one
SELECT COALESCE(SUM(amount), 0)::bigint AS total_micro
FROM balance_ledger
WHERE customer_id = $1;

-- name: SumCustomerSpendInWindow :one
SELECT COALESCE(SUM(
  CASE
    WHEN type = 'FEE' AND amount < 0 THEN -amount
    WHEN type IN ('RECONCILIATION_ADJUST', 'PAYMENT_REFUND') THEN -amount
    ELSE 0
  END
), 0)::bigint AS spend_micro
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3
  AND type IN ('FEE', 'RECONCILIATION_ADJUST', 'PAYMENT_REFUND');

-- name: ListCustomerIDs :many
SELECT id FROM customers ORDER BY id LIMIT $1 OFFSET $2;

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

-- name: VoidInvoice :execrows
UPDATE billing.invoices
SET status = 'VOID'
WHERE id = $1 AND status = 'FINALIZED';

-- name: SumCustomerLedgerBefore :one
SELECT COALESCE(SUM(amount), 0)::bigint AS total_micro
FROM balance_ledger
WHERE customer_id = $1 AND created_at < $2;

-- name: ListCustomerLedgerInWindow :many
SELECT id, customer_id, amount, type, created_at
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3
  AND ($4::bigint = 0 OR id > $4)
ORDER BY id ASC
LIMIT $5;

-- name: CountCustomerLedgerInWindow :one
SELECT COUNT(*)::bigint
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3;

-- name: SumInvoicesMTD :one
SELECT COALESCE(SUM(total_micro), 0)::bigint, COUNT(*)::bigint
FROM billing.invoices
WHERE billing_month >= date_trunc('month', CURRENT_DATE)::date
  AND status = 'FINALIZED';

-- name: ListCustomerInvoicesInWindow :many
SELECT *
FROM billing.invoices
WHERE customer_id = $1
  AND billing_month >= $2::date
  AND billing_month < $3::date
ORDER BY billing_month DESC;

-- name: ListCustomerPaymentTopupsInWindow :many
SELECT id, amount, created_at, payment_intent_id
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= $2
  AND created_at < $3
  AND type = 'PAYMENT_TOPUP'
ORDER BY created_at DESC
LIMIT $4;

-- name: GetCustomerWalletRow :one
SELECT balance, currency, allowed_overdraft
FROM customers
WHERE id = $1;

-- name: GetCustomerLastInvoiceAt :one
SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz)::timestamptz AS last_invoice_at
FROM billing.invoices
WHERE customer_id = $1 AND status = 'FINALIZED';

-- name: SumCustomerSpendLast7Days :one
SELECT COALESCE(SUM(
  CASE WHEN type = 'FEE' AND amount < 0 THEN -amount ELSE 0 END
), 0)::bigint AS spend_micro
FROM balance_ledger
WHERE customer_id = $1
  AND created_at >= (now() AT TIME ZONE 'utc') - interval '7 days'
  AND type = 'FEE';

-- name: CountCustomersWithFeeSpendInWindow :one
SELECT COUNT(DISTINCT customer_id)::bigint
FROM balance_ledger
WHERE created_at >= $1
  AND created_at < $2
  AND type = 'FEE'
  AND amount < 0;

-- name: ListInvoicesAdmin :many
SELECT *
FROM billing.invoices
WHERE ($1::uuid IS NULL OR customer_id = $1)
  AND ($2::date IS NULL OR billing_month = $2)
  AND ($3::text = '' OR status::text = $3)
  AND ($4::bigint = 0 OR total_micro >= $4)
ORDER BY billing_month DESC, created_at DESC
LIMIT $5 OFFSET $6;

-- name: CountInvoicesAdmin :one
SELECT COUNT(*)::bigint
FROM billing.invoices
WHERE ($1::uuid IS NULL OR customer_id = $1)
  AND ($2::date IS NULL OR billing_month = $2)
  AND ($3::text = '' OR status::text = $3)
  AND ($4::bigint = 0 OR total_micro >= $4);

-- name: ListSubscriptionPlans :many
SELECT * FROM billing.subscription_plans;

-- name: GetSubscriptionPlan :one
SELECT * FROM billing.subscription_plans WHERE code = $1;

-- name: GetCustomerSubscription :one
SELECT s.*, p.display_name, p.limits_json, p.features_json, p.base_fee_micro
FROM billing.customer_subscriptions s
JOIN billing.subscription_plans p ON s.plan_code = p.code
WHERE s.customer_id = $1;

-- name: UpsertCustomerSubscription :one
INSERT INTO billing.customer_subscriptions (customer_id, plan_code, status, period_start, period_end, overrides_json, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (customer_id) DO UPDATE SET
  plan_code = EXCLUDED.plan_code,
  status = EXCLUDED.status,
  period_start = EXCLUDED.period_start,
  period_end = EXCLUDED.period_end,
  overrides_json = EXCLUDED.overrides_json,
  updated_at = NOW()
RETURNING *;

-- name: GetUsageMeter :one
SELECT * FROM billing.usage_meters WHERE customer_id = $1 AND meter = $2 AND period = $3;

-- name: ListUsageMeters :many
SELECT * FROM billing.usage_meters WHERE customer_id = $1 AND period = $2;

-- name: IncrementUsageMeter :one
INSERT INTO billing.usage_meters (customer_id, meter, period, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (customer_id, meter, period) DO UPDATE
SET value = billing.usage_meters.value + EXCLUDED.value
RETURNING *;

-- name: IncrementUsageDaily :one
INSERT INTO billing.usage_daily (customer_id, usage_date, meter, value)
VALUES ($1, $2, $3, $4)
ON CONFLICT (customer_id, usage_date, meter) DO UPDATE
SET value = billing.usage_daily.value + EXCLUDED.value
RETURNING *;

-- name: GetUsageDaily :one
SELECT * FROM billing.usage_daily WHERE customer_id = $1 AND usage_date = $2 AND meter = $3;

-- name: ListUsageDaily :many
SELECT * FROM billing.usage_daily WHERE customer_id = $1 AND usage_date >= $2 AND usage_date <= $3;

-- name: GetLicenseStatus :one
SELECT * FROM billing.license_status LIMIT 1;

-- name: UpsertLicenseStatus :one
INSERT INTO billing.license_status (deployment_id, license_id, plan_code, valid_until, state, entitlements_json, last_verified_at, last_refresh_error)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (deployment_id) DO UPDATE SET
  license_id = EXCLUDED.license_id,
  plan_code = EXCLUDED.plan_code,
  valid_until = EXCLUDED.valid_until,
  state = EXCLUDED.state,
  entitlements_json = EXCLUDED.entitlements_json,
  last_verified_at = EXCLUDED.last_verified_at,
  last_refresh_error = EXCLUDED.last_refresh_error
RETURNING *;

-- name: GetVendorLicense :one
SELECT * FROM vendor.licenses WHERE license_key = $1;

-- name: InsertVendorLicense :one
INSERT INTO vendor.licenses (license_key, customer_name, plan_code, valid_from, valid_until, grace_days, limits_json, features_json, support_tier, revoked)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING *;

-- name: RevokeVendorLicense :exec
UPDATE vendor.licenses SET revoked = TRUE, updated_at = NOW() WHERE license_key = $1;

-- name: RenewVendorLicense :one
UPDATE vendor.licenses SET valid_until = $2, updated_at = NOW() WHERE license_key = $1 RETURNING *;

-- name: RecordVendorRenewalEvent :exec
INSERT INTO vendor.renewal_events (license_key, new_valid_until) VALUES ($1, $2);

-- name: GetVendorDeployment :one
SELECT * FROM vendor.deployments WHERE deployment_id = $1;

-- name: UpsertVendorDeployment :one
INSERT INTO vendor.deployments (deployment_id, license_key, fingerprint, activated_at, last_seen_at)
VALUES ($1, $2, $3, NOW(), NOW())
ON CONFLICT (deployment_id) DO UPDATE SET
  last_seen_at = NOW()
RETURNING *;



