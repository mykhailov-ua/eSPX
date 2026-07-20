-- name: ListCostSyncCredentials :many
SELECT * FROM cost_sync_credentials ORDER BY customer_id, network;

-- name: ListCostSyncCredentialsByCustomer :many
SELECT * FROM cost_sync_credentials WHERE customer_id = $1 ORDER BY network;

-- name: GetCostSyncCredential :one
SELECT * FROM cost_sync_credentials WHERE customer_id = $1 AND network = $2;

-- name: UpsertCostSyncCredential :one
INSERT INTO cost_sync_credentials (
    customer_id, network, account_id,
    access_token_encrypted, refresh_token_encrypted, api_key_encrypted,
    extra_config, token_expires_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (customer_id, network) DO UPDATE
SET account_id = EXCLUDED.account_id,
    access_token_encrypted = COALESCE(EXCLUDED.access_token_encrypted, cost_sync_credentials.access_token_encrypted),
    refresh_token_encrypted = COALESCE(EXCLUDED.refresh_token_encrypted, cost_sync_credentials.refresh_token_encrypted),
    api_key_encrypted = COALESCE(EXCLUDED.api_key_encrypted, cost_sync_credentials.api_key_encrypted),
    extra_config = EXCLUDED.extra_config,
    token_expires_at = EXCLUDED.token_expires_at,
    updated_at = NOW()
RETURNING *;

-- name: DeleteCostSyncCredential :exec
DELETE FROM cost_sync_credentials WHERE customer_id = $1 AND network = $2;

-- name: InsertCampaignCost :execrows
INSERT INTO campaign_costs (
    customer_id, campaign_id, cost_date, network, placement_id,
    adset_id, ad_id, line_type, amount_micro, currency, amount_usd_micro, ingest_key
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (ingest_key) DO NOTHING;

-- name: SumCampaignCostsUSDForDate :one
SELECT COALESCE(SUM(amount_usd_micro), 0)::bigint
FROM campaign_costs
WHERE campaign_id = $1 AND cost_date = $2 AND line_type = 'spend';

-- name: SumTrackerEstimatedSpendForDate :one
SELECT COALESCE(SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END), 0)::bigint
FROM balance_ledger
WHERE campaign_id = $1
  AND created_at >= $2::date
  AND created_at < ($2::date + INTERVAL '1 day')
  AND type IN ('FEE', 'RECONCILIATION_ADJUST', 'REFUND');

-- name: InsertCostSyncRun :one
INSERT INTO cost_sync_runs (customer_id, network, cost_date, status, trigger_source)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: CompleteCostSyncRun :exec
UPDATE cost_sync_runs
SET status = $2,
    rows_imported = $3,
    total_amount_usd_micro = $4,
    error_message = $5,
    completed_at = NOW()
WHERE id = $1;

-- name: ListCostSyncRuns :many
SELECT * FROM cost_sync_runs
WHERE ($1::uuid IS NULL OR customer_id = $1)
ORDER BY started_at DESC
LIMIT $2 OFFSET $3;

-- name: UpsertECBRate :exec
INSERT INTO cost_sync_ecb_rates (rate_date, currency, usd_per_unit_micro)
VALUES ($1, $2, $3)
ON CONFLICT (rate_date, currency) DO UPDATE
SET usd_per_unit_micro = EXCLUDED.usd_per_unit_micro;

-- name: GetECBRate :one
SELECT usd_per_unit_micro FROM cost_sync_ecb_rates
WHERE rate_date = $1 AND currency = $2;
