-- name: CreateCustomer :one
INSERT INTO customers (id, name, balance, currency)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: UpdateCustomerBalanceManagement :one
UPDATE customers
SET balance = balance + $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: GetCustomerForUpdate :one
SELECT * FROM customers
WHERE id = $1
FOR UPDATE;

-- name: UpdateCampaignStatus :one
UPDATE campaigns
SET status = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: GetCampaignFull :one
SELECT * FROM campaigns
WHERE id = $1;

-- name: CreateLedgerEntry :one
INSERT INTO balance_ledger (customer_id, campaign_id, amount, type, idempotency_hash)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetLedgerByHash :one
SELECT * FROM balance_ledger
WHERE idempotency_hash = $1;

-- name: CreateStatusHistory :exec
INSERT INTO campaign_status_history (campaign_id, old_status, new_status, reason)
VALUES ($1, $2, $3, $4);

-- name: SoftDeleteCampaign :exec
UPDATE campaigns
SET status = 'DELETED',
    deleted_at = CURRENT_TIMESTAMP
WHERE id = $1;

-- name: CreateAuditLog :one
INSERT INTO admin_audit_log (admin_id, action, target_type, target_id, changes, metadata)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: CleanupAuditLogs :exec
DELETE FROM admin_audit_log
WHERE created_at < $1;

-- name: ListAuditLogs :many
SELECT * FROM admin_audit_log
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;
