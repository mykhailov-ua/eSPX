-- dedup.sql: D3 v2 deterministic dedup adapter (M4).

-- name: DedupSyncIdempotencyExists :one
SELECT EXISTS(SELECT 1 FROM sync_idempotency WHERE id = $1) AS exists;

-- name: RejectStaleDedupProposals :execrows
UPDATE dedup_key_proposals
SET status = 'rejected'
WHERE status = 'pending'
  AND created_at < NOW() - INTERVAL '24 hours';
