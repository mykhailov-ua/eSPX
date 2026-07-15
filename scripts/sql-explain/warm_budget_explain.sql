-- Warm-budget query plan: EXPLAIN (ANALYZE, BUFFERS)
-- Used by OutboxWorker.campaignRemainingBudget for POST /admin/campaigns/{id}/warm-budget
-- Run against a migrated test DB with ANALYZE campaigns;

-- Point lookup by campaign UUID (authoritative remaining budget)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT budget_limit, current_spend
FROM campaigns
WHERE id = '00000000-0000-4000-8000-000000000001'::uuid;

-- Computed remaining (same semantics as application code)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT GREATEST(budget_limit - current_spend, 0) AS remaining_micro
FROM campaigns
WHERE id = '00000000-0000-4000-8000-000000000001'::uuid;
