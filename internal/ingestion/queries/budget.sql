-- name: GetCampaignBudget :one
SELECT 
    c.id, c.customer_id, c.budget_limit, c.current_spend, c.status,
    cust.balance as customer_balance
FROM campaigns c
JOIN customers cust ON c.customer_id = cust.id
WHERE c.id = $1 LIMIT 1;

-- name: UpdateCampaignSpend :exec
UPDATE campaigns 
SET current_spend = current_spend + $2,
    status = CASE 
        WHEN current_spend + $2 >= budget_limit THEN 'EXHAUSTED'::campaign_status_type 
        ELSE status 
    END,
    updated_at = NOW()
WHERE id = $1;

-- name: UpdateCustomerBalance :exec
UPDATE customers 
SET balance = balance - $2,
    updated_at = NOW()
WHERE id = $1;

-- name: ListActiveCampaigns :many
SELECT c.id, c.name, c.status, c.budget_limit, c.created_at, c.updated_at, c.customer_id, c.current_spend, c.deleted_at, c.pacing_mode, c.daily_budget, c.reserve_micro, c.timezone, c.freq_limit, c.freq_window, c.target_countries, c.brand_id, c.brand_fcap_key, c.start_at, c.end_at, c.daypart_hours, c.template_id, c.fraud_threshold_pass, c.fraud_threshold_suspect, c.fraud_threshold_ivt, c.fraud_threshold_block, c.ghost_ivt_enabled, c.behavior_flags, c.supply_chain_nodes, c.require_consent_purposes, c.migration_gen,
       cr.primary_a_shard, cr.primary_b_shard, cr.reserve_shard, cr.h_ema, cr.c_ema, cr.routing_epoch
FROM campaigns c
LEFT JOIN campaign_routing cr ON c.id = cr.campaign_id
WHERE c.status = 'ACTIVE';

-- name: GetCustomerByID :one
SELECT * FROM customers WHERE id = $1 LIMIT 1;
