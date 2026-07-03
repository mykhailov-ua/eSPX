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
SELECT * FROM campaigns WHERE status = 'ACTIVE';

-- name: GetCustomerByID :one
SELECT * FROM customers WHERE id = $1 LIMIT 1;
