-- RTB deals control-plane query plans: EXPLAIN (ANALYZE, BUFFERS)
-- Run against a migrated test DB with ANALYZE rtb_deals; customers;

-- List all deals (admin GET /admin/rtb/deals)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM rtb_deals ORDER BY deal_id;

-- Lookup by internal id (admin GET /admin/rtb/deals/{id})
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM rtb_deals WHERE id = 1;

-- Lookup by business deal_id (tracker reload uniqueness check)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM rtb_deals WHERE deal_id = 'deal-premium-1';

-- Customer-scoped listing (index on customer_id)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM rtb_deals WHERE customer_id = '00000000-0000-4000-8000-000000000001'::uuid;
