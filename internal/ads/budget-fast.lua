-- Tier B fast filter: budget debit + idempotency + stream in one atomic script.
-- No rate limit, fcap, pacing, TTC, or quota-refill side effects.
-- KEYS[1] spend (budget:campaign or budget:quota)
-- KEYS[2] idempotency
-- KEYS[3] campaign_sync
-- KEYS[4] customer_sync
-- KEYS[5] dirty_campaigns
-- KEYS[6] dirty_customers
-- KEYS[7] stream
-- KEYS[8] migration_fence
-- KEYS[9] budget_frozen
-- ARGV[1] amount, [2] idem_ttl, [3] campaign_id, [4] customer_id, [5] max_stream_len,
--       [6] click_id, [7] evt_type, [8] payload, [9] ip, [10] ua, [11] user_id, [12] skip_budget
-- Returns: -1 budget miss, 0 ok, 3 budget exhausted, 11 debit fenced.

local batch = redis.call("MGET", KEYS[1], KEYS[2], KEYS[8], KEYS[9])
local spend = batch[1]
local idem = batch[2]
local fence = batch[3]
local frozen = batch[4]

if not spend then
    return -1
end

if idem then
    return 0
end

if fence or frozen then
    return 11
end

local amount = tonumber(ARGV[1]) or 0
local skip_budget = ARGV[12] == "1"

if not skip_budget then
    local budget = tonumber(spend) or 0
    if budget < amount then
        return 3
    end

    redis.call("INCRBY", KEYS[1], -amount)

    local c_sync = redis.call("INCRBY", KEYS[3], amount)
    if c_sync == amount then
        redis.call("SADD", KEYS[5], ARGV[3])
    end

    local cust_sync = redis.call("INCRBY", KEYS[4], amount)
    if cust_sync == amount then
        redis.call("SADD", KEYS[6], ARGV[4])
    end
end

redis.call("SET", KEYS[2], "1", "EX", ARGV[2])

redis.call("XADD", KEYS[7], "MAXLEN", "~", ARGV[5], "*",
    "click_id", ARGV[6],
    "campaign_id", ARGV[3],
    "user_id", ARGV[11],
    "type", ARGV[7],
    "payload", ARGV[8],
    "ip", ARGV[9],
    "ua", ARGV[10])

return 0
