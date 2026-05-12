-- Unified Filter Lua Script
-- Keys:
-- KEYS[1]: Rate limit key
-- KEYS[2]: Duplicate key
-- KEYS[3]: Budget source key (budget:campaign:{id})
-- KEYS[4]: Idempotency key (for budget specifically)
-- KEYS[5]: Campaign sync key (budget:sync:campaign:{id})
-- KEYS[6]: Customer sync key (budget:sync:customer:{id})
-- KEYS[7]: Dirty campaigns set
-- KEYS[8]: Dirty customers set
-- KEYS[9]: Stream name

-- Args:
-- ARGV[1]: Rate limit window (seconds)
-- ARGV[2]: Rate limit limit
-- ARGV[3]: Duplicate TTL (seconds)
-- ARGV[4]: Amount to deduct
-- ARGV[5]: Idempotency TTL (seconds)
-- ARGV[6]: Campaign ID (string)
-- ARGV[7]: Customer ID (string)
-- ARGV[8]: Stream MaxLen
-- ARGV[9]: Click ID
-- ARGV[10]: Event Type
-- ARGV[11]: Payload
-- ARGV[12]: IP
-- ARGV[13]: User Agent

-- 1. Budget Cache Miss Check (Fastest & No state change)
local b = redis.call("GET", KEYS[3])
if not b then
    return -1 -- Cache miss, need to load from DB
end

-- 2. Deduplication (Event level)
-- We do this BEFORE rate limiting to prevent dups from burning quota.
local is_dup = redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[3])
if not is_dup then
    return 2 -- Duplicate
end

-- 3. Rate Limiting (Per IP/Source)
-- After dedup, so only unique valid requests are counted.
local rl_count = redis.call("INCR", KEYS[1])
if rl_count == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
if rl_count > tonumber(ARGV[2]) then
    return 1 -- Rate limited
end

-- 4. Budget Idempotency
if redis.call("EXISTS", KEYS[4]) == 1 then
    return 0 -- Already processed budget, but let it pass (idempotent)
end

-- 5. Budget Check
local budget = tonumber(b)
local amount = tonumber(ARGV[4])

if budget < amount then
    return 3 -- No budget
end

-- 6. Atomic Deductions, Persistence Marking, and Ingestion
redis.call("INCRBYFLOAT", KEYS[3], -amount)
redis.call("INCRBYFLOAT", KEYS[5], amount)
redis.call("INCRBYFLOAT", KEYS[6], amount)
redis.call("SADD", KEYS[7], ARGV[6])
redis.call("SADD", KEYS[8], ARGV[7])
redis.call("SET", KEYS[4], "1", "EX", ARGV[5])

-- 7. XADD to Stream
redis.call("XADD", KEYS[9], "MAXLEN", "~", ARGV[8], "*", 
    "click_id", ARGV[9],
    "campaign_id", ARGV[6],
    "type", ARGV[10],
    "payload", ARGV[11],
    "ip", ARGV[12],
    "ua", ARGV[13]
)

return 0 -- Success
