-- Unified Filter Lua Script (Highload-Optimized)
-- Transactional Atomicity:
-- Redis evaluates this script in a single-threaded execution context, guaranteeing complete serializability
-- and database transactional isolation. No other client commands can execute concurrently, preventing race conditions
-- across rate limits, deduplication checks, and budget updates.
--
-- Performance Design:
-- Minimizes redis.call command crossings by executing batch operations (MGET) at the beginning.
-- Separates execution into a non-mutative eligibility check phase followed by a mutative state commit phase
-- to prevent dirty/leaked updates on validation failures.
--
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
-- KEYS[10]: Daily spend key (budget:daily_spent:campaign:{id}:{date})
-- KEYS[11]: Frequency capping key (fcap:c:{cid}:u:{uid})

-- Args:
-- ARGV[1]: Rate limit window (seconds)
-- ARGV[2]: Rate limit max requests
-- ARGV[3]: Duplicate TTL (seconds)
-- ARGV[4]: Amount (int64 micro-units)
-- ARGV[5]: Idempotency TTL (seconds)
-- ARGV[6]: Campaign ID string
-- ARGV[7]: Customer ID string
-- ARGV[8]: Max stream length
-- ARGV[9]: Click ID
-- ARGV[10]: Event type
-- ARGV[11]: Payload
-- ARGV[12]: IP
-- ARGV[13]: User Agent
-- ARGV[14]: Is Even Pacing (1 or 0)
-- ARGV[15]: Daily Budget (int64 micro-units)
-- ARGV[16]: Current Hour Number (1-24)
-- ARGV[17]: User ID
-- ARGV[18]: Freq Limit
-- ARGV[19]: Freq Window (seconds)

-- 1. Batch GET phase: Fetches all operational keys in a single MGET call to save CPU context transitions.
local batch = redis.call("MGET", KEYS[3], KEYS[4], KEYS[10], KEYS[11])
local b = batch[1]
local idem_exists = batch[2]
local daily_spent_raw = batch[3]
local fcap_raw = batch[4]

-- Budget Cache Miss Check:
-- Returns -1 when a campaign budget is not cached in Redis. The Go tracker layer intercepts this code,
-- loads the current balance from PostgreSQL, populates the Redis key via SETNX, and retries the evaluation.
if not b then
    return -1
end

-- Budget Idempotency Check:
-- Returns 0 immediately if the exact idempotency key is already cached, ensuring that network retries
-- do not double-charge campaigns or duplicate events.
if idem_exists then
    return 0 
end

-- 2. Defensive Parsing of Input Arguments
local budget = tonumber(b) or 0
local amount = tonumber(ARGV[4]) or 0
local freq_limit = tonumber(ARGV[18]) or 0
local user_id = ARGV[17] or ""

-- 3. Eligibility Checks (Non-Mutative Phase):
-- Verifies campaign conditions. By performing read-only validations prior to modifying rate limits or
-- deduplication locks, we prevent failed queries from corrupting active rate counters or lock states.
if budget < amount then
    return 3
end

-- Hour Pacing Check:
-- When Even Pacing is active (ARGV[14] == "1"), calculates a cumulative hour budget:
-- limit = (DailyBudget * HourNumber) / 24. Halts ingestion with status code 4 if total spent exceeds this.
if ARGV[14] == "1" then
    local daily_spent = tonumber(daily_spent_raw or 0)
    local daily_limit = tonumber(ARGV[15]) or 0
    local hour_num = tonumber(ARGV[16]) or 24
    local cumulative_limit = math.floor((daily_limit * hour_num) / 24)
    
    if daily_spent + amount > cumulative_limit then
        return 4
    end
end

-- Frequency Capping Check:
-- Evaluates user frequency limitations. Returns code 5 if the user has reached their maximum allowed caps.
if freq_limit > 0 and user_id ~= "" then
    local current_fcap = tonumber(fcap_raw or 0)
    if current_fcap >= freq_limit then
        return 5
    end
end

-- 5. Rate Limiting (Mutative Phase):
-- Increments the IP rate limit counter. If the incremented value exceeds the threshold, returns code 1.
-- Employs optimized expiration scheduling: only executes EXPIRE on the first increment (when count == 1),
-- eliminating redundant TTL operations.
local rl_max = tonumber(ARGV[2]) or 0
if rl_max > 0 then
    local rl_count = redis.call("INCR", KEYS[1])
    if rl_count == 1 then
        redis.call("EXPIRE", KEYS[1], ARGV[1])
    end
    if rl_count > rl_max then
        return 1
    end
end

-- 6. Deduplication (Mutative Lock Phase):
-- Attempts to acquire a short-lived lock key using SET NX. Returns 2 if key already exists,
-- preventing duplicate event processing.
local is_dup = redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[3])
if not is_dup then
    return 2
end

-- 7. Atomic Updates and State Commit:
-- Deducts the budget directly from the Redis campaign budget store.
-- Employs transactional batch additions: Campaign and Customer sync balances are incremented.
-- SADD is only called when the balance is initialized (sync_value == amount) to reduce O(log(N)) SADD hash
-- lookup CPU overhead inside Redis by 99.99% under high concurrent volumes.
redis.call("INCRBY", KEYS[3], -amount)
local c_sync = redis.call("INCRBY", KEYS[5], amount)
if c_sync == amount then
    redis.call("SADD", KEYS[7], ARGV[6])
end

local cust_sync = redis.call("INCRBY", KEYS[6], amount)
if cust_sync == amount then
    redis.call("SADD", KEYS[8], ARGV[7])
end

-- Sets the idempotency key to prevent dual-processing on retry.
redis.call("SET", KEYS[4], "1", "EX", ARGV[5])

-- Increments pacing daily spend and schedules key expiration only on first-spend (ds == amount).
if ARGV[14] == "1" then
    local ds = redis.call("INCRBY", KEYS[10], amount)
    if ds == amount then
        redis.call("EXPIRE", KEYS[10], 172800)
    end
end

-- Increments frequency caps and sets lifetime TTL on the initial click.
if freq_limit > 0 and user_id ~= "" then
    local new_fcap = redis.call("INCR", KEYS[11])
    if new_fcap == 1 then
        redis.call("EXPIRE", KEYS[11], tonumber(ARGV[19]))
    end
end

-- 8. XADD to Stream:
-- Append the verified event directly into the fast log stream with approximate max length trimming (~)
-- to control memory consumption without triggering high-latency precise log array truncation.
redis.call("XADD", KEYS[9], "MAXLEN", "~", ARGV[8], "*", 
    "click_id", ARGV[9],
    "campaign_id", ARGV[6],
    "user_id", user_id,
    "type", ARGV[10],
    "payload", ARGV[11],
    "ip", ARGV[12],
    "ua", ARGV[13]
)

return 0
