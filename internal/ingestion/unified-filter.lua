-- Hot-path filter script: non-blocking Redis ops only (MGET, GET, INCR, SET NX, XADD, SADD, EXPIRE).
-- M9-02: fraud blocklist, placement blocklist, ingress quota consolidated in one EVALSHA.
-- M9-03: IP rate limit removed (edge XDP/nginx only).
-- M9-04: tier degradation skips non-critical checks near filter deadline.
-- KEYS[1] blacklist:fraud (was rl:ip; M9-03 edge-only IP RL)
-- KEYS[2..12]: dup, budget, idempotency, sync, dirty, stream, daily, fcap, imp_ts.
-- KEYS[13..15]: quota, refill_lock, refill_needed.
-- KEYS[16..17]: migration_fence, budget_frozen.
-- KEYS[18..19]: ingress:day, placement blacklist (or ignored sentinel).
-- Returns: -1 budget miss, 0 ok, 2 dup, 3 budget, 4 pacing, 5 fcap, 6 low_ttc, 7 missing_imp_ts,
--          10 TTC bypass, 11 debit fenced, 12 daily quota, 14 placement blocked, 21 fraud signal (ok), 20 degraded ok.

local quota_enabled = ARGV[25] == "1"
local chunk_size = tonumber(ARGV[26]) or 0
local refill_threshold_pct = tonumber(ARGV[27]) or 20

local deadline_ns = tonumber(ARGV[29]) or 0
local now_mono_ns = tonumber(ARGV[30]) or 0
local degrade_ns = tonumber(ARGV[31]) or 2000000
local degraded = false
if deadline_ns > 0 and now_mono_ns > 0 and (deadline_ns - now_mono_ns) < degrade_ns then
    degraded = true
end

local b
local idem_exists
local daily_spent_raw
local fcap_raw
local spend_key = KEYS[3]

if quota_enabled then
    local batch = redis.call("MGET", KEYS[13], KEYS[4], KEYS[10], KEYS[11])
    b = batch[1]
    idem_exists = batch[2]
    daily_spent_raw = batch[3]
    fcap_raw = batch[4]
    spend_key = KEYS[13]
else
    local batch = redis.call("MGET", KEYS[3], KEYS[4], KEYS[10], KEYS[11])
    b = batch[1]
    idem_exists = batch[2]
    daily_spent_raw = batch[3]
    fcap_raw = batch[4]
end

if not b then
    return -1
end

if idem_exists then
    return 0
end

local barriers = redis.call("MGET", KEYS[16], KEYS[17])
local redis_epoch = tonumber(barriers[1]) or 0
local routing_epoch = tonumber(ARGV[28]) or 0
if (redis_epoch > 0 and redis_epoch ~= routing_epoch) or barriers[2] then
    return 11
end

local client_ip = ARGV[12] or ""
local fraud_list_hit = false
if client_ip ~= "" and KEYS[1] and KEYS[1] ~= "fcap:ignored" then
    if redis.call("SISMEMBER", KEYS[1], client_ip) == 1 then
        fraud_list_hit = true
    end
end

local placement_id = ARGV[32] or ""
if placement_id ~= "" and KEYS[19] and KEYS[19] ~= "fcap:ignored" then
    if redis.call("HEXISTS", KEYS[19], placement_id) == 1 then
        return 14
    end
end

local max_rpd = tonumber(ARGV[33]) or 0
if max_rpd > 0 and KEYS[18] and KEYS[18] ~= "fcap:ignored" then
    local ingress = redis.call("INCR", KEYS[18])
    if ingress == 1 then
        redis.call("EXPIRE", KEYS[18], tonumber(ARGV[34]) or 100800)
    end
    if ingress > max_rpd then
        return 12
    end
end

local evt_type = ARGV[10] or ""
local ttc_min_ms = tonumber(ARGV[20]) or 0
local now_ms = tonumber(ARGV[21]) or 0
local ttc_fail_closed = ARGV[23] == "1"
local ttc_bypass = false

if not degraded and evt_type == "click" and ttc_min_ms > 0 then
    local imp_ts_raw = redis.call("GET", KEYS[12])
    if imp_ts_raw then
        local imp_ts = tonumber(imp_ts_raw)
        if imp_ts and (now_ms - imp_ts) < ttc_min_ms then
            return 6
        end
    elseif ttc_fail_closed then
        return 7
    else
        ttc_bypass = true
    end
end

local skip_budget = ARGV[24] == "1"

local budget = tonumber(b) or 0
local amount = tonumber(ARGV[4]) or 0
local freq_limit = tonumber(ARGV[18]) or 0
local user_id = ARGV[17] or ""

if not skip_budget then
    if budget < amount then
        return 3
    end

    if not degraded and ARGV[14] == "1" then
        local daily_spent = tonumber(daily_spent_raw or 0)
        local daily_limit = tonumber(ARGV[15]) or 0
        local hour_num = tonumber(ARGV[16]) or 24
        local cumulative_limit = math.floor((daily_limit * hour_num) / 24)

        if daily_spent + amount > cumulative_limit then
            return 4
        end
    end
end

if not degraded and freq_limit > 0 and user_id ~= "" then
    local current_fcap = tonumber(fcap_raw or 0)
    if current_fcap >= freq_limit then
        return 5
    end
end

local is_dup = redis.call("SET", KEYS[2], "1", "NX", "EX", ARGV[3])
if not is_dup then
    return 2
end

if not skip_budget then
    redis.call("INCRBY", spend_key, -amount)
    local c_sync = redis.call("INCRBY", KEYS[5], amount)
    if c_sync == amount then
        redis.call("SADD", KEYS[7], ARGV[6])
    end

    local cust_sync = redis.call("INCRBY", KEYS[6], amount)
    if cust_sync == amount then
        redis.call("SADD", KEYS[8], ARGV[7])
    end

    local dw = redis.call("GET", "slot_migration:dual_write")
    if dw then
        redis.call("XADD", "slot_migration:delta", "MAXLEN", "~", "100000", "*",
            "campaign_id", ARGV[6],
            "amount", ARGV[4],
            "spend_key", spend_key)
    end
end

redis.call("SET", KEYS[4], "1", "EX", ARGV[5])

if not skip_budget and not degraded and ARGV[14] == "1" then
    local ds = redis.call("INCRBY", KEYS[10], amount)
    if ds == amount then
        redis.call("EXPIRE", KEYS[10], 172800)
    end
end

if not degraded and freq_limit > 0 and user_id ~= "" then
    local new_fcap = redis.call("INCR", KEYS[11])
    if new_fcap == 1 then
        redis.call("EXPIRE", KEYS[11], tonumber(ARGV[19]))
    end
end

if not degraded and evt_type == "impression" then
    local imp_ts_ttl = tonumber(ARGV[22]) or 600
    redis.call("SET", KEYS[12], now_ms, "EX", imp_ts_ttl)
end

redis.call("XADD", KEYS[9], "MAXLEN", "~", ARGV[8], "*",
    "click_id", ARGV[9],
    "campaign_id", ARGV[6],
    "user_id", user_id,
    "type", evt_type,
    "payload", ARGV[11],
    "ip", ARGV[12],
    "ua", ARGV[13]
)

if not degraded and quota_enabled and chunk_size > 0 and spend_key == KEYS[13] and not skip_budget then
    local quota_after = budget - amount
    local threshold = math.floor(chunk_size * refill_threshold_pct / 100)
    if quota_after < threshold then
        local locked = redis.call("SET", KEYS[14], "1", "NX", "EX", 10)
        if locked then
            redis.call("SADD", KEYS[15], ARGV[6])
        end
    end
end

if ttc_bypass then
    return 10
end
-- M14-16 branch tags: 21 fraud_signal, 20 tier_degraded (documented as 0x15/0x14).
if fraud_list_hit then
    return 21
end
if degraded then
    return 20
end
return 0
