-- edge-rl.lua: fixed-window per-campaign rate limiter with fraud_score tier scaling.

local edge_config = require "edge-config"
local edge_fraud_tier = require "edge-fraud-tier"

local _M = {}

local rl_dict = ngx.shared.edge_rl

local function tier_limit(base_limit, fraud_score)
    local tier = edge_fraud_tier.tier_from_score(fraud_score)
    local pct = edge_config.get_tier_pct(tier)
    if pct <= 0 then
        return 0, tier
    end
    if pct >= 100 then
        return base_limit, tier
    end
    local scaled = math.max(1, math.floor(base_limit * pct / 100))
    return scaled, tier
end

-- retry_after_sec returns Retry-After for a fraud_score tier.
function _M.retry_after_sec(fraud_score)
    local tier = edge_fraud_tier.tier_from_score(fraud_score)
    return edge_config.get_retry_after(tier)
end

-- allow returns false when the campaign exceeds the tier-scaled window limit.
function _M.allow(campaign_id, fraud_score)
    if not campaign_id or campaign_id == "" then
        return true
    end

    local base_limit, window_ms = edge_config.get()
    if base_limit <= 0 then
        return true
    end

    local limit, tier = tier_limit(base_limit, fraud_score or 0)
    if tier == "block" or limit <= 0 then
        return false
    end

    local window_sec = math.max(1, math.floor(window_ms / 1000))
    local bucket = math.floor(ngx.time() / window_sec)
    local key = campaign_id .. ":" .. tier .. ":" .. tostring(bucket)

    local count, err = rl_dict:incr(key, 1, 0, window_sec * 2)
    if not count then
        ngx.log(ngx.ERR, "edge_rl: incr failed: ", err)
        return false
    end

    return count <= limit
end

return _M
