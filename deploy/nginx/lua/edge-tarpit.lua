-- edge-tarpit.lua: optional slow path for oversized headers / body (M14-08, GAP-CMP-01).
-- Enable with EDGE_TARPIT_ENABLED=true. Cap delay at EDGE_TARPIT_MAX_SEC (default 2, hard max 15).

local edge_metrics = require "edge-metrics"

local _M = {}

local function env_bool(name, default)
    local v = os.getenv(name)
    if v == nil or v == "" then
        return default
    end
    v = string.lower(v)
    return v == "1" or v == "true" or v == "yes" or v == "on"
end

local function env_num(name, default)
    local v = tonumber(os.getenv(name) or "")
    if not v then
        return default
    end
    return v
end

local ENABLED = env_bool("EDGE_TARPIT_ENABLED", false)
local MAX_HEADERS = env_num("EDGE_TARPIT_MAX_HEADERS", 64)
local MAX_BODY = env_num("EDGE_TARPIT_BODY_BYTES", 65536)
local MAX_SEC = env_num("EDGE_TARPIT_MAX_SEC", 2)
if MAX_SEC > 15 then
    MAX_SEC = 15
end
if MAX_SEC < 0 then
    MAX_SEC = 0
end

function _M.maybe_delay()
    if not ENABLED then
        return
    end

    local headers = ngx.req.get_headers()
    local n = 0
    for _ in pairs(headers) do
        n = n + 1
    end

    local cl = tonumber(ngx.var.content_length) or 0
    local delay = 0
    if n > MAX_HEADERS then
        delay = math.min(MAX_SEC, 0.25 + (n - MAX_HEADERS) * 0.05)
    end
    if cl > MAX_BODY then
        local bodyDelay = math.min(MAX_SEC, 0.5 + (cl - MAX_BODY) / MAX_BODY)
        if bodyDelay > delay then
            delay = bodyDelay
        end
    end

    if delay > 0 then
        edge_metrics.record_tarpit(delay)
        ngx.sleep(delay)
    end
end

return _M
