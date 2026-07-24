-- access-check.lua: two-phase OpenResty edge gate for /track (PERIMETER.md).
-- Phase 1 (cheap): limit_req (nginx.conf), circuit breaker, IP blacklist — no read_body.
-- Phase 2 (expensive): read_body, byte DFA, edge_rl — see edge-phase2.lua (default: full).

local edge_metrics = require "edge-metrics"
local edge_phase2 = require "edge-phase2"
local edge_asn = require "edge-asn"
local edge_ingress = require "edge-ingress"
local edge_tarpit = require "edge-tarpit"

local circuit_dict = ngx.shared.circuit_breaker
local blacklist_cache = ngx.shared.blacklist_cache

local FAIL_THRESHOLD = 0.95
local SAMPLE_WINDOW = 100
local BL_STALE_SEC = tonumber(os.getenv("EDGE_BL_STALE_SEC") or "") or 30

local function record_circuit_sample(bucket_curr)
    circuit_dict:incr(bucket_curr .. ":total", 1, 0, 30)
end

local function circuit_breaker_open(bucket_curr, bucket_prev)
    local total_curr = circuit_dict:get(bucket_curr .. ":total") or 0
    local total_prev = circuit_dict:get(bucket_prev .. ":total") or 0
    local total_reqs = total_curr + total_prev
    if total_reqs <= SAMPLE_WINDOW then
        return false
    end
    local errs_curr = circuit_dict:get(bucket_curr .. ":errs") or 0
    local errs_prev = circuit_dict:get(bucket_prev .. ":errs") or 0
    local redis_errs = errs_curr + errs_prev
    return (redis_errs / total_reqs) > FAIL_THRESHOLD
end

local function client_asn()
    local headers = ngx.req.get_headers()
    return headers["X-Client-ASN"] or headers["x-client-asn"]
end

-- phase1_blacklist enforces timer-synced IP blocklist; fail-closed when sync is stale.
local function phase1_blacklist(client_ip)
    if edge_asn.is_whitelisted(client_asn()) then
        return
    end

    local ver = blacklist_cache:get("_bl_ver")
    local sync_ts = blacklist_cache:get("_bl_sync_ts")

    if not ver or not sync_ts then
        edge_metrics.record_blacklist_stale()
        ngx.log(ngx.ERR, "edge blacklist: no successful sync yet")
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end

    if ngx.time() - sync_ts > BL_STALE_SEC then
        edge_metrics.record_blacklist_stale()
        ngx.log(ngx.ERR, "edge blacklist: sync stale > ", BL_STALE_SEC, "s")
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end

    local ip_ver = blacklist_cache:get("b:" .. client_ip)
    if ip_ver and ip_ver == ver then
        edge_metrics.record_blocked_ip()
        ngx.exit(ngx.HTTP_FORBIDDEN)
    end
end

local function phase1(client_ip, bucket_curr, bucket_prev)
    record_circuit_sample(bucket_curr)
    if circuit_breaker_open(bucket_curr, bucket_prev) then
        edge_metrics.record_circuit_reject()
        ngx.log(ngx.ERR, "Edge Circuit Breaker OPEN")
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end

    phase1_blacklist(client_ip)
    edge_metrics.record_phase1_pass()
end

local now = ngx.time()
local bucket_curr = math.floor(now / 10)
local bucket_prev = bucket_curr - 1
local client_ip = ngx.var.remote_addr

phase1(client_ip, bucket_curr, bucket_prev)
edge_tarpit.maybe_delay()
edge_ingress.record_and_forward()
edge_phase2.run()
