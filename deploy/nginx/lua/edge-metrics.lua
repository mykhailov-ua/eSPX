-- edge-metrics.lua: edge phase counters and Prometheus text export.

local _M = {}

local metrics = ngx.shared.edge_metrics
local blacklist_cache = ngx.shared.blacklist_cache

function _M.record_phase1_pass()
    metrics:incr("phase1_pass_total", 1, 0)
end

function _M.record_phase2_pass()
    metrics:incr("phase2_pass_total", 1, 0)
end

function _M.record_body_read()
    metrics:incr("body_read_total", 1, 0)
end

function _M.record_circuit_reject()
    metrics:incr("circuit_reject_total", 1, 0)
end

function _M.record_blocked_ip()
    metrics:incr("blocked_ip_total", 1, 0)
end

function _M.record_blocked_campaign_rl()
    metrics:incr("blocked_campaign_rl_total", 1, 0)
end

function _M.record_blocked_fraud_tier()
    metrics:incr("blocked_fraud_tier_total", 1, 0)
end

function _M.record_parse_oversize()
    metrics:incr("parse_oversize_total", 1, 0)
end

function _M.record_body_stream()
    metrics:incr("body_stream_total", 1, 0)
end

function _M.record_body_peek()
    metrics:incr("body_peek_total", 1, 0)
end

function _M.record_chunked_reject()
    metrics:incr("chunked_reject_total", 1, 0)
end

function _M.record_blacklist_stale()
    metrics:incr("blacklist_stale_total", 1, 0)
end

local function say_metric(name, metric_type, help, value)
    ngx.say("# HELP ", name, " ", help)
    ngx.say("# TYPE ", name, " ", metric_type)
    ngx.say(name, " ", value)
end

function _M.render_prometheus()
    ngx.header["Content-Type"] = "text/plain; version=0.0.4; charset=utf-8"

    local phase1_pass = metrics:get("phase1_pass_total") or 0
    local phase2_pass = metrics:get("phase2_pass_total") or 0
    local body_read = metrics:get("body_read_total") or 0
    local circuit_reject = metrics:get("circuit_reject_total") or 0
    local blocked_ip = metrics:get("blocked_ip_total") or 0
    local blocked_rl = metrics:get("blocked_campaign_rl_total") or 0
    local blocked_fraud_tier = metrics:get("blocked_fraud_tier_total") or 0
    local parse_oversize = metrics:get("parse_oversize_total") or 0
    local body_stream = metrics:get("body_stream_total") or 0
    local body_peek = metrics:get("body_peek_total") or 0
    local chunked_reject = metrics:get("chunked_reject_total") or 0
    local blacklist_stale = metrics:get("blacklist_stale_total") or 0
    local sync_ts = blacklist_cache:get("_bl_sync_ts") or 0
    local bl_count = blacklist_cache:get("_bl_count") or 0

    say_metric(
        "espx_edge_phase1_pass_total",
        "counter",
        "Requests that passed phase-1 edge checks (circuit breaker, IP blacklist).",
        phase1_pass
    )
    say_metric(
        "espx_edge_phase2_pass_total",
        "counter",
        "Requests that passed phase-2 edge checks (body read, parse, campaign RL).",
        phase2_pass
    )
    say_metric(
        "espx_edge_body_read_total",
        "counter",
        "Requests where ngx.req.read_body was invoked at the edge.",
        body_read
    )
    say_metric(
        "espx_edge_circuit_reject_total",
        "counter",
        "Requests rejected by edge circuit breaker (503).",
        circuit_reject
    )
    say_metric(
        "espx_edge_blocked_ip_total",
        "counter",
        "Requests blocked by IP blacklist at OpenResty edge (403).",
        blocked_ip
    )
    say_metric(
        "espx_edge_blocked_campaign_rl_total",
        "counter",
        "Requests blocked by per-campaign edge rate limiter.",
        blocked_rl
    )
    say_metric(
        "espx_edge_blocked_fraud_tier_total",
        "counter",
        "Requests blocked by fraud_score tier at edge (403/429).",
        blocked_fraud_tier
    )
    say_metric(
        "espx_edge_parse_oversize_total",
        "counter",
        "Requests rejected by edge DFA or Content-Length over body/scan limits (413).",
        parse_oversize
    )
    say_metric(
        "espx_edge_body_stream_total",
        "counter",
        "Phase-2 stream mode: no read_body, body proxied without Lua buffering.",
        body_stream
    )
    say_metric(
        "espx_edge_body_peek_total",
        "counter",
        "Phase-2 peek mode: cosocket read of scan window only.",
        body_peek
    )
    say_metric(
        "espx_edge_chunked_reject_total",
        "counter",
        "Requests rejected because chunked encoding is not allowed on edge.",
        chunked_reject
    )
    say_metric(
        "espx_edge_blacklist_stale_total",
        "counter",
        "Requests rejected because blacklist sync is missing or stale (503).",
        blacklist_stale
    )
    say_metric(
        "espx_edge_sync_last_success_timestamp",
        "gauge",
        "Unix time of last successful blacklist sync from Redis shard 0.",
        sync_ts
    )
    say_metric(
        "espx_edge_blacklist_entries",
        "gauge",
        "Blocked IPs in the last successful blacklist sync.",
        bl_count
    )
end

return _M
