-- edge-phase2.lua: PERIMETER.md phase-2 — read_body, DFA, fraud-tier edge_rl (bounded by EDGE_MAX_BODY_BYTES).
-- EDGE_BODY_MODE: full (default) | stream | peek — stream/peek for experiments only.

local edge_rl = require "edge-rl"
local edge_metrics = require "edge-metrics"
local edge_parse_dfa = require "edge-parse-dfa"
local edge_fraud_tier = require "edge-fraud-tier"

local _M = {}

local MAX_SCAN_BYTES = edge_parse_dfa.MAX_SCAN_BYTES

local function bench_file(name)
    local path = "/etc/nginx/lua/.edge_" .. name
    local f = io.open(path, "r")
    if not f then
        return nil
    end
    local v = f:read("*l")
    f:close()
    if v and v ~= "" then
        return v
    end
    return nil
end

local function config_string(name, env_key, default)
    local v = bench_file(name)
    if v then
        return v
    end
    v = os.getenv(env_key)
    if v and v ~= "" then
        return v
    end
    return default
end

local EDGE_MAX_BODY = tonumber(config_string("max_body_bytes", "EDGE_MAX_BODY_BYTES", tostring(MAX_SCAN_BYTES)))
local INGRESS_SCHEMA = config_string("ingress_schema", "TRACKER_INGRESS_SCHEMA", "openrtb_3")

local function content_length()
    local headers = ngx.req.get_headers()
    return tonumber(headers["content-length"] or headers["Content-Length"])
end

local function header_value(name)
    local headers = ngx.req.get_headers()
    return headers[name] or headers[string.lower(name)]
end

local function fraud_score_from_headers()
    local raw = header_value("X-Fraud-Score") or "0"
    return tonumber(raw) or 0
end

local function campaign_id_from_headers()
    return header_value("X-Campaign-Id")
end

local function reject_oversize()
    edge_metrics.record_parse_oversize()
    ngx.exit(ngx.HTTP_REQUEST_ENTITY_TOO_LARGE)
end

local function reject_chunked()
    edge_metrics.record_chunked_reject()
    ngx.status = ngx.HTTP_LENGTH_REQUIRED
    ngx.say("Content-Length required")
    ngx.exit(411)
end

local function check_edge_limits(cl)
    if edge_parse_dfa.check_content_length(cl) == edge_parse_dfa.ERR_OVERSIZE then
        reject_oversize()
    end
    if cl and cl > EDGE_MAX_BODY then
        reject_oversize()
    end
end

local function require_content_length()
    local cl = content_length()
    if not cl then
        reject_chunked()
    end
    return cl
end

local function reject_rate_limited(fraud_score)
    edge_metrics.record_blocked_campaign_rl()
    ngx.header["Retry-After"] = tostring(edge_rl.retry_after_sec(fraud_score))
    ngx.status = ngx.HTTP_TOO_MANY_REQUESTS
    ngx.say("rate limit exceeded")
    ngx.exit(ngx.HTTP_TOO_MANY_REQUESTS)
end

local function reject_fraud_block(fraud_score)
    edge_metrics.record_blocked_fraud_tier()
    ngx.header["Retry-After"] = tostring(edge_rl.retry_after_sec(fraud_score))
    ngx.status = ngx.HTTP_FORBIDDEN
    ngx.say("fraud score block")
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

local function apply_campaign_rl(campaign_id, fraud_score)
    local tier = edge_fraud_tier.tier_from_score(fraud_score or 0)
    if tier == "block" then
        reject_fraud_block(fraud_score)
    end
    if campaign_id and campaign_id ~= "" and not edge_rl.allow(campaign_id, fraud_score) then
        reject_rate_limited(fraud_score)
    end
end

-- read_bounded_body: read_body on edge (max EDGE_MAX_BODY via nginx + CL); full body for DFA when small.
local function read_bounded_body(cl)
    check_edge_limits(cl)
    edge_metrics.record_body_read()

    local read_ok, read_err = pcall(ngx.req.read_body)
    if not read_ok then
        ngx.log(ngx.ERR, "failed to read body: ", read_err)
        return nil, cl
    end

    local body = ngx.req.get_body_data()
    if not body then
        local filename = ngx.req.get_body_file()
        if filename then
            local fh = io.open(filename, "rb")
            if fh then
                body = fh:read(EDGE_MAX_BODY + 1)
                fh:close()
                if body and #body > EDGE_MAX_BODY then
                    reject_oversize()
                end
            end
        end
    elseif #body > EDGE_MAX_BODY then
        reject_oversize()
    end
    return body, cl
end

-- run_full: IDEAS phase-2 — read_body + DFA + edge_rl.
function _M.run_full()
    local cl = require_content_length()
    local body, _ = read_bounded_body(cl)
    local fraud_score = fraud_score_from_headers()
    local campaign_id, perr = edge_parse_dfa.extract_campaign_id(body, cl, INGRESS_SCHEMA)
    if perr == edge_parse_dfa.ERR_OVERSIZE then
        reject_oversize()
    end
    if campaign_id and campaign_id ~= "" then
        ngx.ctx.campaign_id = campaign_id
    end
    apply_campaign_rl(campaign_id, fraud_score)
    edge_metrics.record_phase2_pass()
end

-- run_stream: no read_body; campaign RL via X-Campaign-Id header when present.
function _M.run_stream()
    local cl = require_content_length()
    check_edge_limits(cl)
    local fraud_score = fraud_score_from_headers()
    local hdr_cid = campaign_id_from_headers()
    if hdr_cid and hdr_cid ~= "" then
        ngx.ctx.campaign_id = hdr_cid
    end
    apply_campaign_rl(hdr_cid, fraud_score)
    edge_metrics.record_body_stream()
    edge_metrics.record_phase2_pass()
end

-- run_peek: cosocket window + DFA without read_body (experiment).
function _M.run_peek()
    local cl = require_content_length()
    check_edge_limits(cl)

    local sock, sock_err = ngx.req.socket()
    if not sock then
        ngx.log(ngx.ERR, "edge peek: socket unavailable: ", sock_err)
        edge_metrics.record_body_stream()
        edge_metrics.record_phase2_pass()
        return
    end

    sock:settimeout(500)
    local chunk = sock:receive(MAX_SCAN_BYTES)
    edge_metrics.record_body_peek()

    local fraud_score = fraud_score_from_headers()
    if chunk and #chunk > 0 then
        local campaign_id, perr = edge_parse_dfa.extract_campaign_id(chunk, cl, INGRESS_SCHEMA)
        if perr == edge_parse_dfa.ERR_OVERSIZE then
            reject_oversize()
        end
        if campaign_id and campaign_id ~= "" then
            ngx.ctx.campaign_id = campaign_id
        end
        apply_campaign_rl(campaign_id, fraud_score)
    else
        local hdr_cid = campaign_id_from_headers()
        if hdr_cid and hdr_cid ~= "" then
            ngx.ctx.campaign_id = hdr_cid
        end
        apply_campaign_rl(hdr_cid, fraud_score)
    end

    edge_metrics.record_phase2_pass()
end

function _M.run()
    local mode = config_string("body_mode", "EDGE_BODY_MODE", "full")
    if mode == "stream" then
        _M.run_stream()
    elseif mode == "peek" then
        _M.run_peek()
    else
        _M.run_full()
    end
end

return _M
