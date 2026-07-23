-- edge-ingress.lua: ingress protocol metric + fraud header forwarding for H2/H3 (M5-A).

local edge_metrics = require "edge-metrics"

local _M = {}

local function ingress_protocol()
    if ngx.var.http3 == "h3" then
        return "h3"
    end
    if ngx.var.http2 == "h2" then
        return "h2"
    end
    if ngx.var.server_protocol == "HTTP/2.0" then
        return "h2"
    end
    return "http/1.1"
end

function _M.record_and_forward()
    local proto = ingress_protocol()
    edge_metrics.record_ingress_protocol(proto)

    ngx.req.set_header("X-Original-Method", ngx.var.request_method)
    ngx.req.set_header("X-Original-Path", ngx.var.request_uri)

    local tls_hash = ngx.ctx.tls_hash
    if tls_hash and tls_hash ~= "" then
        ngx.req.set_header("X-TLS-Hash", tls_hash)
    elseif ngx.var.ssl_cipher and ngx.var.ssl_cipher ~= "" then
        -- Passive TLS metadata fallback when ClientHello hook is unavailable.
        ngx.req.set_header("X-TLS-Hash", ngx.var.ssl_protocol .. ":" .. ngx.var.ssl_cipher)
    end
end

return _M
