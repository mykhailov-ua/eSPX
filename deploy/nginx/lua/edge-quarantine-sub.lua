-- edge-quarantine-sub.lua: immediate blacklist flush on fraud:quarantine pub/sub (shard 0).

local redis = require "resty.redis"
local blacklist_sync = require "edge-blacklist-sync"

local _M = {}

local REDIS_HOST = os.getenv("REDIS_HOST") or "127.0.0.1"
local REDIS_PORT = tonumber(os.getenv("REDIS_PORT") or 6379)
local REDIS_PASS = os.getenv("REDIS_PASS") or ""
local REDIS_ADDRS = os.getenv("REDIS_ADDRS") or ""

local CHANNEL = "fraud:quarantine"

local function parse_first_addr()
    if REDIS_ADDRS ~= "" then
        local first = string.match(REDIS_ADDRS, "^%s*([^,]+)")
        if first then
            local host, port = string.match(first, "([^:]+):(%d+)")
            if host and port then
                return host, tonumber(port)
            end
        end
    end
    return REDIS_HOST, REDIS_PORT
end

local function connect_shard0()
    local host, port = parse_first_addr()
    local red = redis:new()
    red:set_timeout(5000)
    local ok, err = red:connect(host, port)
    if not ok then
        return nil, err
    end
    if REDIS_PASS ~= "" then
        local res, auth_err = red:auth(REDIS_PASS)
        if not res then
            red:close()
            return nil, auth_err
        end
    end
    return red, nil
end

local function listen_loop()
    while not ngx.worker.exiting() do
        local red, err = connect_shard0()
        if not red then
            ngx.log(ngx.ERR, "edge_quarantine_sub: connect failed: ", err)
            ngx.sleep(2)
        else
            local res, sub_err = red:subscribe(CHANNEL)
            if not res then
                ngx.log(ngx.ERR, "edge_quarantine_sub: subscribe failed: ", sub_err)
                red:close()
                ngx.sleep(2)
            else
                ngx.log(ngx.INFO, "edge_quarantine_sub: subscribed to ", CHANNEL)
                while not ngx.worker.exiting() do
                    local reply, read_err = red:read_reply()
                    if not reply then
                        ngx.log(ngx.WARN, "edge_quarantine_sub: read_reply: ", read_err)
                        break
                    end
                    if type(reply) == "table" and reply[1] == "message" then
                        blacklist_sync.sync()
                    end
                end
                red:close()
            end
        end
    end
end

-- start spawns a background thread that flushes blacklist_cache on quarantine events.
function _M.start()
    local ok, err = ngx.thread.spawn(listen_loop)
    if not ok then
        ngx.log(ngx.ERR, "edge_quarantine_sub: failed to spawn listener: ", err)
    end
end

return _M
