-- edge-blacklist-sync.lua: background SMEMBERS sync from Redis shard 0 into blacklist_cache.
-- Keys use b:<ip> = version stamp; _bl_ver bumps each successful sync (no dict iteration).

local redis = require "resty.redis"

local _M = {}

local cache = ngx.shared.blacklist_cache
local sentinel_cache = ngx.shared.sentinel_cache

local REDIS_HOST = os.getenv("REDIS_HOST") or "127.0.0.1"
local REDIS_PORT = os.getenv("REDIS_PORT") or 6379
local REDIS_PASS = os.getenv("REDIS_PASS") or ""
local REDIS_ADDRS = os.getenv("REDIS_ADDRS") or ""
local REDIS_SENTINEL_ADDRS = os.getenv("REDIS_SENTINEL_ADDRS") or ""
local REDIS_MASTER_NAMES = os.getenv("REDIS_MASTER_NAMES") or ""
local SENTINEL_CACHE_TTL = 5

local shards
local sentinel_addrs
local master_names
local sentinel_enabled

local function parse_addr_list(raw)
    local out = {}
    if raw == "" then
        return out
    end
    for addr in string.gmatch(raw, "([^,]+)") do
        addr = string.match(addr, "^%s*(.-)%s*$")
        local host, port = string.match(addr, "([^:]+):(%d+)")
        if host and port then
            table.insert(out, {host = host, port = tonumber(port)})
        end
    end
    return out
end

local function sentinel_master_addr(shard_idx, names, sentinels)
    if #names == 0 or #sentinels == 0 then
        return nil, "sentinel not configured"
    end
    if shard_idx < 1 or shard_idx > #names then
        return nil, "shard index out of range"
    end
    local master_name = names[shard_idx]
    local cache_key = "m:" .. master_name
    local cached = sentinel_cache:get(cache_key)
    if cached then
        local host, port = string.match(cached, "([^:]+):(%d+)")
        if host and port then
            return {host = host, port = tonumber(port)}, nil
        end
    end

    local sentinel = sentinels[((shard_idx - 1) % #sentinels) + 1]
    local sred = redis:new()
    sred:set_timeout(200)
    local ok, err = sred:connect(sentinel.host, sentinel.port)
    if not ok then
        return nil, "sentinel connect: " .. (err or "unknown")
    end
    if REDIS_PASS ~= "" then
        local res, auth_err = sred:auth(REDIS_PASS)
        if not res then
            return nil, "sentinel auth: " .. (auth_err or "unknown")
        end
    end

    local res, cmd_err = sred:sentinel("get-master-addr-by-name", master_name)
    sred:set_keepalive(10000, 32)
    if not res or type(res) ~= "table" or #res < 2 then
        return nil, "sentinel get-master-addr-by-name: " .. (cmd_err or "empty response")
    end
    local host = res[1]
    local port = tonumber(res[2])
    if not host or not port then
        return nil, "invalid master addr from sentinel"
    end
    sentinel_cache:set(cache_key, host .. ":" .. port, SENTINEL_CACHE_TTL)
    return {host = host, port = port}, nil
end

local function ensure_redis_topology()
    if shards then
        return
    end
    shards = parse_addr_list(REDIS_ADDRS)
    if #shards == 0 then
        shards = {{host = REDIS_HOST, port = tonumber(REDIS_PORT)}}
    end
    sentinel_addrs = parse_addr_list(REDIS_SENTINEL_ADDRS)
    master_names = {}
    if REDIS_MASTER_NAMES ~= "" then
        for name in string.gmatch(REDIS_MASTER_NAMES, "([^,]+)") do
            name = string.match(name, "^%s*(.-)%s*$")
            if name ~= "" then
                table.insert(master_names, name)
            end
        end
    end
    sentinel_enabled = #sentinel_addrs > 0 and #master_names > 0
end

local function shard0_target()
    ensure_redis_topology()
    local target = shards[1]
    if sentinel_enabled then
        local resolved, resolve_err = sentinel_master_addr(1, master_names, sentinel_addrs)
        if resolved then
            target = resolved
        else
            ngx.log(ngx.WARN, "edge_blacklist_sync: sentinel resolve failed for shard 0: ", resolve_err)
        end
    end
    return target
end

local function connect_shard0()
    local target = shard0_target()
    local red = redis:new()
    red:set_timeout(500)

    local ok, err = red:connect(target.host, target.port)
    if not ok and sentinel_enabled then
        sentinel_cache:delete("m:" .. master_names[1])
        local resolved, resolve_err = sentinel_master_addr(1, master_names, sentinel_addrs)
        if resolved then
            target = resolved
            ok, err = red:connect(target.host, target.port)
        else
            return nil, "sentinel re-resolve: " .. (resolve_err or "unknown")
        end
    end
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

-- sync pulls blacklist:manual, blacklist:auto, and blacklist:fraud from shard 0 into shared dict.
function _M.sync()
    local red, err = connect_shard0()
    if not red then
        ngx.log(ngx.WARN, "edge_blacklist_sync: connect failed: ", err)
        return false
    end

    local manual, err1 = red:smembers("blacklist:manual")
    local auto, err2 = red:smembers("blacklist:auto")
    local fraud, err3 = red:smembers("blacklist:fraud")
    red:set_keepalive(10000, 8)

    if not manual or not auto or not fraud then
        ngx.log(ngx.ERR, "edge_blacklist_sync: smembers failed: ", err1 or err2 or err3)
        return false
    end

    local new_ver = (cache:get("_bl_ver") or 0) + 1
    local count = 0
    local seen = {}

    local function stamp(ip)
        if not ip or ip == "" or seen[ip] then
            return
        end
        seen[ip] = true
        cache:set("b:" .. ip, new_ver)
        count = count + 1
    end

    for _, ip in ipairs(manual) do
        stamp(ip)
    end
    for _, ip in ipairs(auto) do
        stamp(ip)
    end
    for _, ip in ipairs(fraud) do
        stamp(ip)
    end

    cache:set("_bl_ver", new_ver)
    cache:set("_bl_sync_ts", ngx.time())
    cache:set("_bl_count", count)
    ngx.log(ngx.INFO, "edge_blacklist_sync: ", count, " blocked IPs (ver=", new_ver, ")")
    return true
end

return _M
