local redis = require "resty.redis"
local shared_dict = ngx.shared.circuit_breaker

-- Configuration
local REDIS_HOST = os.Getenv("REDIS_HOST") or "127.0.0.1"
local REDIS_PORT = os.Getenv("REDIS_PORT") or 6379
local REDIS_PASS = os.Getenv("REDIS_PASS") or ""
local FAIL_THRESHOLD = 0.05 -- 5%
local SAMPLE_WINDOW = 100

-- Stats tracking
local total_reqs, err = shared_dict:incr("total_reqs", 1, 0)
local redis_errs = shared_dict:get("redis_errs") or 0

-- Circuit Breaker Logic
if total_reqs > SAMPLE_WINDOW then
    if (redis_errs / total_reqs) > FAIL_THRESHOLD then
        ngx.log(ngx.ERR, "Edge Circuit Breaker OPEN: fail rate ", (redis_errs / total_reqs))
        ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
    end
    -- Reset window periodically
    if total_reqs > 1000 then
        shared_dict:set("total_reqs", 0)
        shared_dict:set("redis_errs", 0)
    end
end

-- IP Blacklist Check
local red = redis:new()
red:set_timeout(100) -- 100ms

local ok, err = red:connect(REDIS_HOST, REDIS_PORT)
if not ok then
    shared_dict:incr("redis_errs", 1, 0)
    ngx.log(ngx.ERR, "failed to connect to redis: ", err)
    return -- Fail-open
end

if REDIS_PASS ~= "" then
    local res, err = red:auth(REDIS_PASS)
    if not res then
        shared_dict:incr("redis_errs", 1, 0)
        return -- Fail-open
    end
end

local client_ip = ngx.var.remote_addr

-- Check manual blacklist
local is_manual, err = red:sismember("blacklist:manual", client_ip)
if is_manual == 1 then
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

-- Check auto blacklist (or Bloom filter)
local is_auto, err = red:sismember("blacklist:auto", client_ip)
if is_auto == 1 then
    ngx.exit(ngx.HTTP_FORBIDDEN)
end

-- Put connection back to pool
red:set_keepalive(10000, 100)
