-- init-worker.lua: tracker upstream health checks and edge config/blacklist sync (worker 0 only).

local hc = require "resty.upstream.healthcheck"
local edge_config = require "edge-config"
local blacklist_sync = require "edge-blacklist-sync"
local edge_slot_map = require "edge-slot-map"
local quarantine_sub = require "edge-quarantine-sub"

if ngx.worker.id() ~= 0 then
    return
end

local CONFIG_SYNC_INTERVAL = 5
local BLACKLIST_SYNC_INTERVAL = 5
local SLOT_MAP_SYNC_INTERVAL = tonumber(os.getenv("SLOT_MAP_SYNC_INTERVAL_SEC") or "") or 10

local function sync_edge_config(premature)
    if premature then
        return
    end
    edge_config.sync()
    local ok, timer_err = ngx.timer.at(CONFIG_SYNC_INTERVAL, sync_edge_config)
    if not ok then
        ngx.log(ngx.ERR, "failed to reschedule edge config sync: ", timer_err)
    end
end

local function sync_blacklist(premature)
    if premature then
        return
    end
    blacklist_sync.sync()
    local ok, timer_err = ngx.timer.at(BLACKLIST_SYNC_INTERVAL, sync_blacklist)
    if not ok then
        ngx.log(ngx.ERR, "failed to reschedule blacklist sync: ", timer_err)
    end
end

local timer_ok, timer_err = ngx.timer.at(0, sync_edge_config)
if not timer_ok then
    ngx.log(ngx.ERR, "failed to start edge config sync: ", timer_err)
end

timer_ok, timer_err = ngx.timer.at(0, sync_blacklist)
if not timer_ok then
    ngx.log(ngx.ERR, "failed to start blacklist sync: ", timer_err)
end

quarantine_sub.start()

local function sync_slot_map(premature)
    if premature then
        return
    end
    edge_slot_map.sync()
    local ok, err = ngx.timer.at(SLOT_MAP_SYNC_INTERVAL, sync_slot_map)
    if not ok then
        ngx.log(ngx.ERR, "failed to reschedule slot map sync: ", err)
    end
end

timer_ok, timer_err = ngx.timer.at(0, sync_slot_map)
if not timer_ok then
    ngx.log(ngx.ERR, "failed to start slot map sync: ", timer_err)
end

local ok, err = hc.spawn_checker({
    shm = "healthcheck",
    upstream = "trackers",
    type = "http",
    http_req = "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    interval = 2000,
    timeout = 1000,
    fall = 2,
    rise = 2,
    valid_statuses = {200},
    concurrency = 4,
})
if not ok then
    ngx.log(ngx.ERR, "failed to spawn upstream health checker: ", err)
end
