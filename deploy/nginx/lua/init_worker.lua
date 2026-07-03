    -- init_worker.lua: active /health probes for tracker upstream peers.
-- Worker 0 only; marks DEGRADED replicas down so nginx stops queueing proxy buffers to them.

local hc = require "resty.upstream.healthcheck"

if ngx.worker.id() ~= 0 then
    return
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
