-- edge-shard-balancer.lua: routes /track to the tracker replica for the campaign shard (M9-05).
-- Uses edge-slot-map.get_shard() — same crc32 Castagnoli + slot table as Go StaticSlotSharder.

local balancer = require "ngx.balancer"
local slot_map = require "edge-slot-map"

local TRACKER_PEERS = {
    { host = "127.0.0.1", port = 8181 },
    { host = "127.0.0.1", port = 8182 },
    { host = "127.0.0.1", port = 8183 },
    { host = "127.0.0.1", port = 8184 },
}

local campaign_id = ngx.ctx.campaign_id
if not campaign_id or campaign_id == "" then
    return
end

local shard = slot_map.get_shard(campaign_id)
if shard == nil then
    return
end

local idx = tonumber(shard) + 1
if idx < 1 or idx > #TRACKER_PEERS then
    ngx.log(ngx.WARN, "edge shard balancer: shard out of range: ", shard)
    return
end

local peer = TRACKER_PEERS[idx]
local ok, err = balancer.set_current_peer(peer.host, peer.port)
if not ok then
    ngx.log(ngx.ERR, "edge shard balancer: set_current_peer failed: ", err or "unknown")
end
