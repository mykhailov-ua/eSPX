-- edge-asn.lua: CDN and mobile ASN whitelist from config:values (edge_config sync).

local edge_config = require "edge-config"

local _M = {}

-- is_whitelisted reports whether asn bypasses edge blacklist and tier blocks.
function _M.is_whitelisted(asn)
    if not asn or asn == "" then
        return false
    end
    return edge_config.asn_whitelisted(asn)
end

return _M
