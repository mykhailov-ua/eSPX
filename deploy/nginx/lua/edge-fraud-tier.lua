-- edge-fraud-tier.lua: maps fraud_score (0-100) to perimeter rate-limit tiers.

local _M = {}

local PASS_MAX = 30
local SUSPECT_MAX = 60
local IVT_MAX = 80

-- tier_from_score returns tier name and clamped score.
function _M.tier_from_score(score)
    local n = tonumber(score) or 0
    if n < 0 then
        n = 0
    elseif n > 100 then
        n = 100
    end
    if n <= PASS_MAX then
        return "pass", n
    end
    if n <= SUSPECT_MAX then
        return "suspect", n
    end
    if n <= IVT_MAX then
        return "ivt", n
    end
    return "block", n
end

return _M
