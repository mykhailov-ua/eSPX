-- Return unused local-quanta chunk to budget:quota (M14-13 reverse of refill).
-- KEYS[1] budget:quota:{campaign_id}
-- ARGV[1] amount micro-units to credit back
-- Returns: new quota balance after INCRBY.

local amt = tonumber(ARGV[1]) or 0
if amt <= 0 then
    return tonumber(redis.call("GET", KEYS[1]) or "0")
end
return redis.call("INCRBY", KEYS[1], amt)
