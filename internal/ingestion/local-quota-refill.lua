-- Atomically debit a local-quanta chunk from budget:quota (M8-02).
-- KEYS[1] budget:quota:{campaign_id}
-- ARGV[1] chunk_size micro-units
-- Returns: debited amount, or -1 when insufficient.

local q = tonumber(redis.call("GET", KEYS[1]) or "0")
local chunk = tonumber(ARGV[1]) or 0
if chunk <= 0 then
    return -1
end
if q < chunk then
    return -1
end
redis.call("DECRBY", KEYS[1], chunk)
return chunk
