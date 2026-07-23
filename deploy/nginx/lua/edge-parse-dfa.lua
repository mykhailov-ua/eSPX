-- edge-parse-dfa.lua: single-pass byte DFA for /track body (protobuf wire + JSON).
-- Extracts campaign_id for edge_rl; rejects oversize fields and scan budget immediately.

local bit = require "bit"

local _M = {}

-- Hard ceiling aligned with tracker MAX_REQUEST_BODY_SIZE / nginx client_max_body_size.
_M.MAX_BODY_BYTES = 1048576
-- Max bytes the DFA walks per request (campaign_id is field 1 in typical AdEvent).
_M.MAX_SCAN_BYTES = 8192
-- Max length of campaign_id string value (UUID text or raw proto bytes).
_M.MAX_CAMPAIGN_LEN = 64
-- Max single protobuf length-delimited field skip (prevents huge skip allocations).
_M.MAX_FIELD_LEN = 65536

local MAX_BODY_BYTES = _M.MAX_BODY_BYTES
local MAX_SCAN_BYTES = _M.MAX_SCAN_BYTES
local MAX_CAMPAIGN_LEN = _M.MAX_CAMPAIGN_LEN
local MAX_FIELD_LEN = _M.MAX_FIELD_LEN

local ERR_OVERSIZE = "oversize"
local ERR_MALFORMED = "malformed"

local byte = string.byte
local char = string.char
local sub = string.sub

local HEX = "0123456789abcdef"

-- byte_to_hex returns two lowercase hex digits for one byte.
local function byte_to_hex(b)
    local hi = bit.rshift(b, 4) + 1
    local lo = bit.band(b, 0x0f) + 1
    return char(byte(HEX, hi), byte(HEX, lo))
end

-- format_campaign_id normalizes protobuf 16-byte UUID or passes through JSON UUID text.
local function format_campaign_id(raw)
    if not raw or raw == "" then
        return nil, nil
    end
    if #raw == 16 then
        local g = function(i)
            return byte_to_hex(byte(raw, i))
        end
        return table.concat({
            g(1), g(2), g(3), g(4), "-",
            g(5), g(6), "-",
            g(7), g(8), "-",
            g(9), g(10), "-",
            g(11), g(12), g(13), g(14), g(15), g(16),
        }), nil
    end
    if #raw > MAX_CAMPAIGN_LEN then
        return nil, ERR_OVERSIZE
    end
    return raw, nil
end

-- decode_varint reads one protobuf varint; returns value, next_pos, err.
local function decode_varint(data, pos, limit)
    local val = 0
    local shift = 0
    while pos <= limit do
        local b = byte(data, pos)
        if not b then
            return nil, nil, ERR_MALFORMED
        end
        pos = pos + 1
        val = val + bit.lshift(bit.band(b, 0x7f), shift)
        if bit.band(b, 0x80) == 0 then
            return val, pos, nil
        end
        shift = shift + 7
        if shift >= 35 then
            return nil, nil, ERR_MALFORMED
        end
    end
    return nil, nil, ERR_MALFORMED
end

-- scan_limit_for returns the number of body bytes the DFA may inspect.
local function scan_limit_for(body_len, content_length)
    local limit = body_len
    if content_length and content_length > 0 and content_length < limit then
        limit = content_length
    end
    if limit > MAX_SCAN_BYTES then
        limit = MAX_SCAN_BYTES
    end
    return limit
end

-- scan_proto_dfa walks protobuf wire format; stops on campaign_id (field 1) or scan budget.
local function scan_proto_dfa(data, scan_limit)
    local pos = 1
    while pos <= scan_limit do
        local tag_b = byte(data, pos)
        if not tag_b then
            break
        end
        pos = pos + 1
        local wire = bit.band(tag_b, 0x07)
        local field = bit.rshift(tag_b, 3)

        if wire == 0 then
            local _, next_pos, err = decode_varint(data, pos, scan_limit)
            if err or not next_pos then
                return nil, err or ERR_MALFORMED
            end
            pos = next_pos
        elseif wire == 1 then
            if pos + 7 > scan_limit then
                return nil, ERR_MALFORMED
            end
            pos = pos + 8
        elseif wire == 2 then
            local field_len, new_pos, err = decode_varint(data, pos, scan_limit)
            if err then
                return nil, err
            end
            if not field_len or not new_pos or field_len > MAX_FIELD_LEN then
                return nil, ERR_OVERSIZE
            end
            if new_pos + field_len - 1 > scan_limit then
                return nil, ERR_MALFORMED
            end
            if field == 1 then
                if field_len > MAX_CAMPAIGN_LEN then
                    return nil, ERR_OVERSIZE
                end
                local raw = sub(data, new_pos, new_pos + field_len - 1)
                return format_campaign_id(raw)
            end
            pos = new_pos + field_len
        elseif wire == 5 then
            if pos + 3 > scan_limit then
                return nil, ERR_MALFORMED
            end
            pos = pos + 4
        else
            return nil, ERR_MALFORMED
        end
    end
    return nil, nil
end

-- json_key_id classifies a JSON object key for campaign_id extraction.
local function json_key_id(key)
    if string.find(key, "\\", 1, true) then
        return nil
    end
    local n = #key
    if n == 11 and sub(key, 1, 8) == "campaign" and sub(key, 9, 11) == "_id" then
        return "campaign_id"
    end
    return nil
end

local function is_json_ws(b)
    return b == 32 or b == 9 or b == 10 or b == 13
end

-- skip_json_value advances past one JSON value starting at pos; returns next pos or nil, err.
local function skip_json_value(data, pos, scan_limit)
    local err
    local b = byte(data, pos)
    if not b then
        return nil, ERR_MALFORMED
    end
    if b == 34 then
        pos = pos + 1
        while pos <= scan_limit do
            local c = byte(data, pos)
            if not c then
                return nil, ERR_MALFORMED
            end
            if c == 34 then
                return pos + 1, nil
            end
            if c == 92 then
                pos = pos + 2
            else
                pos = pos + 1
            end
        end
        return nil, ERR_MALFORMED
    end
    if b == 123 then
        pos = pos + 1
        while pos <= scan_limit do
            while pos <= scan_limit and is_json_ws(byte(data, pos)) do
                pos = pos + 1
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            if byte(data, pos) == 125 then
                return pos + 1, nil
            end
            if byte(data, pos) ~= 34 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
            while pos <= scan_limit and byte(data, pos) ~= 34 do
                pos = pos + 1
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
            while pos <= scan_limit and is_json_ws(byte(data, pos)) do
                pos = pos + 1
            end
            if pos > scan_limit or byte(data, pos) ~= 58 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
            local next_pos
            next_pos, err = skip_json_value(data, pos, scan_limit)
            if not next_pos then
                return nil, err
            end
            pos = next_pos
            while pos <= scan_limit and is_json_ws(byte(data, pos)) do
                pos = pos + 1
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            local sep = byte(data, pos)
            if sep == 125 then
                return pos + 1, nil
            end
            if sep ~= 44 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
        end
        return nil, ERR_MALFORMED
    end
    if b == 91 then
        pos = pos + 1
        while pos <= scan_limit do
            while pos <= scan_limit and is_json_ws(byte(data, pos)) do
                pos = pos + 1
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            if byte(data, pos) == 93 then
                return pos + 1, nil
            end
            local next_pos
            next_pos, err = skip_json_value(data, pos, scan_limit)
            if not next_pos then
                return nil, err
            end
            pos = next_pos
            while pos <= scan_limit and is_json_ws(byte(data, pos)) do
                pos = pos + 1
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            local sep = byte(data, pos)
            if sep == 93 then
                return pos + 1, nil
            end
            if sep ~= 44 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
        end
        return nil, ERR_MALFORMED
    end
    if (b >= 48 and b <= 57) or b == 45 then
        pos = pos + 1
        while pos <= scan_limit do
            local c = byte(data, pos)
            if not c then
                break
            end
            if not ((c >= 48 and c <= 57) or c == 46 or c == 101 or c == 69 or c == 43 or c == 45) then
                break
            end
            pos = pos + 1
        end
        return pos, nil
    end
    if b == 116 then
        if sub(data, pos, pos + 3) == "true" then
            return pos + 4, nil
        end
        return nil, ERR_MALFORMED
    end
    if b == 102 then
        if sub(data, pos, pos + 4) == "false" then
            return pos + 5, nil
        end
        return nil, ERR_MALFORMED
    end
    if b == 110 then
        if sub(data, pos, pos + 3) == "null" then
            return pos + 4, nil
        end
        return nil, ERR_MALFORMED
    end
    return nil, ERR_MALFORMED
end

-- scan_json_dfa walks JSON object keys; extracts campaign_id string value (last wins).
local function scan_json_dfa(data, scan_limit)
    local err
    local last_cid = nil
    local pos = 1
    while pos <= scan_limit and is_json_ws(byte(data, pos)) do
        pos = pos + 1
    end
    if pos > scan_limit or byte(data, pos) ~= 123 then
        return nil, ERR_MALFORMED
    end
    pos = pos + 1

    while pos <= scan_limit do
        while pos <= scan_limit and is_json_ws(byte(data, pos)) do
            pos = pos + 1
        end
        if pos > scan_limit then
            return nil, ERR_MALFORMED
        end
        if byte(data, pos) == 125 then
            return last_cid, nil
        end
        if byte(data, pos) ~= 34 then
            return nil, ERR_MALFORMED
        end
        pos = pos + 1
        local key_start = pos
        while pos <= scan_limit and byte(data, pos) ~= 34 do
            if byte(data, pos) == 92 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
        end
        if pos > scan_limit then
            return nil, ERR_MALFORMED
        end
        local key = sub(data, key_start, pos - 1)
        pos = pos + 1

        while pos <= scan_limit and is_json_ws(byte(data, pos)) do
            pos = pos + 1
        end
        if pos > scan_limit or byte(data, pos) ~= 58 then
            return nil, ERR_MALFORMED
        end
        pos = pos + 1
        while pos <= scan_limit and is_json_ws(byte(data, pos)) do
            pos = pos + 1
        end
        if pos > scan_limit then
            return nil, ERR_MALFORMED
        end

        local kid = json_key_id(key)
        if kid == "campaign_id" then
            if byte(data, pos) ~= 34 then
                return nil, ERR_MALFORMED
            end
            pos = pos + 1
            local val_start = pos
            while pos <= scan_limit do
                local c = byte(data, pos)
                if not c then
                    return nil, ERR_MALFORMED
                end
                if c == 34 then
                    break
                end
                if c == 0 then
                    return nil, ERR_MALFORMED
                end
                if c == 92 then
                    pos = pos + 2
                else
                    pos = pos + 1
                end
            end
            if pos > scan_limit then
                return nil, ERR_MALFORMED
            end
            local raw = sub(data, val_start, pos - 1)
            if #raw > MAX_CAMPAIGN_LEN then
                return nil, ERR_OVERSIZE
            end
            last_cid = raw
            pos = pos + 1
        else
            local next_pos
            next_pos, err = skip_json_value(data, pos, scan_limit)
            if not next_pos then
                return nil, err
            end
            pos = next_pos
        end

        while pos <= scan_limit and is_json_ws(byte(data, pos)) do
            pos = pos + 1
        end
        if pos > scan_limit then
            return nil, ERR_MALFORMED
        end
        local sep = byte(data, pos)
        if sep == 125 then
            return last_cid, nil
        end
        if sep ~= 44 then
            return nil, ERR_MALFORMED
        end
        pos = pos + 1
    end
    return nil, ERR_MALFORMED
end

-- check_content_length rejects declared bodies over MAX_BODY_BYTES before read_body.
function _M.check_content_length(content_length)
    if content_length and content_length > MAX_BODY_BYTES then
        return ERR_OVERSIZE
    end
    return nil
end

-- extract_campaign_id runs the byte DFA on body; content_length optional (from header).
function _M.extract_campaign_id(body, content_length)
    if content_length and content_length > MAX_BODY_BYTES then
        return nil, ERR_OVERSIZE
    end
    if not body or body == "" then
        return nil, nil
    end
    local body_len = #body
    if body_len > MAX_BODY_BYTES then
        return nil, ERR_OVERSIZE
    end

    local scan_limit = scan_limit_for(body_len, content_length)
    if scan_limit == 0 then
        return nil, nil
    end

    local pos = 1
    while pos <= scan_limit and is_json_ws(byte(body, pos)) do
        pos = pos + 1
    end
    local first = byte(body, pos)

    if first == 123 then
        return scan_json_dfa(body, scan_limit)
    end
    return scan_proto_dfa(body, scan_limit)
end

_M.ERR_OVERSIZE = ERR_OVERSIZE
_M.ERR_MALFORMED = ERR_MALFORMED

return _M
