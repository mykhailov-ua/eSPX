#!/usr/bin/env bash
# Unit tests for deploy/nginx/lua/edge-parse-dfa.lua (luajit in espx-nginx-1 or Docker).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LUA_MOUNT="/etc/nginx/lua"

run_tests() {
	docker exec espx-nginx-1 /usr/local/openresty/luajit/bin/luajit -e "
package.path = '${LUA_MOUNT}/?.lua;;'
local dfa = require('edge-parse-dfa')
local uuid = string.char(0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,0x18,0x19)
local proto = string.char(0x0a, 16) .. uuid .. string.char(0x12, 5) .. 'click'
local cid, err = dfa.extract_campaign_id(proto, #proto)
assert(cid == '0a0b0c0d-0e0f-1011-1213-141516171819' and err == nil, 'proto')
local json = '{\"campaign_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"type\":\"click\"}'
cid, err = dfa.extract_campaign_id(json, #json)
assert(cid == '550e8400-e29b-41d4-a716-446655440000' and err == nil, 'json')
assert(dfa.check_content_length(dfa.MAX_BODY_BYTES + 1) == dfa.ERR_OVERSIZE, 'cl')
local over = string.char(0x0a, 100) .. string.rep('a', 100)
cid, err = dfa.extract_campaign_id(over, #over)
assert(err == dfa.ERR_OVERSIZE, 'campaign len')
print('edge-parse-dfa: all tests passed')
"
}

if docker ps --format '{{.Names}}' | grep -q '^espx-nginx-1$'; then
	run_tests
else
	docker run --rm -v "$ROOT/deploy/nginx/lua:${LUA_MOUNT}:ro" openresty/openresty:alpine \
		/usr/local/openresty/luajit/bin/luajit -e "
package.path = '${LUA_MOUNT}/?.lua;;'
local dfa = require('edge-parse-dfa')
print('edge-parse-dfa: smoke ok')
"
fi
