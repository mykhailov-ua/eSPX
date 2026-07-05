#!/usr/bin/env bash
# Benchmark edge body strategies: full | stream | peek | cap_8k (stream + nginx 8k).
# Usage: edge_payload_bench.sh [mode|all]
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"
LUA_DIR="$ROOT/deploy/nginx/lua"
NGINX_CONF="$ROOT/deploy/nginx/nginx.conf"
EDGE_URL="${EDGE_URL:-http://127.0.0.1:8180}"
OUT_DIR="${OUT_DIR:-$ROOT/var/edge-payload-bench}"
REQS_PER_SIZE="${REQS_PER_SIZE:-15}"

log() { printf 'edge-payload-bench: %s\n' "$*"; }

metric() {
	curl -sf --max-time 3 "${EDGE_URL}/metrics/edge" 2>/dev/null | awk -v n="$1" '$1 == n { print $2; exit }'
}

snapshot_metrics() {
	printf 'body_read=%s\n' "$(metric espx_edge_body_read_total || echo 0)"
	printf 'body_stream=%s\n' "$(metric espx_edge_body_stream_total || echo 0)"
	printf 'body_peek=%s\n' "$(metric espx_edge_body_peek_total || echo 0)"
	printf 'parse_oversize=%s\n' "$(metric espx_edge_parse_oversize_total || echo 0)"
	printf 'phase2_pass=%s\n' "$(metric espx_edge_phase2_pass_total || echo 0)"
	printf 'chunked_reject=%s\n' "$(metric espx_edge_chunked_reject_total || echo 0)"
}

gen_body() {
	local size=$1
	python3 - "$size" <<'PY'
import sys, uuid
size = int(sys.argv[1])
uid = uuid.uuid4().bytes
et = b"click"
head = bytes([0x0A, len(uid)]) + uid + bytes([0x12, len(et)]) + et
pad = max(0, size - len(head))
body = head + b"\x00" * pad
sys.stdout.buffer.write(body)
PY
}

set_lua_mode() {
	local mode=$1
	local max_body=${2:-8192}
	printf '%s\n' "$mode" >"$LUA_DIR/.edge_body_mode"
	printf '%s\n' "$max_body" >"$LUA_DIR/.edge_max_body_bytes"
}

set_nginx_body_limit() {
	local limit=$1
	sed -i '/location \/track/,/access_by_lua_file/ s/client_max_body_size [^;]*;/client_max_body_size '"${limit}"';/' "$NGINX_CONF"
}

reload_nginx() {
	if docker ps --format '{{.Names}}' | grep -q '^espx-nginx-1$'; then
		docker exec espx-nginx-1 nginx -s reload 2>/dev/null || docker restart espx-nginx-1
		sleep 2
	else
		log "WARN: espx-nginx-1 not running"
	fi
}

run_size_flood() {
	local size=$1
	local body_file
	body_file="$(mktemp)"
	gen_body "$size" >"$body_file"
	local cl
	cl=$(wc -c <"$body_file" | tr -d ' ')
	local status_file
	status_file="$(mktemp)"
	export EDGE_URL BODY_FILE="$body_file" STATUS_FILE="$status_file" CL="$cl"
	seq "$REQS_PER_SIZE" | xargs -P 4 -I{} bash -c '
		code=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 10 \
			-X POST "$EDGE_URL/track" \
			-H "Content-Type: application/x-protobuf" \
			-H "Content-Length: $CL" \
			--data-binary @"$BODY_FILE" 2>/dev/null || echo "000")
		echo "$code" >> "$STATUS_FILE"
	'
	rm -f "$body_file"
	sort "$status_file" | uniq -c | sort -rn | tr '\n' '; ' | sed 's/; $/\n/'
	rm -f "$status_file"
}

bench_mode() {
	local mode=$1
	local nginx_limit=${2:-8k}
	local lua_max=${3:-8192}
	local out="$OUT_DIR/${mode}.txt"
	mkdir -p "$OUT_DIR"

	log "=== mode=$mode nginx_limit=$nginx_limit lua_max=$lua_max ==="
	set_lua_mode "$mode" "$lua_max"
	set_nginx_body_limit "$nginx_limit"
	reload_nginx

	local before after
	before="$(snapshot_metrics)"
	local start_ms end_ms
	start_ms=$(date +%s%3N)

	{
		echo "mode=$mode"
		echo "nginx_client_max_body_size=$nginx_limit"
		echo "lua_EDGE_MAX_BODY_BYTES=$lua_max"
		echo "captured_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "requests_per_size=$REQS_PER_SIZE"
		echo "--- metrics before ---"
		echo "$before"
		echo "--- results ---"
		for size in 200 4096 16384 65536 524288; do
			printf 'size_%s_bytes: ' "$size"
			run_size_flood "$size"
		done
	} | tee "$out"

	end_ms=$(date +%s%3N)
	sleep 1
	after="$(snapshot_metrics)"
	{
		echo "--- metrics after ---"
		echo "$after"
		echo "wall_ms=$((end_ms - start_ms))"
	} | tee -a "$out"
	log "wrote $out"
}

MODE="${1:-all}"
case "$MODE" in
all)
	bench_mode full 1m 1048576
	bench_mode stream 1m 8192
	bench_mode peek 1m 8192
	bench_mode cap_8k 8k 8192
	# Restore production defaults (PERIMETER.md: full + 8k)
	set_lua_mode full 8192
	set_nginx_body_limit 8k
	reload_nginx
	log "restored production: full (IDEAS) + nginx 8k"
	;;
full | stream | peek | cap_8k)
	case "$MODE" in
	full) bench_mode full 1m 1048576 ;;
	stream) bench_mode stream 1m 8192 ;;
	peek) bench_mode peek 1m 8192 ;;
	cap_8k) bench_mode cap_8k 8k 8192 ;;
	esac
	;;
*)
	echo "Usage: $0 [all|full|stream|peek|cap_8k]" >&2
	exit 1
	;;
esac
