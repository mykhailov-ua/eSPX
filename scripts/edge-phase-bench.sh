#!/usr/bin/env bash
# Micro-benchmark for edge two-phase validation: scrape /metrics/edge before/after a POST flood.
# Usage: edge-phase-bench.sh <scenario> [requests] [concurrency]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EDGE_URL="${EDGE_URL:-http://127.0.0.1:8180}"
METRICS_URL="${EDGE_URL}/metrics/edge"
OUT_DIR="${OUT_DIR:-$ROOT/var/edge-baseline}"
SCENARIO="${1:-legit}"
REQUESTS="${2:-500}"
CONCURRENCY="${3:-32}"
REDIS_PORT="${REDIS_PORT:-6479}"
REDIS_PASS="${REDIS_PASS:-redis_secure_pass_456}"

log() { printf 'edge-phase-bench: %s\n' "$*"; }

metric_val() {
	local name=$1
	curl -sf --max-time 3 "$METRICS_URL" 2>/dev/null | awk -v n="$name" '$1 == n { print $2; exit }'
}

snapshot_metrics() {
	local prefix=$1
	local ts
	ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	mkdir -p "$OUT_DIR"
	{
		echo "captured_at_utc=$ts"
		echo "scenario=$SCENARIO"
		echo "requests=$REQUESTS"
		echo "concurrency=$CONCURRENCY"
		echo "phase1_pass=$(metric_val espx_edge_phase1_pass_total || echo 0)"
		echo "body_read=$(metric_val espx_edge_body_read_total || echo 0)"
		echo "blocked_ip=$(metric_val espx_edge_blocked_ip_total || echo 0)"
		echo "blocked_rl=$(metric_val espx_edge_blocked_campaign_rl_total || echo 0)"
		echo "circuit_reject=$(metric_val espx_edge_circuit_reject_total || echo 0)"
	} | tee "$OUT_DIR/${prefix}-${SCENARIO}.txt"
}

# Minimal protobuf AdEvent: campaign_id (field 1, 16-byte UUID) + event_type (field 2).
gen_proto_body() {
	python3 - <<'PY'
import uuid
uid = uuid.uuid4().bytes
et = b"click"
body = bytes([0x0A, len(uid)]) + uid + bytes([0x12, len(et)]) + et
import sys
sys.stdout.buffer.write(body)
PY
}

gen_json_body() {
	python3 - <<'PY'
import json, uuid
print(json.dumps({"campaign_id": str(uuid.uuid4()), "user_id": "bench", "type": "click"}))
PY
}

flood() {
	local body_file status_file
	body_file="$(mktemp)"
	status_file="$(mktemp)"
	case "$SCENARIO" in
	legit | legit_proto)
		gen_proto_body >"$body_file"
		;;
	legit_json)
		gen_json_body >"$body_file"
		;;
	blocked_ip)
		gen_proto_body >"$body_file"
		;;
	*)
		log "unknown scenario: $SCENARIO (use legit|legit_json|blocked_ip)"
		exit 1
		;;
	esac

	local ct="application/x-protobuf"
	if [[ "$SCENARIO" == "legit_json" ]]; then
		ct="application/json"
	fi

	export BODY_FILE="$body_file" STATUS_FILE="$status_file" EDGE_URL="$EDGE_URL" CT="$ct"
	seq "$REQUESTS" | xargs -P "$CONCURRENCY" -I{} bash -c '
		code=$(curl -sf -o /dev/null -w "%{http_code}" --max-time 5 \
			-X POST "$EDGE_URL/track" \
			-H "Content-Type: $CT" \
			--data-binary @"$BODY_FILE" 2>/dev/null || echo "000")
		echo "$code" >> "$STATUS_FILE"
	'

	rm -f "$body_file"
	sort "$status_file" | uniq -c | sort -rn
	rm -f "$status_file"
}

redis_cli() {
	if command -v redis-cli >/dev/null 2>&1; then
		redis-cli -p "$REDIS_PORT" -a "$REDIS_PASS" --no-auth-warning "$@"
	elif docker exec espx-redis-0-1 redis-cli -p 6379 -a "$REDIS_PASS" --no-auth-warning "$@" 2>/dev/null; then
		return 0
	else
		log "redis-cli unavailable; cannot prepare blocked_ip scenario"
		exit 1
	fi
}

prepare_blocked_ip() {
	log "adding 127.0.0.1 to blacklist:manual on redis shard 0"
	redis_cli SADD blacklist:manual 127.0.0.1 >/dev/null
	log "reloading nginx to clear warm per-IP cache paths"
	docker exec espx-nginx-1 nginx -s reload 2>/dev/null || true
	sleep 6
}

case "$SCENARIO" in
blocked_ip) prepare_blocked_ip ;;
esac

log "BEFORE snapshot ($SCENARIO, n=$REQUESTS, c=$CONCURRENCY)"
snapshot_metrics "before"

start_ms=$(date +%s%3N)
flood
end_ms=$(date +%s%3N)
elapsed=$((end_ms - start_ms))
log "flood wall time: ${elapsed}ms"

sleep 1
log "AFTER snapshot"
snapshot_metrics "after"

log "done; results in $OUT_DIR/before-${SCENARIO}.txt and after-${SCENARIO}.txt"
