#!/usr/bin/env bash
# One-shot Prometheus SLA snapshot for edge hardening baseline. Usage: edge_baseline.sh snapshot|verify
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"
MODE="${1:-snapshot}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9190}"
BASELINE_DIR="${BASELINE_DIR:-$ROOT/var/edge-baseline}"
STRICT="${STRICT:-0}"

# SLA targets from .cursorrules / edge-hardening-plan (milliseconds).
TRACKER_P95_MAX_MS=50
TRACKER_P99_MAX_MS=80
REDIS_LUA_P99_MAX_MS=15

log() { printf 'edge-baseline: %s\n' "$*"; }
warn() { printf 'edge-baseline: WARN: %s\n' "$*" >&2; }
die() { printf 'edge-baseline: ERROR: %s\n' "$*" >&2; exit 1; }

prom_query_scalar() {
	local query=$1
	local url body
	url="${PROMETHEUS_URL%/}/api/v1/query"
	body="$(curl -sfG --max-time 5 --data-urlencode "query=${query}" "$url" 2>/dev/null)" || return 1
	python3 - "$body" <<'PY'
import json, sys
raw = sys.argv[1]
data = json.loads(raw)
if data.get("status") != "success":
    sys.exit(1)
results = data.get("data", {}).get("result") or []
if not results:
    sys.exit(1)
val = results[0].get("value", [None, None])[1]
if val is None:
    sys.exit(1)
print(val)
PY
}

prometheus_up() {
	curl -sf --max-time 3 "${PROMETHEUS_URL%/}/-/ready" >/dev/null 2>&1
}

fetch_metrics() {
	TRACKER_P95_MS="$(prom_query_scalar 'histogram_quantile(0.95, sum(rate(ad_http_request_duration_seconds_bucket{job="tracker"}[5m])) by (le)) * 1000' || echo "")"
	TRACKER_P99_MS="$(prom_query_scalar 'histogram_quantile(0.99, sum(rate(ad_http_request_duration_seconds_bucket{job="tracker"}[5m])) by (le)) * 1000' || echo "")"
	REDIS_LUA_P99_MS="$(prom_query_scalar 'max(histogram_quantile(0.99, sum(rate(ad_redis_lua_duration_seconds_bucket{job="tracker"}[5m])) by (le, shard)) * 1000)' || echo "")"
	TRACKER_RPS="$(prom_query_scalar 'sum(rate(ad_http_request_duration_seconds_count{job="tracker"}[5m]))' || echo "")"
	EDGE_PHASE1_RPS="$(prom_query_scalar 'sum(rate(espx_edge_phase1_pass_total[5m]))' || echo "")"
	EDGE_CIRCUIT_RPS="$(prom_query_scalar 'sum(rate(espx_edge_circuit_reject_total[5m]))' || echo "")"
	EDGE_BLOCKED_IP_RPS="$(prom_query_scalar 'sum(rate(espx_edge_blocked_ip_total[5m]))' || echo "")"
	EDGE_BODY_READ_RPS="$(prom_query_scalar 'sum(rate(espx_edge_body_read_total[5m]))' || echo "")"
	EDGE_BL_AGE_SEC="$(prom_query_scalar 'time() - espx_edge_sync_last_success_timestamp' || echo "")"
}

cmp_sla() {
	local name=$1 val=$2 max=$3
	if [[ -z "$val" ]]; then
		warn "$name: no data"
		return 2
	fi
	awk -v v="$val" -v m="$max" 'BEGIN { if (v+0 > m+0) exit 1; exit 0 }' || {
		warn "$name=${val}ms exceeds target <${max}ms"
		return 1
	}
	log "$name=${val}ms (target <${max}ms) OK"
	return 0
}

write_snapshot() {
	local ts file
	ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	mkdir -p "$BASELINE_DIR"
	file="$BASELINE_DIR/baseline-${ts}.txt"
	{
		echo "# eSPX edge Phase 0.3 minimal baseline (not a 24h soak)"
		echo "captured_at_utc=$ts"
		echo "prometheus_url=$PROMETHEUS_URL"
		echo "tracker_p95_ms=${TRACKER_P95_MS:-na}"
		echo "tracker_p99_ms=${TRACKER_P99_MS:-na}"
		echo "redis_lua_p99_ms=${REDIS_LUA_P99_MS:-na}"
		echo "tracker_rps=${TRACKER_RPS:-na}"
		echo "edge_phase1_rps=${EDGE_PHASE1_RPS:-na}"
		echo "edge_circuit_reject_rps=${EDGE_CIRCUIT_RPS:-na}"
		echo "edge_blocked_ip_rps=${EDGE_BLOCKED_IP_RPS:-na}"
		echo "edge_body_read_rps=${EDGE_BODY_READ_RPS:-na}"
		echo "edge_blacklist_sync_age_sec=${EDGE_BL_AGE_SEC:-na}"
		echo "targets: tracker_p95<${TRACKER_P95_MAX_MS}ms tracker_p99<${TRACKER_P99_MAX_MS}ms redis_lua_p99<${REDIS_LUA_P99_MAX_MS}ms"
	} | tee "$BASELINE_DIR/latest.txt" | tee "$file"
	log "wrote $file"
}

snapshot() {
	if ! prometheus_up; then
		warn "Prometheus not reachable at $PROMETHEUS_URL"
		if [[ "$STRICT" == "1" ]]; then
			exit 1
		fi
		log "SKIP minimal baseline (start stack or set PROMETHEUS_URL; use STRICT=1 to fail)"
		exit 0
	fi

	fetch_metrics
	write_snapshot
}

verify_sla() {
	if ! prometheus_up; then
		die "Prometheus not reachable at $PROMETHEUS_URL"
	fi
	fetch_metrics
	local fail=0
	cmp_sla tracker_p95 "$TRACKER_P95_MS" "$TRACKER_P95_MAX_MS" || fail=1
	cmp_sla tracker_p99 "$TRACKER_P99_MS" "$TRACKER_P99_MAX_MS" || fail=1
	cmp_sla redis_lua_p99 "$REDIS_LUA_P99_MS" "$REDIS_LUA_P99_MAX_MS" || fail=1
	[[ "$fail" -eq 0 ]] || exit 1
	log "verify: OK"
}

case "$MODE" in
snapshot | "") snapshot ;;
verify) verify_sla ;;
-h | --help)
	cat <<EOF
Usage: edge_baseline.sh <snapshot|verify>

  snapshot  one-shot Prometheus SLA sample (default; skips if Prometheus down)
  verify    exit 1 if current p95/p99/redis_lua p99 exceed SLA targets

Environment: PROMETHEUS_URL, BASELINE_DIR, STRICT=1
EOF
	;;
*)
	die "unknown mode: $MODE (use snapshot|verify)"
	;;
esac
