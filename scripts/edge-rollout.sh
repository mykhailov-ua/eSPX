#!/usr/bin/env bash
# Edge ingress canary rollout: Phase 0 preflight → 48h soak sign-off → redis reconcile. Usage: edge-rollout.sh <preflight|canary-start|canary-signoff|post-deploy>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-}"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
CANARY_HOURS="${CANARY_HOURS:-48}"
BASELINE_DIR="${BASELINE_DIR:-$ROOT/var/edge-baseline}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9190}"

log() { printf 'edge-rollout: %s\n' "$*"; }
warn() { printf 'edge-rollout: WARN: %s\n' "$*" >&2; }
die() { printf 'edge-rollout: ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
	cat <<EOF
Usage: edge-rollout.sh <preflight|canary-start|canary-signoff|post-deploy>

  preflight       Phase 0: sysctl/NIC verify, prod tuning, baseline snapshot
  canary-start    Capture canary baseline before traffic shift (writes canary-start.txt)
  canary-signoff  Compare current SLA vs canary-start after CANARY_HOURS soak (default 48)
  post-deploy     redis-reconcile-post-deploy.sh + SLA verify

Environment: ENV_FILE, CANARY_HOURS, PROMETHEUS_URL, BASELINE_DIR, STRICT=1
Acceptance: tracker p95<=50ms p99<=80ms; no TrackerLatency* alerts during soak.
EOF
}

prom_alert_firing() {
	local alert=$1
	local url body
	url="${PROMETHEUS_URL%/}/api/v1/alerts"
	body="$(curl -sf --max-time 5 "$url" 2>/dev/null)" || return 1
	python3 - "$alert" "$body" <<'PY'
import json, sys
name, raw = sys.argv[1], sys.argv[2]
data = json.loads(raw)
for a in data.get("data", {}).get("alerts") or []:
    if a.get("labels", {}).get("alertname") == name and a.get("state") == "firing":
        sys.exit(0)
sys.exit(1)
PY
}

preflight() {
	bash "$ROOT/scripts/edge-phase0.sh" "$ENV_FILE"
}

canary_start() {
	mkdir -p "$BASELINE_DIR"
	STRICT=1 bash "$ROOT/scripts/edge-baseline.sh" snapshot
	cp "$BASELINE_DIR/latest.txt" "$BASELINE_DIR/canary-start.txt"
	log "canary baseline saved to $BASELINE_DIR/canary-start.txt"
	log "route canary ingress traffic; soak for ${CANARY_HOURS}h then: edge-rollout.sh canary-signoff"
}

canary_signoff() {
	[[ -f "$BASELINE_DIR/canary-start.txt" ]] || die "missing $BASELINE_DIR/canary-start.txt — run canary-start first"

	STRICT=1 bash "$ROOT/scripts/edge-baseline.sh" verify
	cp "$BASELINE_DIR/latest.txt" "$BASELINE_DIR/canary-end.txt"

	local fail=0
	for alert in TrackerLatencyP95Warning TrackerLatencyP99Critical RedisLuaLatencyHigh EdgeCircuitBreakerRejectHigh EdgeTrack503RateHigh; do
		if prom_alert_firing "$alert"; then
			warn "alert $alert is firing"
			fail=1
		else
			log "alert $alert: not firing"
		fi
	done

	if [[ "$fail" -ne 0 ]]; then
		die "canary sign-off failed: latency or edge alerts firing"
	fi

	log "canary sign-off OK — proceed with full ingress rollout"
	log "compare baselines: diff $BASELINE_DIR/canary-start.txt $BASELINE_DIR/canary-end.txt"
}

post_deploy() {
	bash "$ROOT/scripts/redis-reconcile-post-deploy.sh" "$ENV_FILE"
	STRICT=1 bash "$ROOT/scripts/edge-baseline.sh" verify
	log "post-deploy reconcile and SLA verify OK"
}

case "$MODE" in
preflight) preflight ;;
canary-start) canary_start ;;
canary-signoff) canary_signoff ;;
post-deploy) post_deploy ;;
-h | --help | "") usage ;;
*) die "unknown mode: $MODE" ;;
esac
