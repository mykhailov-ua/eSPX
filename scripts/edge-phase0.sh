#!/usr/bin/env bash
# Phase 0 edge hardening preflight: sysctl, NIC, prod tuning, Prometheus baseline. Usage: edge-phase0.sh [env-file]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env}"
STRICT="${STRICT:-0}"

log() { printf 'edge-phase0: %s\n' "$*"; }
warn() { printf 'edge-phase0: WARN: %s\n' "$*" >&2; }
die() { printf 'edge-phase0: ERROR: %s\n' "$*" >&2; exit 1; }

fail=0

run_check() {
	local name=$1
	shift
	log "check: $name"
	if "$@"; then
		log "  OK: $name"
	else
		warn "  FAIL: $name"
		fail=1
	fi
}

# shellcheck disable=SC1090
if [[ -f "$ENV_FILE" ]]; then
	set -a
	# shellcheck source=/dev/null
	. "$ENV_FILE"
	set +a
fi

run_check "prod FILTER_TIMEOUT_MS" bash "$ROOT/scripts/verify-prod-tuning.sh" "$ENV_FILE" || true

if [[ -x "$ROOT/scripts/edge-sysctl.sh" ]]; then
	run_check "sysctl" bash "$ROOT/scripts/edge-sysctl.sh" verify || {
		[[ "$STRICT" == "1" ]] || warn "sysctl not applied (run: sudo bash scripts/edge-sysctl.sh apply)"
	}
else
	warn "edge-sysctl.sh missing"
fi

if [[ -x "$ROOT/scripts/edge-nic-tune.sh" ]]; then
	run_check "NIC RX/IRQ" bash "$ROOT/scripts/edge-nic-tune.sh" verify || {
		[[ "$STRICT" == "1" ]] || warn "NIC tuning not applied (run: sudo bash scripts/edge-nic-tune.sh apply)"
	}
else
	warn "edge-nic-tune.sh missing"
fi

run_check "nginx edge metrics" bash -c '
	curl -sf --max-time 3 http://127.0.0.1:8180/metrics/edge | grep -q espx_edge_phase1_pass_total
' || warn "nginx :8180 /metrics/edge unreachable (start full stack)"

run_check "prometheus baseline snapshot" bash "$ROOT/scripts/edge-baseline.sh" snapshot || true

if [[ "$fail" -ne 0 && "$STRICT" == "1" ]]; then
	die "one or more Phase 0 checks failed (STRICT=1)"
fi

log "Phase 0 preflight complete (see var/edge-baseline/latest.txt)"
