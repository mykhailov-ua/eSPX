#!/usr/bin/env bash
# Phase 0 edge hardening preflight: sysctl, NIC, prod tuning, Prometheus baseline. Usage: edge_phase0.sh [env-file]
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"
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

run_check "prod FILTER_TIMEOUT_MS" bash "$SCRIPTS/edge/verify_prod_tuning.sh" "$ENV_FILE" || true

if [[ -x "$SCRIPTS/edge/edge_sysctl.sh" ]]; then
	run_check "sysctl" bash "$SCRIPTS/edge/edge_sysctl.sh" verify || {
		[[ "$STRICT" == "1" ]] || warn "sysctl not applied (run: sudo bash scripts/edge/edge_sysctl.sh apply)"
	}
else
	warn "edge_sysctl.sh missing"
fi

if [[ -x "$SCRIPTS/edge/edge_nic_tune.sh" ]]; then
	run_check "NIC RX/IRQ" bash "$SCRIPTS/edge/edge_nic_tune.sh" verify || {
		[[ "$STRICT" == "1" ]] || warn "NIC tuning not applied (run: sudo bash scripts/edge/edge_nic_tune.sh apply)"
	}
else
	warn "edge_nic_tune.sh missing"
fi

run_check "nginx edge metrics" bash -c '
	curl -sf --max-time 3 http://127.0.0.1:8180/metrics/edge | grep -q espx_edge_phase1_pass_total
' || warn "nginx :8180 /metrics/edge unreachable (start full stack)"

run_check "prometheus baseline snapshot" bash "$SCRIPTS/edge/edge_baseline.sh" snapshot || true

if [[ "$fail" -ne 0 && "$STRICT" == "1" ]]; then
	die "one or more Phase 0 checks failed (STRICT=1)"
fi

log "Phase 0 preflight complete (see var/edge-baseline/latest.txt)"
