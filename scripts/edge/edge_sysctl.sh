#!/usr/bin/env bash
# Install and verify ingress sysctl tuning from deploy/edge/99-espx-edge.conf. Usage: edge_sysctl.sh apply|verify|report
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"
MODE="${1:-apply}"
CONF_SRC="$ROOT/deploy/edge/99-espx-edge.conf"
CONF_DST="${ESPX_SYSCTL_CONF:-/etc/sysctl.d/99-espx-edge.conf}"

log() { printf 'edge-sysctl: %s\n' "$*"; }
warn() { printf 'edge-sysctl: WARN: %s\n' "$*" >&2; }
die() { printf 'edge-sysctl: ERROR: %s\n' "$*" >&2; exit 1; }

require_root() {
	[[ "$(id -u)" -eq 0 ]] || die "mode $MODE requires root (sudo)"
}

[[ -f "$CONF_SRC" ]] || die "missing $CONF_SRC"

# Returns expected sysctl keys from the repo conf (comments stripped).
expected_keys() {
	awk -F= '/^[[:space:]]*net\./ {
		gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
		print $1
	}' "$CONF_SRC"
}

# Normalizes "a b c" sysctl values for comparison.
normalize_values() {
	tr -s '[:space:]' ' ' | sed 's/^ //;s/ $//'
}

read_current() {
	local key=$1
	sysctl -n "$key" 2>/dev/null | normalize_values || echo ""
}

read_expected() {
	local key=$1
	grep -E "^[[:space:]]*${key//./\\.}[[:space:]]*=" "$CONF_SRC" | head -1 | sed 's/^[^=]*=[[:space:]]*//' | normalize_values
}

values_match() {
	local key=$1 cur=$2 exp=$3
	if [[ "$key" == "net.ipv4.tcp_tw_reuse" ]]; then
		awk -v c="$cur" 'BEGIN { exit (c + 0 >= 1) ? 0 : 1 }'
		return
	fi
	[[ "$cur" == "$exp" ]]
}

report_status() {
	local key cur exp
	log "config source: $CONF_SRC"
	log "install target: $CONF_DST"
	while IFS= read -r key; do
		[[ -n "$key" ]] || continue
		cur="$(read_current "$key")"
		exp="$(read_expected "$key")"
		if values_match "$key" "$cur" "$exp"; then
			log "OK   $key = $cur"
		elif [[ -z "$cur" ]]; then
			warn "MISS $key (expected $exp)"
		else
			warn "DIFF $key current=$cur expected=$exp"
		fi
	done < <(expected_keys)
}

verify_tuning() {
	local key cur exp fail=0
	while IFS= read -r key; do
		[[ -n "$key" ]] || continue
		cur="$(read_current "$key")"
		exp="$(read_expected "$key")"
		if ! values_match "$key" "$cur" "$exp"; then
			warn "$key: got '$cur', want '$exp'"
			fail=1
		fi
	done < <(expected_keys)

	if [[ -f "$CONF_DST" ]]; then
		log "installed conf present: $CONF_DST"
	else
		warn "missing $CONF_DST (values may reset on reboot)"
		fail=1
	fi

	[[ "$fail" -eq 0 ]] || exit 1
	log "verify: OK"
}

apply_tuning() {
	require_root
	install -d "$(dirname "$CONF_DST")"
	install -m 0644 "$CONF_SRC" "$CONF_DST"
	log "installed $CONF_DST"
	if command -v sysctl >/dev/null 2>&1; then
		sysctl --system >/dev/null
		log "applied via sysctl --system"
	else
		die "sysctl not found"
	fi
	log "apply: done"
}

case "$MODE" in
apply) apply_tuning ;;
verify) verify_tuning ;;
report) report_status ;;
-h | --help)
	cat <<EOF
Usage: edge_sysctl.sh <apply|verify|report>

  apply   install $CONF_DST and run sysctl --system
  verify  exit 1 if live values or install path differ from repo conf
  report  print current vs expected sysctl values

Environment: ESPX_SYSCTL_CONF (default /etc/sysctl.d/99-espx-edge.conf)
EOF
	;;
*)
	die "unknown mode: $MODE (use apply|verify|report)"
	;;
esac
