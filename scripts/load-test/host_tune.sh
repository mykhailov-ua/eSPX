#!/usr/bin/env bash
# Host tuning for local k6 load tests: sysctl, ulimit checks, preflight report.
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

MODE="${1:-report}"
EDGE_CONF="$ROOT/deploy/edge/99-espx-edge.conf"
LOADTEST_CONF="$ROOT/deploy/edge/99-espx-loadtest.conf"
CONF_DST_EDGE="${ESPX_SYSCTL_CONF:-/etc/sysctl.d/99-espx-edge.conf}"
CONF_DST_LOAD="${ESPX_LOADTEST_SYSCTL_CONF:-/etc/sysctl.d/99-espx-loadtest.conf}"

log() { printf 'host-tune: %s\n' "$*"; }
warn() { printf 'host-tune: WARN: %s\n' "$*" >&2; }
die() { printf 'host-tune: ERROR: %s\n' "$*" >&2; exit 1; }

require_root() { [[ "$(id -u)" -eq 0 ]] || die "mode $MODE requires root (sudo)"; }

expected_keys() {
	local f=$1
	awk -F= '/^[[:space:]]*(net\.|fs\.)/ {
		gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
		print $1
	}' "$f"
}

normalize_values() { tr -s '[:space:]' ' ' | sed 's/^ //;s/ $//'; }

read_current() {
	sysctl -n "$1" 2>/dev/null | normalize_values || echo ""
}

read_expected() {
	local key=$1 file=$2
	grep -E "^[[:space:]]*${key//./\\.}[[:space:]]*=" "$file" | head -1 | sed 's/^[^=]*=[[:space:]]*//' | normalize_values
}

values_match() {
	local key=$1 cur=$2 exp=$3
	if [[ "$key" == "net.ipv4.tcp_tw_reuse" ]]; then
		awk -v c="$cur" 'BEGIN { exit (c + 0 >= 1) ? 0 : 1 }'
		return
	fi
	[[ "$cur" == "$exp" ]]
}

check_file() {
	local f=$1 label=$2
	local key cur exp fail=0
	[[ -f "$f" ]] || { warn "missing $label: $f"; return 1; }
	while IFS= read -r key; do
		[[ -n "$key" ]] || continue
		cur="$(read_current "$key")"
		exp="$(read_expected "$key" "$f")"
		if values_match "$key" "$cur" "$exp"; then
			log "OK   $key = $cur"
		elif [[ -z "$cur" ]]; then
			warn "MISS $key (expected $exp)"
			fail=1
		else
			warn "DIFF $key current=$cur expected=$exp"
			fail=1
		fi
	done < <(expected_keys "$f")
	return "$fail"
}

report_ulimit() {
	log "--- session limits ---"
	log "ulimit -n (open files) = $(ulimit -n 2>/dev/null || echo na)"
	log "ulimit -u (max processes) = $(ulimit -u 2>/dev/null || echo na)"
	if command -v systemctl >/dev/null 2>&1; then
		systemctl show "user@$(id -u).service" -p LimitNOFILE 2>/dev/null | sed 's/^/  /' || true
	fi
}

report_ss() {
	log "--- TCP summary (ss -s) ---"
	ss -s 2>/dev/null | sed 's/^/  /' || warn "ss not available"
}

report_ptrace() {
	local v
	v="$(sysctl -n kernel.yama.ptrace_scope 2>/dev/null || echo na)"
	log "kernel.yama.ptrace_scope = $v (0=allow strace attach for syscall profiling)"
	[[ "$v" == "0" ]] || warn "strace -p blocked; set ptrace_scope=0 temporarily for syscall analysis"
}

report_status() {
	log "=== eSPX load-test host preflight ==="
	log "edge sysctl: $EDGE_CONF"
	check_file "$EDGE_CONF" "edge" || true
	log "load-test sysctl: $LOADTEST_CONF"
	check_file "$LOADTEST_CONF" "loadtest" || true
	report_ulimit
	report_ss
	report_ptrace
	log "installed: edge=$([ -f "$CONF_DST_EDGE" ] && echo yes || echo no) loadtest=$([ -f "$CONF_DST_LOAD" ] && echo yes || echo no)"
}

verify_tuning() {
	local fail=0
	check_file "$EDGE_CONF" "edge" || fail=1
	check_file "$LOADTEST_CONF" "loadtest" || fail=1
	local n
	n="$(ulimit -n 2>/dev/null || echo 0)"
	if [[ "$n" -lt 100000 ]]; then
		warn "ulimit -n $n < 100000 (raise for high-connection load tests)"
		fail=1
	fi
	[[ -f "$CONF_DST_EDGE" && -f "$CONF_DST_LOAD" ]] || {
		warn "sysctl conf not installed under /etc/sysctl.d/"
		fail=1
	}
	[[ "$fail" -eq 0 ]] || exit 1
	log "verify: OK"
}

apply_tuning() {
	require_root
	[[ -f "$EDGE_CONF" && -f "$LOADTEST_CONF" ]] || die "missing sysctl conf in deploy/edge/"
	install -d "$(dirname "$CONF_DST_EDGE")"
	install -m 0644 "$EDGE_CONF" "$CONF_DST_EDGE"
	install -m 0644 "$LOADTEST_CONF" "$CONF_DST_LOAD"
	log "installed $CONF_DST_EDGE and $CONF_DST_LOAD"
	sysctl --system >/dev/null
	log "applied via sysctl --system"

	# Session guidance (cannot persist from script without limits.d entry).
	local nr
	nr="$(ulimit -n 2>/dev/null || echo 0)"
	if [[ "$nr" -lt 100000 ]]; then
		warn "current shell ulimit -n=$nr; add '* soft nofile 1048576' to /etc/security/limits.d/99-espx-loadtest.conf and re-login"
	fi
	log "apply: done — run: bash scripts/load-test/host_tune.sh verify"
}

case "$MODE" in
apply) apply_tuning ;;
verify) verify_tuning ;;
report) report_status ;;
-h | --help)
	cat <<EOF
Usage: host_tune.sh <apply|verify|report>

  apply   install edge + load-test sysctl under /etc/sysctl.d/ and sysctl --system
  verify  exit 1 if live values differ from repo or ulimit too low
  report  print current vs expected (default)

See: var/load-test/HOST_LOAD_TEST_TUNING_REPORT.md
EOF
	;;
*) die "unknown mode: $MODE" ;;
esac
