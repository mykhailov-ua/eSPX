#!/usr/bin/env bash
# Ingress NIC RX ring and IRQ spread. Usage: edge_nic_tune.sh apply|verify|report|install-systemd
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"
MODE="${1:-apply}"
DRY_RUN="${DRY_RUN:-0}"
IRQ_STRATEGY="${IRQ_STRATEGY:-auto}"

log() { printf 'edge-nic-tune: %s\n' "$*"; }
warn() { printf 'edge-nic-tune: WARN: %s\n' "$*" >&2; }
die() { printf 'edge-nic-tune: ERROR: %s\n' "$*" >&2; exit 1; }

need_cmd() {
	command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

require_root() {
	[[ "$(id -u)" -eq 0 ]] || die "mode $MODE requires root (sudo)"
}

detect_interface() {
	if [[ -n "${INGRESS_INTERFACE:-}" ]]; then
		printf '%s\n' "$INGRESS_INTERFACE"
		return
	fi
	local iface
	iface="$(ip -o route show default 2>/dev/null | awk '{print $5; exit}')"
	[[ -n "$iface" ]] || die "could not detect default-route interface; set INGRESS_INTERFACE"
	printf '%s\n' "$iface"
}

iface_exists() {
	[[ -d "/sys/class/net/$1" ]]
}

parse_rx_ring() {
	local iface=$1 which=$2 # which: max|current
	need_cmd ethtool
	local line section=""
	while IFS= read -r line; do
		case "$line" in
		"Pre-set maximums:") section=max ;;
		"Current hardware settings:") section=current ;;
		RX:*)
			# Skip RX Mini / RX Jumbo lines.
			if [[ "$line" == *"RX Mini:"* || "$line" == *"RX Jumbo:"* ]]; then
				continue
			fi
			local val
			val="$(awk '{print $2}' <<<"$line")"
			if [[ "$section" == "$which" ]]; then
				printf '%s\n' "$val"
				return 0
			fi
			;;
		esac
	done < <(ethtool -g "$iface" 2>/dev/null)
	return 1
}

nic_irqs() {
	local iface=$1 irq line
	while IFS= read -r line; do
		irq="${line%%:*}"
		irq="${irq//[[:space:]]/}"
		[[ -n "$irq" && "$irq" =~ ^[0-9]+$ ]] || continue
		printf '%s\n' "$irq"
	done < <(grep -F "$iface" /proc/interrupts 2>/dev/null || true)
}

irq_cpu_affinity() {
	local irq=$1
	local path="/proc/irq/$irq/smp_affinity_list"
	[[ -r "$path" ]] || return 1
	tr -d '[:space:]' <"$path"
}

spread_irqs_round_robin() {
	local iface=$1
	local ncpus cpu=0 irq aff
	ncpus="$(nproc)"
	[[ "$ncpus" -gt 0 ]] || die "nproc returned 0"

	local count=0
	while IFS= read -r irq; do
		[[ -n "$irq" ]] || continue
		aff="$cpu"
		if [[ "$DRY_RUN" == "1" ]]; then
			log "[dry-run] would set IRQ $irq smp_affinity_list=$aff"
		else
			echo "$aff" >"/proc/irq/$irq/smp_affinity_list"
			log "IRQ $irq -> CPU $aff"
		fi
		cpu=$(( (cpu + 1) % ncpus ))
		count=$((count + 1))
	done < <(nic_irqs "$iface")

	[[ "$count" -gt 0 ]] || warn "no IRQ lines found for $iface in /proc/interrupts"
}

ensure_irqbalance() {
	if command -v systemctl >/dev/null 2>&1; then
		if systemctl is-enabled irqbalance >/dev/null 2>&1 || systemctl list-unit-files irqbalance.service >/dev/null 2>&1; then
			if [[ "$DRY_RUN" == "1" ]]; then
				log "[dry-run] would enable/start irqbalance"
				return 0
			fi
			systemctl enable irqbalance >/dev/null 2>&1 || true
			systemctl start irqbalance >/dev/null 2>&1 || true
			if systemctl is-active irqbalance >/dev/null 2>&1; then
				log "irqbalance is active"
				return 0
			fi
		fi
	fi
	if command -v irqbalance >/dev/null 2>&1 && pgrep -x irqbalance >/dev/null 2>&1; then
		log "irqbalance process running"
		return 0
	fi
	return 1
}

tune_irqs() {
	local iface=$1
	case "$IRQ_STRATEGY" in
	auto)
		if ensure_irqbalance; then
			return 0
		fi
		log "irqbalance unavailable; spreading IRQs manually"
		require_root
		spread_irqs_round_robin "$iface"
		;;
	irqbalance)
		require_root
		ensure_irqbalance || die "failed to start irqbalance"
		;;
	spread)
		require_root
		spread_irqs_round_robin "$iface"
		;;
	*)
		die "unknown IRQ_STRATEGY=$IRQ_STRATEGY (use auto|irqbalance|spread)"
		;;
	esac
}

tune_rx_ring() {
	local iface=$1 max_rx cur_rx
	max_rx="$(parse_rx_ring "$iface" max)" || die "ethtool -g $iface: could not read RX max"
	cur_rx="$(parse_rx_ring "$iface" current)" || die "ethtool -g $iface: could not read RX current"

	log "RX ring $iface: current=$cur_rx max=$max_rx"
	if [[ "$cur_rx" -ge "$max_rx" ]]; then
		log "RX ring already at hardware maximum"
		return 0
	fi

	if [[ "$DRY_RUN" == "1" ]]; then
		log "[dry-run] would run: ethtool -G $iface rx $max_rx"
		return 0
	fi
	require_root
	ethtool -G "$iface" rx "$max_rx"
	log "set RX ring to $max_rx"
}

irq_spread_ok() {
	local iface=$1
	local -a affinities=()
	local irq aff
	local count=0
	while IFS= read -r irq; do
		[[ -n "$irq" ]] || continue
		aff="$(irq_cpu_affinity "$irq" 2>/dev/null || echo "")"
		[[ -n "$aff" ]] || continue
		affinities+=("$aff")
		count=$((count + 1))
	done < <(nic_irqs "$iface")

	[[ "$count" -gt 0 ]] || {
		warn "no IRQ entries for $iface; skipping IRQ spread check"
		return 0
	}

	if [[ "$count" -eq 1 ]]; then
		log "single IRQ queue for $iface (RSS may be limited by hardware/driver)"
		return 0
	fi

	local -A seen=()
	for aff in "${affinities[@]}"; do
		seen["$aff"]=1
	done
	if [[ "${#seen[@]}" -ge 2 ]]; then
		return 0
	fi

	if ensure_irqbalance 2>/dev/null; then
		return 0
	fi

	return 1
}

report_status() {
	local iface
	iface="$(detect_interface)"
	iface_exists "$iface" || die "interface $iface not found"

	log "ingress interface: $iface"
	if command -v ethtool >/dev/null 2>&1; then
		local max_rx cur_rx
		max_rx="$(parse_rx_ring "$iface" max 2>/dev/null || echo "?")"
		cur_rx="$(parse_rx_ring "$iface" current 2>/dev/null || echo "?")"
		log "RX ring: current=$cur_rx max=$max_rx"
		if command -v ethtool >/dev/null 2>&1; then
			ethtool -l "$iface" 2>/dev/null | sed 's/^/  /' || true
		fi
	else
		warn "ethtool not installed"
	fi

	log "IRQ lines for $iface:"
	grep -F "$iface" /proc/interrupts 2>/dev/null | sed 's/^/  /' || warn "no IRQ lines in /proc/interrupts"

	local irq aff
	while IFS= read -r irq; do
		[[ -n "$irq" ]] || continue
		aff="$(irq_cpu_affinity "$irq" 2>/dev/null || echo "?")"
		log "  IRQ $irq affinity: $aff"
	done < <(nic_irqs "$iface")
}

verify_tuning() {
	local iface fail=0
	iface="$(detect_interface)"
	iface_exists "$iface" || die "interface $iface not found"
	need_cmd ethtool

	local max_rx cur_rx
	max_rx="$(parse_rx_ring "$iface" max)" || die "ethtool -g $iface failed"
	cur_rx="$(parse_rx_ring "$iface" current)" || die "ethtool -g $iface failed"
	if [[ "$cur_rx" -lt "$max_rx" ]]; then
		warn "RX ring $cur_rx < hardware max $max_rx"
		fail=1
	else
		log "RX ring OK ($cur_rx)"
	fi

	if irq_spread_ok "$iface"; then
		log "IRQ spread OK"
	else
		warn "IRQ affinities not spread across CPUs; run apply or enable irqbalance"
		fail=1
	fi

	[[ "$fail" -eq 0 ]] || exit 1
	log "verify: OK"
}

apply_tuning() {
	local iface
	iface="$(detect_interface)"
	iface_exists "$iface" || die "interface $iface not found"
	need_cmd ethtool

	log "tuning ingress NIC $iface (IRQ_STRATEGY=$IRQ_STRATEGY)"
	tune_rx_ring "$iface"
	tune_irqs "$iface"
	log "apply: done"
}

install_systemd() {
	local unit_src="$ROOT/deploy/edge/nic-tune.service"
	local env_example="$ROOT/deploy/edge/nic-tune.env.example"
	[[ -f "$unit_src" ]] || die "missing $unit_src"
	require_root

	install -d /etc/espx
	if [[ ! -f /etc/espx/edge-nic-tune.env ]]; then
		install -m 0644 "$env_example" /etc/espx/edge-nic-tune.env
		log "installed /etc/espx/edge-nic-tune.env (edit INGRESS_INTERFACE if needed)"
	fi
	install -m 0755 "$SCRIPTS/edge/edge_nic_tune.sh" /usr/local/bin/edge_nic_tune.sh
	install -m 0644 "$unit_src" /etc/systemd/system/espx-edge-nic-tune.service
	systemctl daemon-reload
	systemctl enable espx-edge-nic-tune.service
	log "installed espx-edge-nic-tune.service; run: systemctl start espx-edge-nic-tune"
}

case "$MODE" in
apply) apply_tuning ;;
verify) verify_tuning ;;
report) report_status ;;
install-systemd) install_systemd ;;
-h | --help)
	cat <<EOF
Usage: edge_nic_tune.sh <apply|verify|report|install-systemd>

  apply            set RX ring to hardware max; spread IRQs (default)
  verify           exit 1 if RX ring or IRQ spread is suboptimal
  report           print NIC ring and IRQ status
  install-systemd  install oneshot unit + /usr/local/bin/edge_nic_tune.sh

Environment: INGRESS_INTERFACE, DRY_RUN=1, IRQ_STRATEGY=auto|irqbalance|spread
EOF
	;;
*)
	die "unknown mode: $MODE (use apply|verify|report|install-systemd)"
	;;
esac
