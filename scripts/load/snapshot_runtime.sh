#!/usr/bin/env bash
# Snapshot FD usage, socket stats, and optional strace syscall summary for hot-path containers.
# Usage: snapshot_runtime.sh <output_dir> [sample_sec]
set -euo pipefail

OUT="${1:?output dir required}"
SAMPLE_SEC="${2:-30}"
mkdir -p "$OUT"

log() { printf 'snapshot-runtime: %s\n' "$*"; }

timestamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

	snapshot_container() {
	local name=$1
	local ts
	ts="$(timestamp)"
	log "container=$name"
	{
		echo "# snapshot $ts container=$name"
		echo "## fd_count (inside container)"
		docker exec "$name" sh -c 'ls -1 /proc/1/fd 2>/dev/null | wc -l' 2>/dev/null || echo "na"
		echo "## limits (pid 1)"
		docker exec "$name" sh -c 'cat /proc/1/limits 2>/dev/null' 2>/dev/null || true
		echo "## status"
		docker exec "$name" sh -c 'awk "/VmRSS|Threads|voluntary_ctxt_switches|nonvoluntary_ctxt_switches/ {print}" /proc/1/status 2>/dev/null' 2>/dev/null || true
		echo "## ss_summary (host)"
		ss -s 2>/dev/null | head -8 || true
	} >"$OUT/${name}-${ts}.txt"

	local pid
	pid="$(docker inspect -f '{{.State.Pid}}' "$name" 2>/dev/null)" || return 0
	if command -v strace >/dev/null 2>&1 && [[ "${STRACE_SAMPLE:-1}" == "1" && "$pid" != "0" ]]; then
		log "strace sample ${SAMPLE_SEC}s on $name (pid=$pid)"
		timeout "$SAMPLE_SEC" strace -c -p "$pid" -f 2>"$OUT/${name}-strace-${ts}.txt" || true
	fi
}

log "writing to $OUT"
{
	echo "captured_at=$(timestamp)"
	echo "## ss_summary"
	ss -s 2>/dev/null || true
	echo "## ulimit"
	ulimit -n 2>/dev/null || true
} >"$OUT/host-$(timestamp).txt"

for c in espx-tracker-0-1 espx-tracker-1-1 espx-tracker-2-1 espx-tracker-3-1 espx-processor-1 espx-nginx-1; do
	snapshot_container "$c" || true
done

log "done"
