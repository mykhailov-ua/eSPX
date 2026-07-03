#!/bin/sh
# Starts edge-xdp then edge-bpf-sync after the blocklist BPF map is pinned.
set -eu

if [ -z "${INGRESS_INTERFACE:-}" ]; then
	echo "edge-xdp: INGRESS_INTERFACE is required" >&2
	exit 1
fi

/usr/local/bin/edge-xdp &
xdp_pid=$!

i=0
while [ "$i" -lt 50 ]; do
	if [ -e /sys/fs/bpf/espx/blocklist_v4 ]; then
		break
	fi
	i=$((i + 1))
	sleep 0.2
done

if [ ! -e /sys/fs/bpf/espx/blocklist_v4 ]; then
	echo "edge-xdp: timed out waiting for pinned blocklist_v4 map" >&2
	kill "$xdp_pid" 2>/dev/null || true
	wait "$xdp_pid" 2>/dev/null || true
	exit 1
fi

cleanup() {
	kill "$xdp_pid" 2>/dev/null || true
	wait "$xdp_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

exec /usr/local/bin/edge-bpf-sync
