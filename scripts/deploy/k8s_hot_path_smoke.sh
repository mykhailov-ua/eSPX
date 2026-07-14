#!/usr/bin/env bash
# Smoke test for k3s hot-path (trackers + OpenResty on hostNetwork).
set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config-espx}"

pass=0
fail=0

check() {
	local name="$1"
	shift
	if "$@"; then
		echo "PASS  $name"
		pass=$((pass + 1))
	else
		echo "FAIL  $name"
		fail=$((fail + 1))
	fi
}

http_code() {
	curl -sf -o /dev/null -w '%{http_code}' --connect-timeout 3 "$1" 2>/dev/null || echo "000"
}

NODE_IP="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true)"
if [[ -z "$NODE_IP" ]]; then
	echo "FAIL  cannot resolve node InternalIP"
	exit 1
fi

echo "eSPX k3s hot-path smoke (node=${NODE_IP})"

ready_count="$(kubectl get pods -n espx-edge -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null | grep -c true || true)"
total_count="$(kubectl get pods -n espx-edge --no-headers 2>/dev/null | wc -l)"
check "hot-path containers ready (${ready_count}/${total_count})" test "$ready_count" -eq "$total_count"

check "nginx-edge pod Ready" kubectl get pods -n espx-edge -l app.kubernetes.io/name=nginx-edge -o jsonpath='{.items[0].status.containerStatuses[0].ready}' | grep -q true

for port in 8181 8182 8183 8184; do
	check "tracker :${port}/health" test "$(http_code "http://${NODE_IP}:${port}/health")" = "200"
done

check "nginx edge :8180 listening" test "$(http_code "http://${NODE_IP}:8180/")" != "000"

echo "pass=${pass} fail=${fail}"
if [[ "$fail" -gt 0 ]]; then
	kubectl get pods -n espx-edge -o wide
	exit 1
fi
