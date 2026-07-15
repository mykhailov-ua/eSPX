#!/usr/bin/env bash
# Smoke test for k3s cold-path stack after terraform apply.
# Usage: k8s_cold_path_smoke.sh
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"

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
	echo "FAIL  cannot resolve node InternalIP (is k3s up?)"
	exit 1
fi

echo "eSPX k3s cold-path smoke (node=${NODE_IP})"

not_ready="$(kubectl get pods -n espx --field-selector=status.phase!=Running -o name 2>/dev/null | wc -l)"
terminating="$(kubectl get pods -n espx --field-selector=status.phase=Running -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{"\n"}{end}' 2>/dev/null | wc -l)"
check "all pods Running" test "$((not_ready + terminating))" -eq 0

ready_count="$(kubectl get pods -n espx -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null | grep -c true || true)"
total_count="$(kubectl get pods -n espx --no-headers 2>/dev/null | wc -l)"
check "containers ready (${ready_count}/${total_count})" test "$ready_count" -eq "$total_count"

check "management /health (NodePort 30188)" test "$(http_code "http://${NODE_IP}:30188/health")" = "200"
check "processor /health (NodePort 30186)" test "$(http_code "http://${NODE_IP}:30186/health")" = "200"

echo "pass=${pass} fail=${fail}"
if [[ "$fail" -gt 0 ]]; then
	kubectl get pods -n espx
	exit 1
fi
