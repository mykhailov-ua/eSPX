#!/usr/bin/env bash
# Render prometheus-k3s.yml with the current node InternalIP for host-side Prometheus.
# Usage: render_prometheus_k3s.sh [output-path]
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config-espx}"
OUT="${1:-$ROOT/deploy/monitoring/prometheus-k3s.rendered.yml}"

NODE_IP="$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
sed "s/__NODE_IP__/${NODE_IP}/g" deploy/monitoring/prometheus-k3s.yml >"$OUT"
printf 'render-prometheus-k3s: wrote %s (node=%s)\n' "$OUT" "$NODE_IP"
