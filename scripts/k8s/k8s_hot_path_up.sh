#!/usr/bin/env bash
# Hot-path bring-up: tracker x4 + OpenResty on hostNetwork in k3s namespace espx-edge.
# Prereq: cold-path data plane (compose infra) and management NodePort 30188.
# Usage: k8s_hot_path_up.sh [--skip-build] [--skip-sync]
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

SKIP_BUILD=0
SKIP_SYNC=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--skip-build) SKIP_BUILD=1; shift ;;
	--skip-sync) SKIP_SYNC=1; shift ;;
	*)
		echo "usage: $0 [--skip-build] [--skip-sync]" >&2
		exit 2
		;;
	esac
done

log() { printf 'k8s-hot-path-up: %s\n' "$*"; }

sudo_cmd() {
	if [[ -n "${SUDO_PASSWORD:-}" ]]; then
		echo "$SUDO_PASSWORD" | sudo -S "$@"
	else
		sudo "$@"
	fi
}

render_tpl() {
	local src="$1" dst="$2"
	envsubst '${host_ip} ${geoip_host_path} ${redis_password}' <"$src" >"$dst"
}

sync_geoip() {
	local geoip_dst="/var/lib/espx/geoip"
	log "syncing geoip to ${geoip_dst}"
	sudo_cmd mkdir -p "$geoip_dst" /var/lib/espx/logs
	if compgen -G "deploy/geoip/*" >/dev/null; then
		sudo_cmd cp -a deploy/geoip/. "$geoip_dst/" 2>/dev/null || true
	fi
}

apply_nginx_configmaps() {
	log "creating nginx ConfigMaps from deploy/nginx/"
	kubectl create configmap nginx-edge-conf -n espx-edge \
		--from-file=nginx.conf=deploy/nginx/nginx.conf \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl create configmap nginx-edge-lua -n espx-edge \
		--from-file=deploy/nginx/lua \
		--dry-run=client -o yaml | kubectl apply -f -
}

stop_compose_hot_path() {
	# hostNetwork k8s pods bind the same ports as compose tracker/nginx services.
	log "stopping compose hot-path containers to avoid port conflicts"
	docker compose stop nginx tracker-0 tracker-1 tracker-2 tracker-3 2>/dev/null || true
}

ensure_infra() {
	if ! (echo >/dev/tcp/127.0.0.1/5430) 2>/dev/null; then
		log "compose data plane not listening on :5430 — starting infra"
		bash scripts/local-dev/dev_stack.sh infra
	fi
}

ensure_env() {
	if [[ ! -f .env ]]; then
		cp .env.example .env
	fi
}

ensure_env
ensure_infra
stop_compose_hot_path

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config-espx}"
if ! kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' >/dev/null 2>&1; then
	log "k3s not reachable (set KUBECONFIG?)"
	exit 1
fi

# hostNetwork pods share the node network namespace; compose data plane listens on loopback.
export host_ip="127.0.0.1"
export geoip_host_path="/var/lib/espx/geoip"
export redis_password="$(grep -m1 '^REDIS_PASSWORD=' .env | cut -d= -f2-)"
export redis_password="${redis_password:-your_redis_password_here}"

if [[ "$SKIP_SYNC" -eq 0 ]]; then
	sync_geoip
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
	bash scripts/k8s/k8s_import_image.sh
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

render_tpl deploy/k8s/hot-path/configmap-env.yaml.tpl "$tmpdir/configmap.yaml"
render_tpl deploy/k8s/hot-path/secret-env.yaml.tpl "$tmpdir/secret.yaml"
render_tpl deploy/k8s/hot-path/deployment-trackers.yaml.tpl "$tmpdir/trackers.yaml"

log "applying hot-path manifests"
kubectl apply -f deploy/k8s/hot-path/namespace.yaml
apply_nginx_configmaps
kubectl apply -f "$tmpdir/configmap.yaml"
kubectl apply -f "$tmpdir/secret.yaml"
kubectl apply -f "$tmpdir/trackers.yaml"
kubectl apply -f deploy/k8s/hot-path/daemonset-nginx.yaml

log "waiting for hot-path rollouts"
kubectl rollout restart deploy/tracker-0 deploy/tracker-1 deploy/tracker-2 deploy/tracker-3 -n espx-edge
kubectl rollout status deploy/tracker-0 -n espx-edge --timeout=120s
kubectl rollout status deploy/tracker-1 -n espx-edge --timeout=120s
kubectl rollout status deploy/tracker-2 -n espx-edge --timeout=120s
kubectl rollout status deploy/tracker-3 -n espx-edge --timeout=120s
kubectl rollout status daemonset/nginx-edge -n espx-edge --timeout=120s || true

bash scripts/k8s/k8s_hot_path_smoke.sh
