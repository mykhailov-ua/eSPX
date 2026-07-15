#!/usr/bin/env bash
# Full local cold-path bring-up: compose data plane, k3s image import, terraform apply.
# Usage: k8s_cold_path_up.sh [--skip-build] [--skip-infra]
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

SKIP_BUILD=0
SKIP_INFRA=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--skip-build) SKIP_BUILD=1; shift ;;
	--skip-infra) SKIP_INFRA=1; shift ;;
	*)
		echo "usage: $0 [--skip-build] [--skip-infra]" >&2
		exit 2
		;;
	esac
done

log() { printf 'k8s-cold-path-up: %s\n' "$*"; }

ensure_env() {
	if [[ ! -f .env ]]; then
		log "creating .env from .env.example"
		cp .env.example .env
	fi
	# compose healthcheck interpolation fails when these are empty.
	for kv in \
		HEALTHCHECK_INTERVAL=5s \
		HEALTHCHECK_TIMEOUT=5s \
		HEALTHCHECK_RETRIES=5 \
		REDIS_PASSWORD=your_redis_password_here; do
		key="${kv%%=*}"
		if ! grep -q "^${key}=" .env; then
			echo "$kv" >> .env
		fi
	done
}

sync_geoip() {
	local dst="/var/lib/espx/geoip"
	log "syncing geoip to ${dst}"
	if [[ -n "${SUDO_PASSWORD:-}" ]]; then
		echo "$SUDO_PASSWORD" | sudo -S mkdir -p "$dst"
		if compgen -G "deploy/geoip/*" >/dev/null; then
			echo "$SUDO_PASSWORD" | sudo -S cp -a deploy/geoip/. "$dst/" 2>/dev/null || true
		fi
	else
		sudo mkdir -p "$dst"
		if compgen -G "deploy/geoip/*" >/dev/null; then
			sudo cp -a deploy/geoip/. "$dst/" 2>/dev/null || true
		fi
	fi
}

ensure_env

if [[ "$SKIP_INFRA" -eq 0 ]]; then
	log "starting compose data plane (infra)"
	bash scripts/local-dev/dev_stack.sh infra
fi

log "applying cold-path database migrations"
export DB_DSN="$(grep -m1 '^DB_DSN=' .env | cut -d= -f2-)"
go run ./cmd/migrate-cold-path --only=notifier || log "notifier migrations skipped (DB may already be migrated)"
go run ./cmd/migrate-cold-path --only=ads,auth,billing || log "core migrations incomplete (existing DB drift; notifier startup also applies its schema)"

sync_geoip

if [[ "$SKIP_BUILD" -eq 0 ]]; then
	bash scripts/k8s/k8s_import_image.sh
fi

export TF_VAR_geoip_host_path="/var/lib/espx/geoip"
log "terraform apply"
(
	cd deploy/terraform/envs/local
	if [[ ! -d .terraform ]]; then
		terraform init
	fi
	terraform apply -auto-approve
)

log "waiting for cold-path pods"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config-espx}"
kubectl rollout restart deploy -n espx
kubectl rollout status deploy -n espx --timeout=180s || true

bash scripts/k8s/k8s_cold_path_smoke.sh
