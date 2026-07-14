#!/usr/bin/env bash
# Install single-node k3s for eSPX cold-path workloads.
# Traefik is disabled: public ingress is OpenResty on hostNetwork edge nodes.
# Usage: install_k3s.sh [--kubeconfig PATH]
# Prereq: passwordless sudo, or export SUDO_PASSWORD before running.
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"

KUBECONFIG_DST="${HOME}/.kube/config-espx"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--kubeconfig)
		KUBECONFIG_DST="$2"
		shift 2
		;;
	*)
		echo "usage: $0 [--kubeconfig PATH]" >&2
		exit 2
		;;
	esac
done

log() { printf 'install-k3s: %s\n' "$*"; }
sudo_cmd() {
	if [[ -n "${SUDO_PASSWORD:-}" ]]; then
		echo "$SUDO_PASSWORD" | sudo -S "$@"
	else
		sudo "$@"
	fi
}

if command -v k3s >/dev/null 2>&1 && sudo_cmd k3s kubectl get nodes >/dev/null 2>&1; then
	log "k3s already running"
else
	log "installing k3s (traefik disabled)"
	installer="$(mktemp)"
	trap 'rm -f "$installer"' EXIT
	curl -sfL https://get.k3s.io -o "$installer"
	# write-kubeconfig-mode 644 lets the deploy user copy k3s.yaml without root shell.
	sudo_cmd env INSTALL_K3S_EXEC="--disable traefik --write-kubeconfig-mode 644" sh "$installer"
fi

mkdir -p "$(dirname "$KUBECONFIG_DST")"
sudo_cmd cp /etc/rancher/k3s/k3s.yaml "$KUBECONFIG_DST"
sudo_cmd chown "$(id -u):$(id -g)" "$KUBECONFIG_DST"
chmod 600 "$KUBECONFIG_DST"

export KUBECONFIG="$KUBECONFIG_DST"
if command -v k3s >/dev/null 2>&1; then
	sudo_cmd k3s kubectl wait --for=condition=Ready node --all --timeout=120s
fi

log "kubeconfig: $KUBECONFIG_DST"
log "nodes:"
k3s kubectl get nodes -o wide 2>/dev/null || kubectl --kubeconfig="$KUBECONFIG_DST" get nodes -o wide
