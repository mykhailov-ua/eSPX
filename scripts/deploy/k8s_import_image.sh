#!/usr/bin/env bash
# Build ad-event-processor image and import into k3s containerd.
# k3s does not see docker daemon images; import is required before cold-path pods start.
# Usage: k8s_import_image.sh [image-tag]
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

IMAGE_TAG="${1:-ad-event-processor:latest}"

log() { printf 'k8s-import-image: %s\n' "$*"; }
sudo_cmd() {
	if [[ -n "${SUDO_PASSWORD:-}" ]]; then
		echo "$SUDO_PASSWORD" | sudo -S "$@"
	else
		sudo "$@"
	fi
}

log "building ${IMAGE_TAG}"
docker build -t "$IMAGE_TAG" .

log "importing into k3s containerd"
tar="$(mktemp)"
trap 'rm -f "$tar"' EXIT
docker save "$IMAGE_TAG" -o "$tar"
sudo_cmd k3s ctr images import "$tar"

log "done: ${IMAGE_TAG}"
