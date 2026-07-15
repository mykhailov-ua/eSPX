#!/usr/bin/env bash
# Apply staging cold-path via Terraform (remote k3s + external data plane).
# Usage: k8s_staging_apply.sh [terraform-args...]
# Prereq: kubeconfig for staging cluster; terraform.tfvars with secrets (see terraform.tfvars.example).
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

log() { printf 'k8s-staging-apply: %s\n' "$*"; }

TF_DIR="deploy/terraform/envs/staging"

if [[ ! -f "$TF_DIR/terraform.tfvars" ]]; then
	log "missing $TF_DIR/terraform.tfvars — copy from terraform.tfvars.example"
	exit 1
fi

(
	cd "$TF_DIR"
	if [[ ! -d .terraform ]]; then
		log "terraform init"
		terraform init
	fi
	log "terraform apply"
	terraform apply -auto-approve "$@"
)

log "rollout status"
export KUBECONFIG="${KUBECONFIG:-$(grep -m1 '^kubeconfig_path' "$TF_DIR/terraform.tfvars" | sed 's/.*= *"\?\([^"]*\)"\?/\1/' | sed "s|^~|$HOME|")}"
kubectl rollout status deploy -n espx --timeout=180s || true
kubectl get pods -n espx
