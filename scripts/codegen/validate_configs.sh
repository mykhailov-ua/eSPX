#!/usr/bin/env bash
# Validate buf.gen.yaml and sqlc.yaml output paths before codegen (no writes).
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
source "$SCRIPTS/lib/safe_paths.sh"

safe_validate_codegen_configs
echo "codegen configs OK"
