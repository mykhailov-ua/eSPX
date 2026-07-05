#!/usr/bin/env bash
# Validate buf.gen.yml and sqlc.yml output paths before codegen (no writes).
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
source "$SCRIPTS/_common/safe_paths.sh"

safe_validate_codegen_configs
echo "codegen configs OK"
