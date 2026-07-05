#!/usr/bin/env bash
# Fast local gate before push: lint, alloc gate, unit+integration tests, docker build.
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

bash "$SCRIPTS/codegen/validate_configs.sh"
make test-alloc-gate
make test
make build
