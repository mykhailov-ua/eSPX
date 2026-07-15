#!/usr/bin/env bash
# Post-compose checks: dependency ports/migrations then HTTP/redis smoke.
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

bash "$SCRIPTS/ci/check_deps.sh"
bash "$SCRIPTS/dev/smoke_local.sh"
