#!/usr/bin/env bash
# Post-compose checks: dependency ports/migrations then HTTP/redis smoke.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

bash "$ROOT/scripts/check-deps.sh"
bash "$ROOT/scripts/smoke-local.sh"
