#!/usr/bin/env bash
# Prepare .env for sentinel failover chaos (CI and local).
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

cp .env.example .env
sed -i 's/your_redis_password_here/sentinel_chaos_ci/' .env
