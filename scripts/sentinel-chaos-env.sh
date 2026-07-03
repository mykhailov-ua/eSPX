#!/usr/bin/env bash
# Prepare .env for sentinel failover chaos (CI and local).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

cp .env.example .env
sed -i 's/your_redis_password_here/sentinel_chaos_ci/' .env
