#!/usr/bin/env bash
# Verify production env tuning (FILTER_TIMEOUT_MS, ENV). Usage: verify-prod-tuning.sh [env-file]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env}"
MAX_FILTER_TIMEOUT_MS=100

log() { printf 'verify-prod-tuning: %s\n' "$*"; }
die() { printf 'verify-prod-tuning: ERROR: %s\n' "$*" >&2; exit 1; }

if [[ ! -f "$ENV_FILE" ]]; then
	die "missing env file: $ENV_FILE"
fi

# shellcheck disable=SC1090
set -a
. "$ENV_FILE"
set +a

ENV="${ENV:-development}"
FILTER_TIMEOUT_MS="${FILTER_TIMEOUT_MS:-0}"

if [[ "$ENV" != "production" ]]; then
	log "ENV=$ENV (not production) — FILTER_TIMEOUT_MS check skipped"
	exit 0
fi

if [[ "$FILTER_TIMEOUT_MS" -le 0 ]]; then
	die "production requires explicit FILTER_TIMEOUT_MS (see .env.prod.example)"
fi

if [[ "$FILTER_TIMEOUT_MS" -gt "$MAX_FILTER_TIMEOUT_MS" ]]; then
	die "FILTER_TIMEOUT_MS=$FILTER_TIMEOUT_MS exceeds production ceiling ${MAX_FILTER_TIMEOUT_MS}ms"
fi

log "OK: ENV=production FILTER_TIMEOUT_MS=${FILTER_TIMEOUT_MS}ms (ceiling ${MAX_FILTER_TIMEOUT_MS}ms)"
