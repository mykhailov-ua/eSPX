#!/usr/bin/env bash
# Post-deploy smoke for local docker-compose / host-network stack.
# Catches broken health endpoints and Redis persistence before manual QA or load tests.
#
# Skip policy: when a dependency port is not listening, that check is SKIP (not FAIL).
# Run against a full stack (dev-stack.sh full) for pass=N fail=0 with no skips on core paths.
#
# Usage: ./scripts/smoke-local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

TRACKER_PORT="${SERVER_PORT:-8181}"
PROCESSOR_PORT="${PROCESSOR_PORT:-8186}"
EDGE_PORT="${EDGE_PORT:-8180}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_SHARD_PORTS=(6479 6480 6481 6482)

pass=0
fail=0
skip=0

# Records pass/fail for a named smoke check.
check() {
  local name="$1"
  shift
  if "$@"; then
    echo "PASS  $name"
    pass=$((pass + 1))
  else
    echo "FAIL  $name"
    fail=$((fail + 1))
  fi
}

skip_check() {
  echo "SKIP  $1"
  skip=$((skip + 1))
}

# Returns 0 when host:port accepts TCP (stack may be down).
port_open() {
  local port=$1
  nc -z 127.0.0.1 "$port" >/dev/null 2>&1
}

# Fetches HTTP status code; prints 000 on connection failure.
http_code() {
  curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 5 "$1" 2>/dev/null || echo "000"
}

# Fetches HTTP response body; empty string on failure.
http_body() {
  curl -sf --connect-timeout 2 --max-time 5 "$1" 2>/dev/null || true
}

echo "eSPX local smoke"

if port_open "$TRACKER_PORT"; then
  code="$(http_code "http://127.0.0.1:${TRACKER_PORT}/health")"
  check "tracker /health (${TRACKER_PORT}) HTTP 200" test "$code" = "200"
  body="$(http_body "http://127.0.0.1:${TRACKER_PORT}/health")"
  if [[ "$code" == "200" && "$body" == OK* ]]; then
    check "tracker /health not DEGRADED" true
  elif [[ "$code" == "503" && "$body" == DEGRADED* ]]; then
    check "tracker /health not DEGRADED" false
  else
    skip_check "tracker DEGRADED probe (unexpected health body: code=${code})"
  fi
else
  skip_check "tracker /health (:${TRACKER_PORT} not listening)"
  skip_check "tracker DEGRADED probe (tracker down)"
fi

if port_open "$PROCESSOR_PORT"; then
  check "processor /health (${PROCESSOR_PORT})" test "$(http_code "http://127.0.0.1:${PROCESSOR_PORT}/health")" = "200"
else
  skip_check "processor /health (:${PROCESSOR_PORT} not listening)"
fi

if port_open "$EDGE_PORT"; then
  edge_code="$(http_code "http://127.0.0.1:${EDGE_PORT}/metrics/edge")"
  check "edge /metrics/edge (:${EDGE_PORT})" test "$edge_code" = "200"
  edge_body="$(http_body "http://127.0.0.1:${EDGE_PORT}/metrics/edge")"
  if [[ -n "$edge_body" ]]; then
    check "edge metrics expose phase1 counter" grep -q 'espx_edge_phase1_pass_total' <<<"$edge_body"
  else
    skip_check "edge metrics body (empty response)"
  fi
else
  skip_check "edge /metrics/edge (:${EDGE_PORT} not listening)"
  skip_check "edge metrics body (nginx down)"
fi

if command -v redis-cli >/dev/null 2>&1 && [[ -n "${REDIS_PASSWORD}" ]]; then
  redis_up=0
  for port in "${REDIS_SHARD_PORTS[@]}"; do
    if ! port_open "$port"; then
      skip_check "redis shard :${port} PING (not listening)"
      continue
    fi
    redis_up=1
    check "redis shard :${port} PING" redis-cli -p "${port}" -a "${REDIS_PASSWORD}" --no-auth-warning ping 2>/dev/null | grep -q PONG
  done
  if [[ "$redis_up" -eq 1 ]]; then
    check "redis shard :6479 AOF enabled" redis-cli -p 6479 -a "${REDIS_PASSWORD}" --no-auth-warning INFO persistence 2>/dev/null | grep -q 'aof_enabled:1'
  else
    skip_check "redis AOF check (no shard ports open)"
  fi
else
  skip_check "redis checks (redis-cli or REDIS_PASSWORD missing)"
fi

if [[ -f deploy/geoip/GeoLite2-Country.mmdb ]]; then
  echo "INFO  GeoLite2 mmdb present"
else
  echo "WARN  deploy/geoip/GeoLite2-Country.mmdb missing (OK when ENV=development)"
fi

echo "pass=${pass} fail=${fail} skip=${skip}"
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
