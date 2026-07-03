#!/usr/bin/env bash
# Broker durability lab chaos: slow fsync, page cache, CPU throttle, Redis outage, optional Sentinel stack.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOG="${BROKER_CHAOS_LAB_LOG:-/tmp/espx-broker-chaos-lab.log}"
export BROKER_CHAOS_LAB=1

echo "=== Broker chaos lab (durability and coordination) ==="
if command -v stress-ng >/dev/null 2>&1; then
	echo "stress-ng: $(stress-ng --version 2>&1 | head -1)"
else
	echo "stress-ng not installed; page cache test will skip"
fi
if command -v cpulimit >/dev/null 2>&1; then
	echo "cpulimit: available"
else
	echo "cpulimit not installed; CPU throttle test will skip"
fi
go test -count=1 -v -run 'TestChaos_(SlowFsync|PageCache|CPUThrottle|RedisOutage|RedisSentinel)' -timeout 25m ./pkg/broker/server/... 2>&1 | tee "$LOG"

PROOFS="$(grep -c 'chaos_proof fault=' "$LOG" || true)"
echo "chaos_proof lines: $PROOFS"
test "$PROOFS" -ge 2

if command -v docker >/dev/null 2>&1 && [ "${BROKER_CHAOS_SKIP_SENTINEL:-0}" != "1" ]; then
	echo "=== Sentinel coordination lab (optional) ==="
	COMPOSE_FILE="deploy/broker-lab/docker-compose.sentinel.yml"
	docker compose -f "$COMPOSE_FILE" up -d
	trap 'docker compose -f "$COMPOSE_FILE" down' EXIT

	echo "waiting for sentinel to monitor master..."
	sleep 8

	export BROKER_CHAOS_SENTINEL=1
	export BROKER_REDIS_SENTINEL_MASTER=broker-coord
	export BROKER_REDIS_SENTINEL_ADDRS=127.0.0.1:26390
	export BROKER_REDIS_URL=redis://127.0.0.1:6380/0
	export BROKER_CHAOS_SENTINEL_STOP_CONTAINER=broker-lab-redis-master-1

	go test -count=1 -v -run 'TestChaos_RedisSentinelFailover' -timeout 10m ./pkg/broker/server/... 2>&1 | tee -a "$LOG"

	SENTINEL_PROOFS="$(grep -c 'chaos_proof fault=redis_sentinel_failover' "$LOG" || true)"
	test "$SENTINEL_PROOFS" -ge 1
fi

echo "Broker chaos lab complete. Log: $LOG"
