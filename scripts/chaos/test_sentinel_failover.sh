#!/usr/bin/env bash
# Sentinel failover chaos (CHAOS.md §6 scenario B): load during pause, promotion SLA, budget consistency.
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env}"
if [ ! -f "$ENV_FILE" ]; then
	cp .env.example "$ENV_FILE"
	sed -i 's/your_redis_password_here/sentinel_chaos_test/' "$ENV_FILE" 2>/dev/null || \
		sed -i '' 's/your_redis_password_here/sentinel_chaos_test/' "$ENV_FILE"
fi

# shellcheck disable=SC1090
set -a
. "$ENV_FILE"
set +a

REDIS_PASSWORD="${REDIS_PASSWORD:?REDIS_PASSWORD required in $ENV_FILE}"
SENTINEL_FAILOVER_MAX_MS="${SENTINEL_FAILOVER_MAX_MS:-15000}"
LOAD_WARMUP_SEC="${SENTINEL_LOAD_WARMUP_SEC:-3}"

now_ms() {
	date +%s%3N
}

wait_service_healthy() {
	local service="$1"
	local attempts=90
	while [ "$attempts" -gt 0 ]; do
		local state health
		state="$(docker compose ps "$service" --format '{{.State}}' 2>/dev/null | head -1)"
		health="$(docker compose ps "$service" --format '{{.Health}}' 2>/dev/null | head -1)"
		if [ "$state" = "running" ] && { [ "$health" = "healthy" ] || [ -z "$health" ]; }; then
			return 0
		fi
		sleep 2
		attempts=$((attempts - 1))
	done
	echo "test_sentinel_failover: timeout waiting for $service" >&2
	return 1
}

echo "test_sentinel_failover: starting Redis + Sentinel stack..."
docker compose up -d \
	redis-0 redis-1 redis-2 redis-3 \
	redis-0-replica redis-1-replica redis-2-replica redis-3-replica \
	sentinel-0 sentinel-1 sentinel-2

wait_service_healthy redis-0
wait_service_healthy sentinel-0

compose_network() {
	docker compose ps -q sentinel-0 | head -1 | xargs docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' | head -1
}

TEST_BIN="$ROOT/.cache/sentinel_chaos.test"
mkdir -p "$ROOT/.cache"

build_sentinel_test() {
	echo "test_sentinel_failover: compiling test binary..."
	CGO_ENABLED=0 go test -c -o "$TEST_BIN" ./internal/database/
}

run_sentinel_test() {
	local run_filter="$1"
	shift
	build_sentinel_test
	# shellcheck disable=SC2068
	docker run --rm --network "$NET" \
		-v "$TEST_BIN:/sentinel.test:ro" \
		-e SENTINEL_CHAOS=1 \
		-e REDIS_PASSWORD="$REDIS_PASSWORD" \
		-e REDIS_SENTINEL_ADDRS=sentinel-0:26379,sentinel-1:26379,sentinel-2:26379 \
		-e REDIS_MASTER_NAMES=espx-shard-0,espx-shard-1,espx-shard-2,espx-shard-3 \
		-e REDIS_ADDRS=redis-0:6379,redis-1:6379,redis-2:6379,redis-3:6379 \
		"$@" \
		debian:bookworm-slim \
		/sentinel.test -test.count=1 -test.timeout=5m -test.v -test.run "$run_filter"
}

start_load_worker() {
	build_sentinel_test
	docker rm -f espx_sentinel_load >/dev/null 2>&1 || true
	docker run -d --name espx_sentinel_load --network "$NET" \
		-v "$TEST_BIN:/sentinel.test:ro" \
		-e SENTINEL_CHAOS=1 \
		-e SENTINEL_LOAD_WORKER=1 \
		-e REDIS_PASSWORD="$REDIS_PASSWORD" \
		-e REDIS_SENTINEL_ADDRS=sentinel-0:26379,sentinel-1:26379,sentinel-2:26379 \
		-e REDIS_MASTER_NAMES=espx-shard-0,espx-shard-1,espx-shard-2,espx-shard-3 \
		-e REDIS_ADDRS=redis-0:6379,redis-1:6379,redis-2:6379,redis-3:6379 \
		debian:bookworm-slim \
		/sentinel.test -test.count=1 -test.timeout=3m -test.v -test.run TestSentinelFailoverLoadWorker
}

stop_load_worker() {
	if docker ps -q -f name=espx_sentinel_load | grep -q .; then
		docker stop -t 5 espx_sentinel_load >/dev/null
	fi
	if docker ps -aq -f name=espx_sentinel_load | grep -q .; then
		echo "test_sentinel_failover: load worker logs:"
		docker logs espx_sentinel_load 2>&1 | tail -30 || true
		docker rm -f espx_sentinel_load >/dev/null 2>&1 || true
	fi
}

NET="$(compose_network)"
if [ -z "$NET" ]; then
	echo "test_sentinel_failover: could not resolve compose network" >&2
	exit 1
fi

echo "test_sentinel_failover: pre-failover Sentinel connect..."
run_sentinel_test TestSentinelConnectAllShards

wait_sentinel_master_promoted() {
	local master_name="$1"
	local attempts=40
	while [ "$attempts" -gt 0 ]; do
		local host
		host="$(docker compose exec -T sentinel-0 redis-cli -p 26379 SENTINEL get-master-addr-by-name "$master_name" 2>/dev/null | head -1 | tr -d '\r')"
		if [ -n "$host" ] && [ "$host" != "redis-0" ]; then
			echo "test_sentinel_failover: $master_name promoted to $host"
			return 0
		fi
		sleep 2
		attempts=$((attempts - 1))
	done
	echo "test_sentinel_failover: forcing SENTINEL failover $master_name..."
	docker compose exec -T sentinel-0 redis-cli -p 26379 SENTINEL failover "$master_name" >/dev/null || true
	attempts=30
	while [ "$attempts" -gt 0 ]; do
		local host
		host="$(docker compose exec -T sentinel-0 redis-cli -p 26379 SENTINEL get-master-addr-by-name "$master_name" 2>/dev/null | head -1 | tr -d '\r')"
		if [ -n "$host" ] && [ "$host" != "redis-0" ]; then
			echo "test_sentinel_failover: $master_name promoted to $host"
			return 0
		fi
		sleep 2
		attempts=$((attempts - 1))
	done
	echo "test_sentinel_failover: sentinel did not promote $master_name" >&2
	return 1
}

trap stop_load_worker EXIT

echo "test_sentinel_failover: starting background track-like load (Redis budget GET via Sentinel)..."
start_load_worker
sleep "$LOAD_WARMUP_SEC"

echo "test_sentinel_failover: pausing redis-0 master (keeps DNS; stop removes hostname and breaks Sentinel)..."
FAILOVER_START_MS="$(now_ms)"
docker compose pause redis-0
wait_sentinel_master_promoted espx-shard-0
FAILOVER_END_MS="$(now_ms)"
FAILOVER_DURATION_MS=$((FAILOVER_END_MS - FAILOVER_START_MS))
echo "test_sentinel_failover: promotion duration_ms=$FAILOVER_DURATION_MS (max=${SENTINEL_FAILOVER_MAX_MS})"

if [ "$FAILOVER_DURATION_MS" -gt "$SENTINEL_FAILOVER_MAX_MS" ]; then
	echo "test_sentinel_failover: FAIL promotion took ${FAILOVER_DURATION_MS}ms > max ${SENTINEL_FAILOVER_MAX_MS}ms" >&2
	exit 1
fi

sleep 2
stop_load_worker
trap - EXIT

echo "test_sentinel_failover: post-failover verify (marker, budget, load stats, chaos_proof)..."
run_sentinel_test TestSentinelActiveFailoverVerify \
	-e SENTINEL_FAILOVER_DONE=1 \
	-e SENTINEL_FAILOVER_DURATION_MS="$FAILOVER_DURATION_MS" \
	-e SENTINEL_FAILOVER_MAX_MS="$SENTINEL_FAILOVER_MAX_MS"

echo "test_sentinel_failover: restoring redis-0..."
docker compose unpause redis-0
wait_service_healthy redis-0

echo "test_sentinel_failover: OK"
