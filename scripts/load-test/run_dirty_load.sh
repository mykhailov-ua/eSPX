#!/usr/bin/env bash
# Run k6 dirty-traffic load test with constrained Docker/GOMAXPROCS profile for laptops.
#
# Usage:
#   bash scripts/load-test/run_dirty_load.sh              # constrained 2k RPS × 5m (default)
#   bash scripts/load-test/run_dirty_load.sh smoke        # 500 RPS × 2m
#   CONSTRAINED=0 bash scripts/load-test/run_dirty_load.sh full  # full compose, 5k RPS
#   PREPARE=1 bash scripts/load-test/run_dirty_load.sh
#
# Env:
#   CONSTRAINED=1  use docker-compose.load-test.yaml (default)
#   RATE=2000 DURATION=5m GOMAXPROCS=4 (k6) PREALLOC_VUS=200 MAX_VUS=800
#
# Outputs: var/load-test/<timestamp>/k6.log, bottleneck-report.md
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

MODE="${1:-full}"
CONSTRAINED="${CONSTRAINED:-1}"
RATE="${RATE:-}"
DURATION="${DURATION:-5m}"
PREPARE="${PREPARE:-0}"
K6_DOCKER="${K6_DOCKER:-1}"
K6_GOMAXPROCS="${GOMAXPROCS:-4}"
OUT="$ROOT/var/load-test/$(date -u +%Y%m%dT%H%M%SZ)"
K6_LOG="$OUT/k6.log"
mkdir -p "$OUT"

COMPOSE=(docker compose)
if [[ "$CONSTRAINED" == "1" ]]; then
	COMPOSE+=( -f docker-compose.yaml -f docker-compose.load-test.yaml )
fi

LOAD_SERVICES=(
	db redis-0 redis-1 redis-2 redis-3 redis-4 redis-5 clickhouse
	processor tracker-0 tracker-1 nginx prometheus grafana
)

log() { printf 'run-dirty-load: %s\n' "$*"; }
die() { printf 'run-dirty-load: ERROR: %s\n' "$*" >&2; exit 1; }

case "$MODE" in
smoke)
	RATE="${RATE:-500}"
	DURATION=2m
	log "smoke mode: ${RATE} RPS × ${DURATION} (constrained=${CONSTRAINED})"
	;;
full)
	RATE="${RATE:-$([[ "$CONSTRAINED" == "1" ]] && echo 2000 || echo 5000)}"
	log "full mode: ${RATE} RPS × ${DURATION} (constrained=${CONSTRAINED})"
	;;
*)
	die "unknown mode: $MODE (use smoke|full)"
	;;
esac

if [[ "$CONSTRAINED" == "1" ]]; then
	TRACKER_BASES="${TRACKER_BASES:-http://127.0.0.1:8181,http://127.0.0.1:8182}"
	PREALLOC_VUS="${PREALLOC_VUS:-200}"
	MAX_VUS="${MAX_VUS:-800}"
	OVERSIZE_BYTES="${OVERSIZE_BYTES:-65536}"
else
	TRACKER_BASES="${TRACKER_BASES:-http://127.0.0.1:8181,http://127.0.0.1:8182,http://127.0.0.1:8183,http://127.0.0.1:8184}"
	PREALLOC_VUS="${PREALLOC_VUS:-500}"
	MAX_VUS="${MAX_VUS:-2000}"
	OVERSIZE_BYTES="${OVERSIZE_BYTES:-2097152}"
fi

compose_up() {
	"${COMPOSE[@]}" up -d --remove-orphans "$@" 2>&1 | tee -a "$OUT/compose.log"
}

if [[ "$PREPARE" == "1" ]]; then
	log "preparing stack (prepare_test.sh)"
	export RATE_LIMIT_PER_MIN=1000000
	bash scripts/ci/prepare_test.sh
else
	log "ensuring constrained stack is up"
	compose_up "${LOAD_SERVICES[@]}"
	# Extra replicas are profile-disabled; stop if left over from a prior full run.
	"${COMPOSE[@]}" stop tracker-2 tracker-3 2>/dev/null || true

	log "waiting for tracker health"
	for port in 8181 8182; do
		for _ in $(seq 1 120); do
			if curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
				break
			fi
			sleep 1
		done
	done
fi

{
	echo "constrained=$CONSTRAINED"
	echo "rate=$RATE duration=$DURATION"
	echo "trackers=$TRACKER_BASES"
	echo "gomaxprocs_k6=$K6_GOMAXPROCS prealloc_vus=$PREALLOC_VUS max_vus=$MAX_VUS"
} >"$OUT/profile.txt"

log "Grafana:    http://127.0.0.1:3100"
log "Prometheus: http://127.0.0.1:9190"
log "Output dir: $OUT"

if ! curl -sf http://127.0.0.1:9190/-/ready >/dev/null 2>&1; then
	log "WARN: Prometheus not ready — metrics analysis may be incomplete"
fi

SNAP_PID=""
if [[ "${SNAPSHOT_RUNTIME:-1}" == "1" ]]; then
	bash scripts/load-test/snapshot_runtime.sh "$OUT/runtime-pre" 5 &
	SNAP_PID=$!
fi

run_k6() {
	local k6_img="grafana/k6:latest"
	local script="$ROOT/scripts/load-test/k6_dirty_traffic.js"
	if [[ "$K6_DOCKER" == "1" ]]; then
		docker run --rm --network host \
			-e GOMAXPROCS="$K6_GOMAXPROCS" \
			-v "$script:/scripts/k6_dirty_traffic.js:ro" \
			-e RATE="$RATE" \
			-e DURATION="$DURATION" \
			-e TRACKER_BASES="$TRACKER_BASES" \
			-e EDGE_URL="${EDGE_URL:-http://127.0.0.1:8180}" \
			-e PREALLOC_VUS="$PREALLOC_VUS" \
			-e MAX_VUS="$MAX_VUS" \
			-e OVERSIZE_BYTES="$OVERSIZE_BYTES" \
			"$k6_img" run /scripts/k6_dirty_traffic.js
	else
		GOMAXPROCS="$K6_GOMAXPROCS" k6 run \
			-e RATE="$RATE" \
			-e DURATION="$DURATION" \
			-e TRACKER_BASES="$TRACKER_BASES" \
			-e EDGE_URL="${EDGE_URL:-http://127.0.0.1:8180}" \
			-e PREALLOC_VUS="$PREALLOC_VUS" \
			-e MAX_VUS="$MAX_VUS" \
			-e OVERSIZE_BYTES="$OVERSIZE_BYTES" \
			"$script"
	fi
}

log "starting k6 dirty traffic: ${RATE} req/s for ${DURATION}"
START_TS=$(date -u +%s)
if run_k6 2>&1 | tee "$K6_LOG"; then
	log "k6 finished OK"
else
	log "k6 exited non-zero (see $K6_LOG)"
fi
END_TS=$(date -u +%s)
log "k6 wall time: $((END_TS - START_TS))s"

if [[ -n "$SNAP_PID" ]]; then
	wait "$SNAP_PID" 2>/dev/null || true
fi
bash scripts/load-test/snapshot_runtime.sh "$OUT/runtime-post" 10

log "running bottleneck analysis"
bash scripts/load-test/analyze_bottlenecks.sh "$OUT"

log "done — artifacts in $OUT"
log "k6 summary (tail):"
tail -45 "$K6_LOG" || true
