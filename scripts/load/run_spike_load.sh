#!/usr/bin/env bash
# M6 spike load: 1×→10× ramp, 30s hold, ramp down. Writes var/load-test/<ts>/bottleneck-report.md.
#
# Usage:
#   bash scripts/load/run_spike_load.sh
#   BASE_RATE=500 SPIKE_MULT=10 bash scripts/load/run_spike_load.sh
#   CONSTRAINED=0 bash scripts/load/run_spike_load.sh
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

CONSTRAINED="${CONSTRAINED:-1}"
BASE_RATE="${BASE_RATE:-200}"
SPIKE_MULT="${SPIKE_MULT:-10}"
OUT="$ROOT/var/load-test/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"

COMPOSE=(docker compose)
if [[ "$CONSTRAINED" == "1" ]]; then
	COMPOSE+=( -f docker-compose.yml -f docker-compose.load-test.yml )
fi

log() { printf 'run-spike-load: %s\n' "$*"; }

log "ensuring stack (constrained=${CONSTRAINED})"
"${COMPOSE[@]}" up -d --remove-orphans db redis-0 redis-1 redis-2 redis-3 processor tracker-0 tracker-1 nginx prometheus grafana 2>&1 | tee "$OUT/compose.log"

bash "$ROOT/scripts/load/snapshot_runtime.sh" "$OUT/runtime-pre" || true

TRACKER_BASES="${TRACKER_BASES:-http://127.0.0.1:8181,http://127.0.0.1:8182}"
K6_LOG="$OUT/k6-spike.log"

log "spike profile base=${BASE_RATE} mult=${SPIKE_MULT} → peak=$((BASE_RATE * SPIKE_MULT)) RPS"
docker run --rm --network host \
	-v "$ROOT/scripts/load:/scripts" \
	-e TRACKER_BASES="$TRACKER_BASES" \
	-e BASE_RATE="$BASE_RATE" \
	-e SPIKE_MULT="$SPIKE_MULT" \
	-e RAMP_UP="${RAMP_UP:-10s}" \
	-e HOLD="${HOLD:-30s}" \
	-e RAMP_DOWN="${RAMP_DOWN:-10s}" \
	grafana/k6:latest run /scripts/k6_spike_traffic.js 2>&1 | tee "$K6_LOG"

bash "$ROOT/scripts/load/snapshot_runtime.sh" "$OUT/runtime-post" || true
bash "$ROOT/scripts/load/analyze_bottlenecks.sh" "$OUT"

log "done: $OUT/bottleneck-report.md"
