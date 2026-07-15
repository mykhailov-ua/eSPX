#!/usr/bin/env bash
# Compose game day: baseline checks, spike load, dirty-traffic soak.
#
# Usage:
#   bash scripts/load-test/run_game_day.sh check
#   bash scripts/load-test/run_game_day.sh spike
#   bash scripts/load-test/run_game_day.sh dirty
#   bash scripts/load-test/run_game_day.sh all
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

MODE="${1:-check}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$ROOT/var/load-test/game-day-$STAMP"
LOG="$OUT/game-day.log"
mkdir -p "$OUT"

log() { printf '%s\n' "$*" | tee -a "$LOG"; }

print_scenarios() {
	cat <<'EOF'
## Game day scenarios

| ID | Fault | Manual steps | CI analogue |
|----|-------|--------------|-------------|
| A | Shard 0 outage | `docker stop espx-redis-0-1`; verify shards 1–3 p99 <80ms | tests/chaos/shard0_outage_chaos_test.go |
| B | Sentinel failover | `docker kill` redis master under load | sentinel-chaos workflow |
| C | Processor↔PG partition | block tcp/5432 on processor | processor_pg_partition |
| D | Clock drift +3600s | shift tracker clock; TTC must pass | clock_drift_chaos_test.go |
| E | Staggered Redis+PG | stop Redis then PG; recover in order | manual only |
| F | ClickHouse slow | throttle CH writes | manual only |
| G | Combined UDP+Redis | tc netem on UDP + Redis ports | §7.3 combined profile |
| H | Full edge abuse | nginx rate limit + dirty traffic | k6_dirty_traffic.js |
| UDP severe | 20% loss, 10ms delay | `tc netem` on tracker UDP :8191 | udp_control_chaos_test.go |

Abort criteria (R1): control-cohort p99 >80 ms for 30 s, or AssertBudgetInvariant diff >1 micro.
Record: `chaos_proof fault=<name> ...` per scenario in game-day.log.
EOF
}

case "$MODE" in
check)
	log "=== game day check $STAMP ==="
	bash scripts/ci/check_deps.sh 2>&1 | tee -a "$LOG"
	bash scripts/redis-ops/verify_redis_topology.sh 2>&1 | tee -a "$LOG"
	docker compose ps 2>&1 | tee -a "$LOG"
	print_scenarios | tee -a "$LOG"
	log "log file: $LOG"
	;;
spike)
	log "=== spike load ==="
	bash scripts/load-test/run_spike_load.sh 2>&1 | tee -a "$LOG"
	;;
dirty)
	log "=== dirty traffic soak ==="
	bash scripts/load-test/run_dirty_load.sh smoke 2>&1 | tee -a "$LOG"
	;;
all)
	bash "$0" check
	bash "$0" dirty
	bash "$0" spike
	;;
*)
	printf 'usage: %s check|spike|dirty|all\n' "$0" >&2
	exit 1
	;;
esac
