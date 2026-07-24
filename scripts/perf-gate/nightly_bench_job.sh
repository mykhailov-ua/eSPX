#!/usr/bin/env bash
# Nightly bench regression: run suite, compare to cached baseline, update baseline file.
# Usage: nightly_bench_job.sh redis|broker
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
source "$SCRIPTS/lib/safe_paths.sh"
cd "$ROOT"

KIND="${1:-}"
case "$KIND" in
redis)
	BASELINE_DIR=".ci-baselines/redis"
	BENCH_PATTERN='BenchmarkUnifiedFilter_Check_RealRedis'
	BENCH_PKG='./internal/ingestion'
	OUT_BENCH="redis_lua_bench.txt"
	OUT_GATE="redis_lua_gate.txt"
	RUN_SQLC=1
	;;
broker)
	BASELINE_DIR=".ci-baselines/broker"
	BENCH_PATTERN='Benchmark(BrokerThroughput|SegmentWrite)'
	BENCH_PKG='./pkg/broker/server/... ./pkg/broker/log/... ./pkg/broker/protocol/'
	OUT_BENCH="broker_proto_bench.txt"
	OUT_GATE="broker_proto_gate.txt"
	RUN_SQLC=0
	;;
*)
	echo "usage: $0 redis|broker" >&2
	exit 2
	;;
esac

mkdir -p "$BASELINE_DIR"
"$SCRIPTS/perf/install_benchstat.sh"

if [[ "$RUN_SQLC" -eq 1 ]]; then
	safe_validate_sqlc_yml "$ROOT/sqlc.yaml"
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
fi

"$SCRIPTS/perf/run_bench.sh" "$BENCH_PATTERN" "$BENCH_PKG" | tee "$OUT_BENCH"
"$SCRIPTS/perf/perf_baseline_gate.sh" "$BASELINE_DIR/bench.txt" "$OUT_BENCH" | tee "$OUT_GATE"
cp "$OUT_BENCH" "$BASELINE_DIR/bench.txt"
