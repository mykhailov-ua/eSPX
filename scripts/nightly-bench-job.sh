#!/usr/bin/env bash
# Nightly bench regression: run suite, compare to cached baseline, update baseline file.
# Usage: nightly-bench-job.sh redis|broker
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

KIND="${1:-}"
case "$KIND" in
redis)
	BASELINE_DIR=".ci-baselines/redis"
	BENCH_PATTERN='BenchmarkUnifiedFilter_Check_RealRedis'
	BENCH_PKG='./internal/ads'
	OUT_BENCH="redis_lua_bench.txt"
	OUT_GATE="redis_lua_gate.txt"
	RUN_SQLC=1
	;;
broker)
	BASELINE_DIR=".ci-baselines/broker"
	BENCH_PATTERN='Benchmark'
	BENCH_PKG='./pkg/broker/protocol/'
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
"$ROOT/scripts/install-benchstat.sh"

if [[ "$RUN_SQLC" -eq 1 ]]; then
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
fi

"$ROOT/scripts/run-bench.sh" "$BENCH_PATTERN" "$BENCH_PKG" | tee "$OUT_BENCH"
"$ROOT/scripts/perf-baseline-gate.sh" "$BASELINE_DIR/bench.txt" "$OUT_BENCH" | tee "$OUT_GATE"
cp "$OUT_BENCH" "$BASELINE_DIR/bench.txt"
