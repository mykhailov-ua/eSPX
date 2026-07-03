#!/usr/bin/env bash
# perf-gate-bench.sh: hot-path benchmarks for CI perf gate (proto accept/reject/infra + micro benches).
# Excludes legacy JSON handler and ExtraRepeated (allocating repeated-field parse).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BENCH_PATTERN='Benchmark(AdsPacketHandlerProto$$|AdsPacketHandlerProto_NoExtra|AdsPacketHandlerProto_ExtraBytes|HotPath_|TrackRequest_ParseJSON|CompositeRouting_Protobuf|Auction$$)'

export GOMAXPROCS=1
exec go test -run='^$' \
	-bench="$BENCH_PATTERN" \
	-benchmem \
	-benchtime=200ms \
	-count=10 \
	-cpu=1 \
	./internal/ads ./internal/rtb
