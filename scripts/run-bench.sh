#!/usr/bin/env bash
# Shared go test benchmark runner (nightly regression suites).
# Usage: run-bench.sh <bench_regex> <package...>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ $# -lt 2 ]]; then
	echo "usage: $0 <bench_regex> <package...>" >&2
	exit 2
fi

PATTERN="$1"
shift

export GOMAXPROCS=1
exec go test -run='^$' \
	-bench="$PATTERN" \
	-benchmem \
	-benchtime=200ms \
	-count=10 \
	-cpu=1 \
	"$@"
