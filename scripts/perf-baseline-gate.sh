#!/usr/bin/env bash
# perf-baseline-gate.sh: benchstat regression vs cached baseline; seeds baseline on first run.
set -euo pipefail

if [[ $# -lt 2 ]]; then
	echo "usage: perf-baseline-gate.sh <baseline.txt> <current.txt>" >&2
	exit 2
fi

BASELINE="$1"
CURRENT="$2"

if [[ ! -s "$BASELINE" ]]; then
	echo "WARN: no baseline at $BASELINE; seeding from current run (first nightly or cache miss)"
	cp "$CURRENT" "$BASELINE"
	exit 0
fi

go run scripts/perf_gate.go --cpu-only "$BASELINE" "$CURRENT"
