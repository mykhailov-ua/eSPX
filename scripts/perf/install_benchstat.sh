#!/usr/bin/env bash
# Ensures benchstat is on PATH for perf_gate.go.
set -euo pipefail

if command -v benchstat >/dev/null 2>&1; then
	exit 0
fi

go install golang.org/x/perf/cmd/benchstat@latest
