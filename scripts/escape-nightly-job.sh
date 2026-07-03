#!/usr/bin/env bash
# Escape analysis: compile report; optional regression gate vs baseline count file.
# Usage:
#   escape-nightly-job.sh [report.txt]                    # report only (local)
#   escape-nightly-job.sh <report.txt> <baseline.count>   # report + gate (CI)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REPORT="${1:-escape_report.txt}"
BASELINE_FILE="${2:-}"
PKG="${ESCAPE_PKG:-./internal/ads/...}"

go build -gcflags="-m" $PKG 2>&1 | tee "$REPORT"
COUNT="$(grep -c 'escapes to heap' "$REPORT" || true)"
echo "escape_to_heap_lines=$COUNT"
grep -E 'escapes to heap|escapes' "$REPORT" || true

if [[ -z "$BASELINE_FILE" ]]; then
	exit 0
fi

CURRENT="$COUNT"
if [[ -z "$CURRENT" ]]; then
	echo "FAIL: escape_to_heap_lines not found in $REPORT" >&2
	exit 1
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
	echo "WARN: no escape baseline; seeding count=$CURRENT"
	echo "$CURRENT" >"$BASELINE_FILE"
	exit 0
fi

BASELINE="$(tr -d '[:space:]' <"$BASELINE_FILE")"
if [[ "$CURRENT" -gt "$BASELINE" ]]; then
	echo "FAIL: escape_to_heap_lines regressed: baseline=$BASELINE current=$CURRENT" >&2
	exit 1
fi

echo "PASS: escape_to_heap_lines baseline=$BASELINE current=$CURRENT"
