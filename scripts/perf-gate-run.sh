#!/usr/bin/env bash
# Full PR perf gate: baseline worktree benches + current HEAD + perf_gate.go report.
# Env: BASELINE_REF (default main), BASELINE_WORKTREE (default ../baseline), OUTDIR (default repo root).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASELINE_REF="${BASELINE_REF:-main}"
BASELINE_WORKTREE="${BASELINE_WORKTREE:-../baseline}"
OUTDIR="${OUTDIR:-$ROOT}"

if [[ "$BASELINE_WORKTREE" != /* ]]; then
	BASELINE_WORKTREE="$ROOT/$BASELINE_WORKTREE"
fi

PR_BENCH="$OUTDIR/pr_bench.txt"
BASELINE_BENCH="$OUTDIR/baseline_bench.txt"
GATE_REPORT="$OUTDIR/gate_report.txt"

"$ROOT/scripts/install-benchstat.sh"

echo "perf-gate-run: generating sqlc on current tree..."
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
"$ROOT/scripts/perf-gate-bench.sh" >"$PR_BENCH"

echo "perf-gate-run: baseline ref=$BASELINE_REF worktree=$BASELINE_WORKTREE"
git worktree prune || true
rm -rf "$BASELINE_WORKTREE" || true
git worktree add --detach "$BASELINE_WORKTREE" "$BASELINE_REF"

(
	cd "$BASELINE_WORKTREE"
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
	"$ROOT/scripts/perf-gate-bench.sh" >"$BASELINE_BENCH"
)

git worktree remove --force "$BASELINE_WORKTREE" 2>/dev/null || rm -rf "$BASELINE_WORKTREE"

go run "$ROOT/scripts/perf_gate.go" "$BASELINE_BENCH" "$PR_BENCH" >"$GATE_REPORT"
cat "$GATE_REPORT"
