#!/usr/bin/env bash
# Perf gate: baseline worktree benches + current HEAD + perf_gate.go report.
# Env: BASELINE_REF (default main), BASELINE_WORKTREE (default .cache/perf-baseline-worktree), OUTDIR (default repo root).
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
source "$SCRIPTS/_common/safe_paths.sh"
cd "$ROOT"

BASELINE_REF="${BASELINE_REF:-main}"
BASELINE_WORKTREE="${BASELINE_WORKTREE:-$ROOT/.cache/perf-baseline-worktree}"
OUTDIR="${OUTDIR:-$ROOT}"

if [[ "$BASELINE_WORKTREE" != /* ]]; then
	BASELINE_WORKTREE="$ROOT/$BASELINE_WORKTREE"
fi
BASELINE_WORKTREE="$(safe_worktree_dir "$BASELINE_WORKTREE")"

PR_BENCH="$OUTDIR/pr_bench.txt"
BASELINE_BENCH="$OUTDIR/baseline_bench.txt"
GATE_REPORT="$OUTDIR/gate_report.txt"
STRICT="${PERF_GATE_STRICT:-true}"

"$SCRIPTS/perf/install_benchstat.sh"

echo "perf-gate-run: generating sqlc on current tree..."
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
"$SCRIPTS/perf/perf_gate_bench.sh" >"$PR_BENCH"

if [[ "$STRICT" != "true" ]]; then
	echo "perf-gate-run: smoke mode — zero-alloc check only"
	go run "$SCRIPTS/perf/perf_gate.go" /dev/null "$PR_BENCH" | tee "$GATE_REPORT"
	exit 0
fi

echo "perf-gate-run: strict mode — baseline ref=$BASELINE_REF worktree=$BASELINE_WORKTREE"
git worktree prune || true
safe_rm_rf "$BASELINE_WORKTREE"
git worktree add --detach "$BASELINE_WORKTREE" "$BASELINE_REF"

(
	cd "$BASELINE_WORKTREE"
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
	"$SCRIPTS/perf/perf_gate_bench.sh" >"$BASELINE_BENCH"
)

git worktree remove --force "$BASELINE_WORKTREE" 2>/dev/null || safe_rm_rf "$BASELINE_WORKTREE"

go run "$SCRIPTS/perf/perf_gate.go" "$BASELINE_BENCH" "$PR_BENCH" >"$GATE_REPORT"
cat "$GATE_REPORT"
