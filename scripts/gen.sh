#!/usr/bin/env bash
# Code generation: sqlc (default), optional templ, buf, and bpf via flags.
# Usage: gen.sh [--proto] [--templ] [--bpf] [--all]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUN_PROTO=0
RUN_TEMPL=0
RUN_BPF=0

for arg in "$@"; do
	case "$arg" in
	--proto) RUN_PROTO=1 ;;
	--templ) RUN_TEMPL=1 ;;
	--bpf) RUN_BPF=1 ;;
	--all)
		RUN_PROTO=1
		RUN_TEMPL=1
		RUN_BPF=1
		;;
	esac
done

echo "gen: sqlc..."
go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate

if [[ "$RUN_TEMPL" -eq 1 ]]; then
	if command -v templ >/dev/null 2>&1; then
		echo "gen: templ..."
		templ generate
	else
		echo "gen: skip templ (binary not installed)" >&2
	fi
fi

if [[ "$RUN_PROTO" -eq 1 ]]; then
	echo "gen: buf..."
	go run github.com/bufbuild/buf/cmd/buf@latest generate --template api/buf.gen.yaml api
	# buf writes to api/gen/ (isolated); sync into internal/*/pb for go_package paths.
	rsync -a api/gen/ "$ROOT/"
fi

if [[ "$RUN_BPF" -eq 1 ]]; then
	if command -v clang >/dev/null 2>&1; then
		echo "gen: bpf2go..."
		( cd internal/edge/bpf && go generate ./... )
	else
		echo "gen: skip bpf (clang not installed; use committed edge_*.o or Docker bpf2go)" >&2
	fi
fi
