#!/usr/bin/env bash
# Code generation: sqlc (default), optional templ, buf, and bpf via flags.
# Usage: gen.sh [--proto] [--templ] [--bpf] [--all]
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
source "$SCRIPTS/_common/safe_paths.sh"
cd "$ROOT"

safe_validate_codegen_configs

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
	# Staging only: api/gen/. Never set buf out to . or repo root.
	safe_rm_rf "$ROOT/api/gen"
	mkdir -p "$ROOT/api/gen"
	# buf out: gen is relative to api/; running from repo root would write to $ROOT/gen.
	( cd "$ROOT/api" && go run github.com/bufbuild/buf/cmd/buf@latest generate --template buf.gen.yml . )
	safe_sync_proto_gen
fi

if [[ "$RUN_BPF" -eq 1 ]]; then
	if command -v clang >/dev/null 2>&1; then
		echo "gen: bpf2go..."
		( cd internal/edge/bpf && go generate ./... )
	else
		echo "gen: skip bpf (clang not installed; use committed edge_*.o or Docker bpf2go)" >&2
	fi
fi
