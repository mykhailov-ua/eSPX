#!/usr/bin/env bash
# Full test suite (no -short, skip Chaos tag). Used by CI and make test-full.
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

make gen
bash "$SCRIPTS/ci/check_comments.sh"
make lint
go test ./... -count=1 -timeout 30m -skip Chaos
