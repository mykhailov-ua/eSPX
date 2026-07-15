#!/usr/bin/env bash
# R9.1: ASCII-only comments, no banned words, no unicode dashes in internal/pkg/cmd (non-test, non-generated).
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

exec go run ./scripts/ci/check_comments
