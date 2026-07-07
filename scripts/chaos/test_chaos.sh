#!/usr/bin/env bash
# Container chaos suite with minimum chaos_proof line count (testcontainers; requires Docker).
set -euo pipefail

source "$(cd "$(dirname "$0")/../_common" && pwd)/paths.sh"
cd "$ROOT"

LOG="${CHAOS_LOG:-/tmp/espx-chaos.log}"
MIN_PROOFS="${CHAOS_MIN_PROOFS:-30}"
export BROKER_CHAOS_LAB=1

go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
go fmt ./...

go test -count=1 -v -run 'Chaos' -timeout 20m \
	./tests/... \
	./internal/auth/... \
	./internal/ads/... \
	./internal/payment/... \
	./pkg/broker/server/... \
	./internal/management/... \
	./internal/edge/perimeter/... \
	./internal/rtb/... \
	./internal/logevacuator/... \
	2>&1 | tee "$LOG"

PROOFS="$(grep -c 'chaos_proof fault=' "$LOG" || true)"
echo "chaos_proof lines: $PROOFS"
test "$PROOFS" -ge "$MIN_PROOFS"
