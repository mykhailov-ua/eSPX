#!/usr/bin/env bash
# Container chaos suite with minimum chaos_proof line count (testcontainers; requires Docker).
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

LOG="${CHAOS_LOG:-/tmp/espx-chaos.log}"
# M3 adds 6 licensing/subscription proofs; default floor raised from 46 → 52.
MIN_PROOFS="${CHAOS_MIN_PROOFS:-52}"
export BROKER_CHAOS_LAB=1

go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.28.0 generate
go fmt ./...

# Milestone 3 (licensing/subscriptions): internal/licensing + management licensing_chaos_test.go
# See scripts/chaos-drills/m3/README.md for fault catalog.
go test -count=1 -v -run 'Chaos' -timeout 20m \
	./tests/... \
	./internal/auth/... \
	./internal/ingestion/... \
	./internal/payment/... \
	./internal/billing/... \
	./internal/licensing/... \
	./internal/notifier/... \
	./internal/ivtdetector/... \
	./internal/fraudscoring/... \
	./pkg/broker/server/... \
	./internal/management/... \
	./internal/edge/perimeter/... \
	./internal/rtb/... \
	./internal/logevacuator/... \
	2>&1 | tee "$LOG"

PROOFS="$(grep -c 'chaos_proof fault=' "$LOG" || true)"
echo "chaos_proof lines: $PROOFS (min $MIN_PROOFS)"
test "$PROOFS" -ge "$MIN_PROOFS"
