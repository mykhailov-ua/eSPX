#!/usr/bin/env bash
# Broker produce/fetch smoke under sustained RPS (M6-16).
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

DURATION="${BROKER_LOAD_DURATION:-10s}"
RPS="${BROKER_LOAD_RPS:-2000}"

echo "broker load: duration=${DURATION} target_rps=${RPS}"

go test -run='^$' -bench='BenchmarkBrokerThroughput/Produce-Sequential' \
	-benchtime="${DURATION}" -count=1 ./pkg/broker/server/ \
	| tee /tmp/espx_broker_load.txt

echo "broker load complete; see /tmp/espx_broker_load.txt"
