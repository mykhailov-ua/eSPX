#!/usr/bin/env bash
# M14-05: Shard-0 failure chaos drill — runs TestChaos_Shard0Outage and prints ops expectations.
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"

echo "=== M14 Shard-0 Failure Drill ==="
echo "Expected during redis-0 outage:"
echo "  - Shards 1-3 track continue (p99 within SLA)"
echo "  - Shard-0 campaigns return 503 shard_unavailable (or registry_stale for unknown IDs)"
echo "  - Management outbox UPDATE_SETTINGS stays PENDING until redis-0 recovers"
echo "  - AssertBudgetInvariant holds on surviving shards"
echo ""

go test -count=1 -v -run 'TestChaos_Shard0Outage' -timeout 15m ./tests/chaos/ 2>&1 | tee /tmp/espx-m14-shard0.log

if grep -q 'chaos_proof fault=shard_0_outage' /tmp/espx-m14-shard0.log || \
   grep -q 'chaos_proof fault=shard0_survival_shards_1_3' /tmp/espx-m14-shard0.log; then
  echo "OK: shard-0 survival chaos_proof present"
  exit 0
fi
echo "FAIL: missing chaos_proof for shard-0 survival"
exit 1
