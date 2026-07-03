#!/usr/bin/env bash
# Playbook A (CHAOS.md §6): shard 0 outage — track isolation, outbox PENDING, recovery.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

go test -count=1 -v -run TestChaos_Shard0Outage -timeout 15m ./tests/...
