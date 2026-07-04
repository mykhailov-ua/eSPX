#!/usr/bin/env bash
# Docker Compose stack helpers for local development.
# Usage: dev-stack.sh {infra|full|sentinel|down|status|build}
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CMD="${1:-status}"

INFRA=(db redis-0 redis-1 redis-2 redis-3 clickhouse)
FULL=(db redis-0 redis-1 redis-2 redis-3 clickhouse processor tracker-0 auth management payment billing notifier ivt-detector)
SENTINEL=(redis-0 redis-0-replica sentinel-0 sentinel-1 sentinel-2)

case "$CMD" in
infra | up-infra)
	docker compose up -d "${INFRA[@]}"
	;;
full | up-full)
	docker compose up -d "${FULL[@]}"
	;;
sentinel | up-sentinel)
	docker compose up -d "${SENTINEL[@]}"
	;;
down)
	docker compose down
	;;
status)
	docker compose ps
	;;
build)
	docker compose build
	;;
*)
	echo "usage: $0 {infra|full|sentinel|down|status|build}" >&2
	exit 2
	;;
esac
