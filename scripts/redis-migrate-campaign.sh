#!/usr/bin/env bash
# Migrate campaign Redis keys from source shard to target shard (StaticSlot realignment / shard loss recovery).
# Usage: redis-migrate-campaign.sh <campaign_uuid> [source_shard] [target_shard]
# When source/target omitted, source is computed via StaticSlot (N=6); target defaults to source (no-op guard).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
SHARD_COUNT="${REDIS_SHARD_COUNT:-4}"

if [ $# -lt 1 ]; then
	echo "usage: $0 <campaign_uuid> [source_shard] [target_shard]" >&2
	exit 1
fi

CAMPAIGN_ID="$1"
SOURCE_SHARD="${2:-}"
TARGET_SHARD="${3:-}"

if [ ! -f "$ENV_FILE" ]; then
	echo "redis_migrate_campaign: missing $ENV_FILE" >&2
	exit 1
fi

# shellcheck disable=SC1090
set -a
. "$ENV_FILE"
set +a

REDIS_PASSWORD="${REDIS_PASSWORD:?REDIS_PASSWORD required}"
REDIS_ADDRS="${REDIS_ADDRS:?REDIS_ADDRS required}"

IFS=',' read -r -a ADDRS <<< "$(echo "$REDIS_ADDRS" | tr -d ' ')"
if [ "${#ADDRS[@]}" -ne "$SHARD_COUNT" ]; then
	echo "redis_migrate_campaign: expected $SHARD_COUNT shards in REDIS_ADDRS, got ${#ADDRS[@]}" >&2
	exit 1
fi

if [ -z "$SOURCE_SHARD" ]; then
	SOURCE_SHARD="$(cd "$ROOT" && go run ./scripts/campaign_shard.go "$CAMPAIGN_ID" "$SHARD_COUNT")"
fi

if [ -z "$TARGET_SHARD" ]; then
	TARGET_SHARD="$SOURCE_SHARD"
fi

if [ "$SOURCE_SHARD" = "$TARGET_SHARD" ]; then
	echo "redis_migrate_campaign: source and target shard are both $SOURCE_SHARD (nothing to do)" >&2
	exit 0
fi

if [ "$SOURCE_SHARD" -lt 0 ] || [ "$SOURCE_SHARD" -ge "$SHARD_COUNT" ] || [ "$TARGET_SHARD" -lt 0 ] || [ "$TARGET_SHARD" -ge "$SHARD_COUNT" ]; then
	echo "redis_migrate_campaign: shard index out of range [0,$((SHARD_COUNT - 1))]" >&2
	exit 1
fi

SRC="${ADDRS[$SOURCE_SHARD]}"
DST="${ADDRS[$TARGET_SHARD]}"
SRC_HOST="${SRC%%:*}"
SRC_PORT="${SRC##*:}"
DST_HOST="${DST%%:*}"
DST_PORT="${DST##*:}"

redis_src() {
	redis-cli -h "$SRC_HOST" -p "$SRC_PORT" -a "$REDIS_PASSWORD" --no-auth-warning "$@"
}

redis_dst() {
	redis-cli -h "$DST_HOST" -p "$DST_PORT" -a "$REDIS_PASSWORD" --no-auth-warning "$@"
}

migrate_key() {
	local key="$1"
	if ! redis_src EXISTS "$key" | grep -q '^1$'; then
		echo "  skip missing key: $key"
		return 0
	fi
	local ttl
	ttl="$(redis_src TTL "$key" | tr -d '\r')"
	if [ "$ttl" -lt 0 ]; then
		ttl=0
	fi
	local tmp
	tmp="$(mktemp)"
	redis_src --raw DUMP "$key" >"$tmp"
	redis_dst -x RESTORE "$key" "$ttl" REPLACE <"$tmp"
	rm -f "$tmp"
	echo "  migrated: $key (ttl=$ttl)"
}

echo "redis_migrate_campaign: $CAMPAIGN_ID shard $SOURCE_SHARD -> $TARGET_SHARD"
echo "  source=$SRC target=$DST"

migrate_key "budget:campaign:${CAMPAIGN_ID}"
migrate_key "budget:quota:${CAMPAIGN_ID}"
migrate_key "budget:refill_lock:${CAMPAIGN_ID}"
migrate_key "budget:sync:campaign:${CAMPAIGN_ID}"
migrate_key "budget:inflight:campaign:${CAMPAIGN_ID}"
migrate_key "budget:lock:campaign:${CAMPAIGN_ID}"
migrate_key "budget:txid:campaign:${CAMPAIGN_ID}"
migrate_key "campaign:settings:${CAMPAIGN_ID}"

FCAP_PREFIX="fcap:c:${CAMPAIGN_ID}:u:"
cursor=0
while true; do
	result="$(redis_src SCAN "$cursor" MATCH "${FCAP_PREFIX}*" COUNT 100)"
	cursor="$(echo "$result" | head -1 | tr -d '\r')"
	keys="$(echo "$result" | tail -n +2)"
	if [ -n "$keys" ]; then
		while IFS= read -r key; do
			[ -n "$key" ] || continue
			migrate_key "$key"
		done <<< "$keys"
	fi
	[ "$cursor" = "0" ] && break
done

DAILY_PREFIX="budget:daily_spent:campaign:${CAMPAIGN_ID}:"
cursor=0
while true; do
	result="$(redis_src SCAN "$cursor" MATCH "${DAILY_PREFIX}*" COUNT 100)"
	cursor="$(echo "$result" | head -1 | tr -d '\r')"
	keys="$(echo "$result" | tail -n +2)"
	if [ -n "$keys" ]; then
		while IFS= read -r key; do
			[ -n "$key" ] || continue
			migrate_key "$key"
		done <<< "$keys"
	fi
	[ "$cursor" = "0" ] && break
done

echo "redis_migrate_campaign: verify target budget key..."
val="$(redis_dst GET "budget:campaign:${CAMPAIGN_ID}" 2>/dev/null | tr -d '\r' || true)"
if [ -z "$val" ] || [ "$val" = "(nil)" ]; then
	echo "redis_migrate_campaign: WARN budget:campaign:${CAMPAIGN_ID} missing on target (campaign may be paused / zero budget)" >&2
else
	echo "  budget:campaign:${CAMPAIGN_ID}=$val"
fi

echo "redis_migrate_campaign: OK — pause campaign before migrate, resume after verify (see docs/development.md#post-deploy-redis-reconciliation)"
