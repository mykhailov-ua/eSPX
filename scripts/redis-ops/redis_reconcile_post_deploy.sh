#!/usr/bin/env bash
# Post-deploy reconciliation: compare config:* and blacklist cardinality across all Redis shards.
# Exit 0 when consistent; non-zero when drift detected. Read-only.
set -euo pipefail

source "$(cd "$(dirname "$0")/../lib" && pwd)/paths.sh"
cd "$ROOT"
ENV_FILE="${1:-$ROOT/.env}"
SHARD_COUNT="${REDIS_SHARD_COUNT:-4}"

if [ ! -f "$ENV_FILE" ]; then
	echo "redis_reconcile_post_deploy: missing env file: $ENV_FILE" >&2
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
	echo "redis_reconcile_post_deploy: expected $SHARD_COUNT shards, got ${#ADDRS[@]}" >&2
	exit 1
fi

redis_cli() {
	local hostport="$1"
	shift
	local host="${hostport%%:*}"
	local port="${hostport##*:}"
	redis-cli -h "$host" -p "$port" -a "$REDIS_PASSWORD" --no-auth-warning "$@"
}

FAIL=0

echo "redis_reconcile_post_deploy: checking config:version on ${#ADDRS[@]} shards..."
REF_VERSION=""
for i in "${!ADDRS[@]}"; do
	v="$(redis_cli "${ADDRS[$i]}" GET config:version 2>/dev/null | tr -d '\r' || true)"
	if [ -z "$v" ] || [ "$v" = "(nil)" ]; then
		v="MISSING"
	fi
	if [ -z "$REF_VERSION" ]; then
		REF_VERSION="$v"
	fi
	if [ "$v" != "$REF_VERSION" ]; then
		echo "  shard $i (${ADDRS[$i]}): config:version=$v (ref=$REF_VERSION)" >&2
		FAIL=1
	else
		echo "  shard $i: config:version=$v"
	fi
done

echo "redis_reconcile_post_deploy: checking config:values field count..."
REF_FIELDS=""
for i in "${!ADDRS[@]}"; do
	n="$(redis_cli "${ADDRS[$i]}" HLEN config:values 2>/dev/null | tr -d '\r' || echo 0)"
	if [ -z "$REF_FIELDS" ]; then
		REF_FIELDS="$n"
	fi
	if [ "$n" != "$REF_FIELDS" ]; then
		echo "  shard $i: HLEN config:values=$n (ref=$REF_FIELDS)" >&2
		FAIL=1
	else
		echo "  shard $i: HLEN config:values=$n"
	fi
done

echo "redis_reconcile_post_deploy: checking blacklist:manual SCARD..."
REF_BL=""
for i in "${!ADDRS[@]}"; do
	n="$(redis_cli "${ADDRS[$i]}" SCARD blacklist:manual 2>/dev/null | tr -d '\r' || echo 0)"
	if [ -z "$REF_BL" ]; then
		REF_BL="$n"
	fi
	if [ "$n" != "$REF_BL" ]; then
		echo "  shard $i: SCARD blacklist:manual=$n (ref=$REF_BL)" >&2
		FAIL=1
	else
		echo "  shard $i: SCARD blacklist:manual=$n"
	fi
done

echo "redis_reconcile_post_deploy: checking blacklist:fraud SCARD..."
REF_FRAUD=""
for i in "${!ADDRS[@]}"; do
	n="$(redis_cli "${ADDRS[$i]}" SCARD blacklist:fraud 2>/dev/null | tr -d '\r' || echo 0)"
	if [ -z "$REF_FRAUD" ]; then
		REF_FRAUD="$n"
	fi
	if [ "$n" != "$REF_FRAUD" ]; then
		echo "  shard $i: SCARD blacklist:fraud=$n (ref=$REF_FRAUD)" >&2
		FAIL=1
	else
		echo "  shard $i: SCARD blacklist:fraud=$n"
	fi
done

if [ "$FAIL" -ne 0 ]; then
	echo "redis_reconcile_post_deploy: DRIFT detected — run management cold sync or scripts/redis-ops/redis_migrate_campaign.sh" >&2
	exit 1
fi

echo "redis_reconcile_post_deploy: OK"
