#!/bin/sh
# Verifies REDIS_ADDRS shard count matches the fixed production topology (N=4).
set -eu

EXPECTED="${REDIS_SHARD_COUNT:-4}"
ENV_FILE="${1:-.env}"

if [ ! -f "$ENV_FILE" ]; then
	echo "verify_redis_topology: missing env file: $ENV_FILE" >&2
	exit 1
fi

REDIS_ADDRS="$(grep -E '^REDIS_ADDRS=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '"' | tr -d "'")"
if [ -z "$REDIS_ADDRS" ]; then
	echo "verify_redis_topology: REDIS_ADDRS not set in $ENV_FILE" >&2
	exit 1
fi

COUNT=0
OLDIFS="$IFS"
IFS=,
for addr in $REDIS_ADDRS; do
	addr="$(echo "$addr" | tr -d ' ')"
	[ -n "$addr" ] || continue
	COUNT=$((COUNT + 1))
done
IFS="$OLDIFS"

if [ "$COUNT" -ne "$EXPECTED" ]; then
	echo "verify_redis_topology: expected $EXPECTED Redis shards, got $COUNT (REDIS_ADDRS=$REDIS_ADDRS)" >&2
	exit 1
fi

echo "verify_redis_topology: OK ($COUNT shards)"
