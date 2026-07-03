#!/bin/sh
# Generates sentinel.conf for all eSPX Redis shards and execs redis-sentinel.
set -eu

CONF=/tmp/sentinel.conf
: "${REDIS_PASSWORD:?REDIS_PASSWORD is required}"
: "${REDIS_SHARD_COUNT:=4}"

cat >"$CONF" <<EOF
port 26379
dir /tmp
protected-mode no
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
EOF

i=0
while [ "$i" -lt "$REDIS_SHARD_COUNT" ]; do
	cat >>"$CONF" <<EOF
sentinel monitor espx-shard-${i} redis-${i} 6379 2
sentinel auth-pass espx-shard-${i} ${REDIS_PASSWORD}
sentinel down-after-milliseconds espx-shard-${i} 5000
sentinel failover-timeout espx-shard-${i} 10000
sentinel parallel-syncs espx-shard-${i} 1
EOF
	i=$((i + 1))
done

exec redis-sentinel "$CONF" --sentinel
