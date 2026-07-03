# Broker lab (HA + chaos)

Local stack for mmap broker HA drills and coordination chaos tests. Not part of the main `docker-compose.yml`.

## Build

```bash
go build -o deploy/broker/bin/espx-broker ./cmd/broker
```

## HA stack (two brokers + Redis + HAProxy)

```bash
docker compose -f deploy/broker/docker-compose.yml up -d
```

| Port | Role |
|------|------|
| 9092 | HAProxy produce (leader-only via `/leaderz`) |
| 9093 | HAProxy any healthy broker (fetch) |
| 9093/9094 | Broker TCP (direct) |
| 8081/8082 | Broker health |
| 6379 | Coordination Redis |

Override binary: `ESPX_BROKER_BIN=/path/to/espx-broker docker compose -f deploy/broker/docker-compose.yml up -d`

## Sentinel overlay

Same coordination Redis (6379); adds replica on 6380 and Sentinel on 26379.

```bash
docker compose -f deploy/broker/docker-compose.yml \
  -f deploy/broker/docker-compose.sentinel.yml up -d
```

Coordination-only (chaos sentinel test, no brokers):

```bash
docker compose -f deploy/broker/docker-compose.yml \
  -f deploy/broker/docker-compose.sentinel.yml up -d redis redis-replica redis-sentinel
```

Or run the full script: `scripts/broker-chaos-lab.sh`
