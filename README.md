# eSPX (Event Stream Pacing)

Real-time ad event ingestion, budget enforcement, and settlement pipeline. Go services on a client-sharded Redis edge state layer, PostgreSQL ledger, and ClickHouse telemetry.

## System Overview

Request flow: Nginx (:8180) load-balances `/track` to tracker replicas (:8181-8184). Processor (:8186) drains streams to Postgres and ClickHouse. Management (:8188, settlement gRPC :51053) runs admin API and Redis propagation workers. Auth (:51051), Payment (:51052/:8187), Billing (:51054), and Notifier (:8085) are separate gRPC services. IVT detector runs as an off-path batch job.

**Hot path:** `POST /track` on gnet trackers. One Redis round trip per accepted event via `unified-filter.lua` (budget, pacing, dedup, rate limit, fcap, stream enqueue). Go filters run before Lua for geo, schedule, fraud, and emergency breaker checks that cannot execute in Redis.

**Cold path:** Management REST API, auth gRPC, payment webhooks, outbox workers, reconciliation, pacing controller.

## Services

| Binary | Port(s) | Role |
| :--- | :--- | :--- |
| `tracker` | 8181-8184 | gnet ingestion, Lua filter, registry sync, budget warmer |
| `processor` | 8186 | Per-shard stream consumers to Postgres + ClickHouse; budget sync |
| `management` | 8188, 51053 | Admin REST, settlement gRPC, outbox to Redis, recon/pacing workers |
| `auth` | 51051 | gRPC: Argon2id, PASETO, sessions, API keys |
| `payment` | 51052, 8187 | Payment intents gRPC, Stripe webhooks, settlement outbox |
| `billing` | 51054 | Invoice generation gRPC; management HTMX proxy when configured |
| `notifier` | 8085 | Async notifications gRPC (Telegram, Slack, SMTP, SMS) |
| `ivt-detector` | — | ClickHouse IVT scan → fraud blacklist via management API |
| `telegram` | 8222 | Alertmanager to Telegram proxy |
| `log-evacuator` | — | Rotated tracker log segments → S3 (compose profile `tools`) |
| `broker` | — | mmap log broker (not in compose; optional log evacuation) |
| `dlq`, `admin` | — | Operator CLIs |

## Redis Topology

Six standalone Redis masters (not Redis Cluster). Campaign-scoped keys colocate on one shard via `StaticSlotSharder`: `crc32Castagnoli(campaignID) & 1023`, then `slot % N`.

| Pattern | Scope | Purpose |
| :--- | :--- | :--- |
| `budget:campaign:{id}` | Shard-local | Live remaining budget (micro-units) |
| `budget:sync:*`, `budget:dirty_*` | Shard-local | PG sync deltas and dirty sets |
| `dup:*`, `idempotency:click:*` | Shard-local | Dedup and idempotency |
| `fcap:*`, `imp_ts:*` | Shard-local | Frequency cap, time-to-click |
| `ad:events:stream` | Shard-local | Ingestion stream (one namespace per shard) |
| `config:values`, `blacklist:*` | All shards | Replicated global state |
| `brand:creatives:{id}` | All shards | Creative rotation payload |

**Why client sharding, not Cluster:** Lua scripts require multi-key atomicity within a single Redis instance. Cluster would force hash tags on every key and still complicate global replication. Fixed N=4 shards give predictable blast radius (one shard down affects ~1/N campaigns) without slot migration machinery.

**Why StaticSlot over JumpHash:** O(1) table lookup, no branches, no float math on the hot path. Trade-off: changing N remaps ~100% of keys (vs ~1/N for JumpHash). Production policy locks N=6; resize requires blue/green key migration.

**Sentinel:** Go services (`ConnectRedisShards`) and Nginx Lua use Sentinel when `REDIS_SENTINEL_ADDRS` is set. Empty sentinel addrs fall back to direct `REDIS_ADDRS`.

## Unified Filter (Lua)

`internal/ads/filter/unified.lua` embedded via `//go:embed`, preloaded with `SCRIPT LOAD` at tracker startup, invoked via `EVALSHA` with `EVAL` fallback on `NOSCRIPT`.

Single atomic script per event: MGET budget state, TTC check, pacing, fcap, rate limit, dedup SET NX, budget decrement, sync delta, stream XADD.

| Return | Meaning |
| :--- | :--- |
| `0` | Success or idempotent replay |
| `-1` | Budget key missing; Go reloads from registry then Postgres |
| `1`-`5` | Rate, dup, budget, pacing, fcap |
| `6`-`7` | Low TTC, missing impression timestamp (fail-closed) |
| `10` | TTC bypass (fail-open, no `imp_ts` key) |

**Why one Lua script:** One network round trip replaces 8-12 pipelined commands. Trade-off: longer Redis engine lock per call; acceptable because all keys are colocated on one shard and the script avoids cross-key races without WATCH/MULTI.

**Why Lua for filter but Go for geo/schedule:** MaxMind lookups and daypart logic are CPU-bound and change frequently. Running them in Redis would require embedding GeoIP data or RPC from Lua, neither viable at tracker RPS.

## Design Decisions

| Subsystem | Choice | Why | Trade-off |
| :--- | :--- | :--- | :--- |
| Serialization | Protobuf + vtproto | Pool-backed marshal/unmarshal; `bytes` fields slice from socket buffers | Schema evolution requires buf generate + deploy coordination |
| Networking | gnet + pinned worker pool | Event-driven I/O; MPSC ring offload for parse/filter work | More complex than net/http; stdlib router kept for tests only |
| Sharding | StaticSlot (1024 slots) | Bitwise mask + precomputed table; fits L1 | Cluster resize is a migration project, not a config change |
| Budget | int64 micro-units (10^6) | No float rounding in Lua INCRBY or PG ledger | Display layer must divide; overflow range is ample for ad spend |
| Outbox | `SKIP LOCKED` polling | Short PG transactions; Redis I/O outside TX boundary | Latency floor = poll interval (20ms mgmt, 100ms payment); no LISTEN/NOTIFY push |
| Rate limiting | INCR + EXPIRE in Lua | Atomic without separate Lua script per concern | Per-IP counter lives on campaign shard, not IP shard |
| Monotonic time | `go:linkname` nanotime | Filter deadlines immune to NTP jumps; no heap escape | Internal runtime dependency; breaks if Go renames symbol |
| Telemetry | LatencyRing + sampling | Prometheus observations off gnet event loop | Lossy under overload; fraud ring drops at 4096 capacity |
| Global config | Replicate to all shards | Shard-local reads on hot path; no cross-shard fetch | Write amplification on config change (4x HSET) |
| Payment | Separate service + schema | Money state isolated from ingestion; advisory locks on amount | Cross-service gRPC to management for ledger credit |
| Money source of truth | Postgres `balance_ledger` | ACID, audit trail, reconciliation | Redis budget is a cache; drift corrected by sync worker + recon |

## Money Flow

1. Operator or customer initiates top-up via management to payment gRPC `CreatePaymentIntent`.
2. Stripe webhook (or mock provider) confirms payment; row in `payment.payment_outbox`.
3. Payment outbox worker calls management settlement gRPC `ApplyPaymentCredit`.
4. Management credits `balance_ledger` with `ON CONFLICT` guard on `payment_intent_id`.
5. Campaign spend: tracker Lua decrements `budget:campaign:*`; processor sync worker flushes deltas to Postgres.
6. Monthly billing: management calls billing gRPC `GenerateInvoice` to aggregate ledger spend into `billing.invoices`.

Redis does not hold payment or ledger state.

## Observability

- Prometheus scrapes tracker (:8181–8184 `/metrics`), processor (:8186), management (:8188), auth (:9091), payment (:8187). Tracker also exposes a metrics sidecar on :9090 when configured.
- Grafana dashboards: throughput, Redis breaker state, Lua NOSCRIPT fallbacks, budget cache misses.
- Alertmanager to `cmd/telegram` for Telegram delivery.
- Key alerts: circuit breaker open >5m, DLQ >100, p99 tracker latency >15ms, TTC bypass rate >1%.

## Tooling and CI

Local workflows use `Makefile`, optional [Task](Taskfile.yml), and flat `scripts/` (codegen, compose, perf gate, Redis ops, chaos). See [Development Guide](docs/development.md) for commands and CI workflow map under `.github/workflows/`.

## Documentation

- [Architecture](docs/architecture.md) — subsystem workflows, Lua key layout, failover, limitations
- [Development](docs/development.md) — setup, ports, scripts, runbooks, perf gate, CI
