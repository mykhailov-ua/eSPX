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
| `fraud-scorer` | — | Standalone fraud scorer + model registry watcher (`fraud-scorer` compose profile) |
| `alertmanager-telegram` | 8222 | Alertmanager to Telegram proxy |
| `log-evacuator` | — | Rotated tracker log segments → S3 (compose profile `tools`) |
| `broker` | — | mmap log broker (not in compose; optional log evacuation) |
| `dlq`, `admin` | — | Operator CLIs |

## Redis Sharding Strategy

Client-sharded standalone Redis masters (**not Redis Cluster**). Production locks **`N = 4`** shards (`config.ExpectedRedisShardCount`; `ENV=production` rejects `len(REDIS_ADDRS) != 4`). Lua scripts require multi-key atomicity on one instance; cluster hash tags would not remove cross-shard coordination for global config.

Full design, chaos profile, and rollout: **[Edge layer](docs/EDGE.md)** (Part II sharding, Part III control plane). Legal/compliance guardrails (Art. 361 UK, passive telemetry, eBPF): **[GUIDE_COMPLIANCE.md](GUIDE_COMPLIANCE.md)**.

### Campaign → shard routing

All budget, filter, and stream keys route by **`campaign_id` only** (composite `campaign_id + user_id` is for edge rate limiting, not shard pick):

```
slot  = crc32_castagnoli(campaign_id) & 1023    // 1024 fixed slots
shard = slot_table[slot]                        // precomputed; default slot % N
```

| Component | Implementation |
| :--- | :--- |
| Go tracker / processor | `StaticSlotSharder` (`internal/ingestion/sharding.go`) — 1024-entry table behind `atomic.Value`; cold-path reload via `StoreSlotMap` / `SwapSnapshot` |
| Nginx edge | `deploy/nginx/lua/edge-slot-map.lua` — same Castagnoli CRC + slot table (must match Go) |
| Slot index (migration) | `CampaignSlotIndex(id)` — `crc32 & 1023` for cohort copy / autoscale |
| Legacy (do not use in prod) | `JumpHashSharder` — different distribution; management and tracker must share `StaticSlotSharder` |

Hot-path `GetShard` is **~5.6 ns/op, 0 allocs/op** (amd64 HW CRC). Shard count does not change lookup cost.

### Control plane (async, off hot path)

| Mechanism | Role |
| :--- | :--- |
| **Outbox** (`management`) | Replicate `config:values`, blacklist, campaign keys to every shard; priority lanes for pause/freeze |
| **Slot map** (`slot_map_versions` PG) | Draft → MIGRATING → ACTIVE; `SlotMigrationOrchestrator` copies keys; trackers reload table atomically |
| **Migration fence** (`MIGRATION_FENCE_ENABLED`) | Lua rejects debit when `migration_gen` mismatches PG — no dual debit during copy |
| **UDP quotas** (`UDP_CONTROL_ENABLED`) | Management `:8190` → tracker `:8191`: per-shard RPS epochs, `CONFIG_SNAPSHOT` / `CONFIG_REQUEST`; ingress gate only (never writes `budget:*`) |
| **Quota repair** (`QUOTA_MODE=live`, `QUOTA_AUTO_REPAIR`) | PG↔Redis drift auto-repair; dead-shard reservation release with quorum |
| **Registry invalidation** | `campaigns:update` pub/sub on **shard 0** → tracker in-memory reload |

**Degradation:** per-shard circuit breakers, Sentinel failover (~10–15 s), UDP STALE → canary ingress floor (fail-closed). Recovery runbook: [EDGE.md Part III §1.1](docs/EDGE.md).

### Tiered Lua

| Tier | Script | When |
| :--- | :--- | :--- |
| A | Go pre-gates | Geo, schedule, fraud, emergency breaker — 0 Redis RTT |
| B | `budget_fast.lua` | Impressions with warmed budget (`LUA_FAST_PATH_ENABLED`) |
| C | `unified-filter.lua` | Clicks, TTC, fcap, full debit + stream XADD |

One `EVALSHA` per accepted event; financial debit stays atomic in Lua.

### Key layout

| Pattern | Scope | Purpose |
| :--- | :--- | :--- |
| `budget:campaign:{id}` | Shard-local | Live remaining budget (micro-units) |
| `budget:sync:*`, `budget:dirty_*`, `budget:migration_gen:{id}` | Shard-local | PG sync deltas, dirty sets, migration fence |
| `dup:*`, `idempotency:click:*` | Shard-local | Dedup and idempotency |
| `fcap:*`, `imp_ts:*` | Shard-local | Frequency cap, time-to-click |
| `ad:events:stream` | Shard-local | Ingestion stream (one namespace per shard) |
| `config:values`, `blacklist:*` | All shards | Replicated global state |
| `brand:creatives:{id}` | All shards | Creative rotation payload |

**Shard-0 conventions** (by policy, not hash): `campaigns:update` pub/sub, auth lockout, brand creatives. Campaign budget keys still follow `StaticSlotSharder(campaign_id)`.

### StaticSlot vs JumpHash

| | StaticSlot | JumpHash |
| :--- | :--- | :--- |
| Hot-path cost | O(1) table + HW CRC, no float math | Loop over buckets |
| Resize blast radius | ~67% campaigns remap on N change | ~1/N remap |
| Ops model | Fixed N=4; resize = slot migration project | Better for elastic N |

Production uses **fixed slot map + orchestrated migration**, not live JumpHash resharding.

### Sentinel and clients

Go services (`ConnectRedisShards`) and Nginx Lua resolve masters via Sentinel when `REDIS_SENTINEL_ADDRS` is set; otherwise direct `REDIS_ADDRS`. Smoke: `scripts/redis-ops/verify_redis_topology.sh`.

### Rollout flags (staging defaults)

| Flag | Staging | Purpose |
| :--- | :--- | :--- |
| `MIGRATION_FENCE_ENABLED` | `true` | Lua migration_gen reject |
| `UDP_CONTROL_ENABLED` | `true` | UDP control plane + ingress gate |
| `QUOTA_MODE` / `QUOTA_AUTO_REPAIR` | `live` / `true` | Distributed quota + repair |
| `LUA_FAST_PATH_ENABLED` | `false` | Tier B fast Lua (canary after soak) |

Apply: `bash scripts/k8s/k8s_staging_apply.sh` (see [EDGE.md](docs/EDGE.md) Part III §8).

## Unified Filter (Lua)

`internal/ingestion/unified-filter.lua` (Tier C) and `budget_fast.lua` (Tier B) embedded via `//go:embed`, preloaded with `SCRIPT LOAD` at tracker startup, invoked via `EVALSHA` with `EVAL` fallback on `NOSCRIPT`. Router in `UnifiedFilter.Check` picks tier by event type and flags.

Single atomic script per event: MGET budget state, TTC check, pacing, fcap, rate limit, dedup SET NX, budget decrement, sync delta, stream XADD.

| Return | Meaning |
| :--- | :--- |
| `0` | Success or idempotent replay |
| `-1` | Budget key missing; Go reloads from registry then Postgres |
| `1`-`5` | Rate, dup, budget, pacing, fcap |
| `6`-`7` | Low TTC, missing impression timestamp (fail-closed) |
| `10` | TTC bypass (fail-open, no `imp_ts` key) |

Single Lua script per event: one network round trip replaces 8–12 pipelined commands. Trade-off: longer Redis engine lock per call; acceptable because keys are colocated on one shard.

Geo, schedule, and fraud checks run in Go before Lua. MaxMind and daypart logic are CPU-bound; embedding them in Redis would require GeoIP in Lua or RPC from Lua.

## Design Decisions

| Subsystem | Choice | Rationale | Trade-off |
| :--- | :--- | :--- | :--- |
| Serialization | Protobuf + vtproto | Pool-backed marshal/unmarshal; `bytes` fields slice from socket buffers | Schema evolution requires buf generate + deploy coordination |
| Networking | gnet + pinned worker pool | Event-driven I/O; MPSC ring offload for parse/filter work | More complex than net/http; stdlib router kept for tests only |
| Sharding | StaticSlot (1024 slots → N masters) | HW CRC + atomic slot table; edge/Go parity | Resize = slot migration, not config flip |
| Ingress control | UDP epoch quotas + padded atomics | Sub-second per-shard RPS without HTTP on hot path | Fail-closed when STALE; budget stays in Lua/PG |
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
- Alertmanager to `cmd/alertmanager-telegram` for Telegram delivery.
- Key alerts: circuit breaker open >5m, DLQ >100, p99 tracker latency >15ms, TTC bypass rate >1%.

## Tooling and CI

Local workflows use `Makefile`, optional [Task](Taskfile.yaml), and categorized `scripts/<area>/` (codegen, compose, perf gate, Redis ops, chaos). See [Development Guide](docs/DEVELOPMENT.md) for commands and CI workflow map under `.github/workflows/`.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — topology, patterns, microservices, Fraud scoring
- [Development](docs/DEVELOPMENT.md) — setup, runbooks, CI, infra, perf gate
- [Go hot path](docs/GO.md) — gnet, zero-alloc, filter engine
- [Edge layer](docs/EDGE.md) — ingress, Redis/Lua, UDP control (Part III)
- [Databases](docs/DATABASE.md) — PostgreSQL ledger + ClickHouse analytics (incl. ML features)
- [Redis hot state](docs/REDIS.md) — sharding, Lua validation, risks
- [Crypto payments](docs/CRYPTO_GATEWAY.md) — BTC/ETH/USDT gateway options; Stripe patterns
- [Administrative complex](docs/MANAGEMENT.md) — cold-path admin API, billing, reports, roadmap
