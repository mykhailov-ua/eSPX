# eSPX Architecture

## Topology

Five layers. All application services use host networking in compose; stateful stores publish ports on a bridge network.

1. **Ingress** (Nginx :8180): `/admin/*` to management, `/track/*` to tracker upstream. OpenResty Lua: per-campaign rate limit, edge blacklist, shard pick (CRC32). Edge hardening + optional XDP: [edge-hardening-plan.md](edge-hardening-plan.md); XDP detail: [edge-xdp-design.md](edge-xdp-design.md).
2. **Ingestion** (tracker x4, :8181-8184): gnet, PinnedWorkerPool, `processTrack()` shared core.
3. **Edge state** (Redis x4, :6479-6482, plus replicas and Sentinel x3): client-sharded; Lua atomicity per shard.
4. **Application**: Processor (:8186); Management (:8188, :51053); Auth and Payment gRPC (:51051, :51052).
5. **Persistence**: Postgres 16 and ClickHouse 24 for ads and auth; isolated `payment` schema.

### Control plane

- **Management** (`cmd/management`): REST admin API, HTMX cookie auth gateway, background workers (outbox, drain, schedule, pacing, recon, credit scoring). Settlement gRPC on `SETTLEMENT_SERVER_PORT` (default 51053).
- **Auth** (`cmd/auth`): gRPC for registration, login, PASETO tokens, API keys, email verification. Redis on shard 0 only (lockout, revocation).
- **Payment** (`cmd/payment`): gRPC intents, Stripe webhook HTTP, settlement outbox worker. Separate `payment` schema in Postgres.

### Ingestion plane

- **Tracker** (`cmd/tracker`): gnet `AdsPacketHandler`, 2 event loops per instance, `gnet.WithLockOSThread(false)`. Filter pipeline then `processTrack()` in `track_core.go` (shared with deprecated stdlib router for tests).
- **Processor** (`cmd/processor`): Per-shard `StreamConsumer` (Postgres group, ClickHouse group, fraud group), `SyncWorker`, `PartitionManager`.

---

## Redis Sharding

### Model

Four standalone Redis masters. `config.ExpectedRedisShardCount = 4`. `ENV=production` rejects `len(REDIS_ADDRS) != 4` at startup.

Not Redis Cluster: no hash slots, no `MOVED` redirects, no cross-slot transactions.

### Routing

```go
// internal/ads/sharding.go
slot := crc32Castagnoli(&campaignID) & 1023
shard := staticSlotSharder.slots[slot]  // precomputed: slot % N
```

| Consumer | Sharder | File |
| :--- | :--- | :--- |
| Tracker, management | `StaticSlotSharder` | `internal/ads/sharding.go` |
| Nginx Lua | `edge-slot-map.lua` (Castagnoli CRC32 + 1024 slots) | `deploy/nginx/lua/edge-slot-map.lua` |
| Tests (resize analysis) | `JumpHashSharder` | same package |

Routing is strictly `campaign_id`-based using the Fixed Slot Map. Composite keys or `user_id` routing are not used for sharding.

All services that write Redis keys by `campaign_id` must use `StaticSlotSharder`. `TestSharderStaticVsJumpHashDivergence` shows ~84% key mismatch between StaticSlot and JumpHash at N=4.

**StaticSlot vs JumpHash:**

| | StaticSlot | JumpHash |
| :--- | :--- | :--- |
| Hot-path cost | O(1) array index | Loop + float division |
| N change remap | ~100% keys | ~1/N keys |
| Use when | Fixed cluster size (production) | Frequent autoscaling experiments |

### Connection layer

`internal/database/redis_shards.go`:

- `ConnectRedisShards` — all shards for tracker, processor, management
- `ConnectRedisShard(ctx, cfg, 0)` — auth uses shard 0 only
- Direct mode: `Addrs = REDIS_ADDRS[i]`
- Sentinel mode: `MasterName = espx-shard-{i}`, `Addrs = REDIS_SENTINEL_ADDRS`; go-redis internal failover

Per-shard circuit breaker hook (`internal/database/redis_breaker.go`): trips on network errors after `REDIS_BREAKER_FAIL_THRESHOLD` (default 150), half-open probe after 5s.

### Shard-0 conventions

Several subsystems pin to shard 0 by convention, not by sharding algorithm:

- `campaigns:update` pub/sub: management **publishes** and trackers **subscribe** on shard 0 only (`publishCampaignUpdate` / `getPubSubRDB`)
- `BrandCreativeStore` reads `brand:creatives:*`
- Auth lockout, revocation, rate limit keys

Campaign budget and filter keys remain on `StaticSlotSharder(campaign_id)` shards via `getRDB`.

**Why:** These are low-volume global or session-scoped operations. Replicating pub/sub to all shards adds complexity without hot-path benefit. Trade-off: shard 0 outage affects registry refresh and auth lockout even if other shards are healthy.

### Global key replication

`internal/management/redis_global.go` writes to every shard:

| Key | Type | Content |
| :--- | :--- | :--- |
| `config:values` | HASH | `emergency_breaker`, `rate_limit_per_min`, billing amounts |
| `config:version` | STRING | Monotonic version for `SettingsWatcher` |
| `blacklist:{reason}` | SET | IP blocks (`manual`, `auto`, `fraud`, ...) |
| `brand:creatives:{brandID}` | STRING | JSON creative weights |

Write path: `syncGlobalConfigToAllShards` pipelines HSET + SET on each shard; `replicateConfigVersionFromPrimary` copies version from shard 0 on cold sync. Call sites: `handleUpdateSettings`, `SyncSystemState`, blacklist outbox handlers.

Read path: `SettingsWatcher` (`internal/ads/settings.go`) iterates shards until `GET config:version` succeeds, then loads `HGETALL config:values` from the responsive shard.

**Why replicate instead of a dedicated global Redis:** Eliminates extra network hop and failure domain on the filter hot path. Trade-off: N-fold write amplification on blacklist or config updates; management outbox pipelines these concurrently. Partial shard failure during write reverts outbox event to PENDING; lagging shards catch up on next successful sync or minute `SyncSystemState` tick.

### Sentinel and failover

Compose: `redis-N` masters, `redis-N-replica`, `sentinel-0/1/2` (`deploy/redis/sentinel-entrypoint.sh`).

Per master: quorum 2, `down-after-milliseconds 5000`, `failover-timeout 10000`.

Go services reconnect via Sentinel automatically. Expected recovery: ~10-15s (subjective down + promotion + breaker half-open).

**Dev default:** empty `REDIS_SENTINEL_ADDRS` dials `REDIS_ADDRS` directly.

**Production:**

```bash
REDIS_SENTINEL_ADDRS=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381
REDIS_MASTER_NAMES=espx-shard-0,espx-shard-1,espx-shard-2,espx-shard-3
```

Failover sequence: master stops responding; Sentinel marks SUBJECTIVE_DOWN after 5s; Sentinel promotes replica with SLAVEOF NO ONE; go-redis `FailoverClient` resolves new master via `get-master-addr-by-name`; tracker resumes PING and EVAL after breaker half-open (~5s default).

Nginx Lua (`deploy/nginx/lua/access-check.lua`): when `REDIS_SENTINEL_ADDRS` and `REDIS_MASTER_NAMES` are set, resolves master per shard via Sentinel (5s `sentinel_cache` TTL), falls back to `REDIS_ADDRS` on resolve failure.

**Not in scope:** Redis Cluster, dynamic N resharding. `campaigns:update` pub/sub is shard-0 only (publish and subscribe); campaign Redis keys stay on StaticSlot routes.

---

## Lua Scripts

### Unified filter (hot path)

| | |
| :--- | :--- |
| Source | `internal/ads/unified-filter.lua` |
| Embed | `//go:embed` in `unified_filter.go` |
| Load | `UnifiedFilter.PreloadScripts` then `SCRIPT LOAD` per shard at startup |
| Invoke | `EvalSha`; `Eval` on `NOSCRIPT`; metric `ad_redis_lua_noscript_total` |

**12 KEYS** (built in `UnifiedFilter.Check`):

| # | Key pattern |
| :--- | :--- |
| 1 | `rl:ip:{ip}` |
| 2 | `dup:{type}:{click_id}` |
| 3 | `budget:campaign:{uuid}` |
| 4 | `idempotency:click:{click_id}` |
| 5-6 | `budget:sync:campaign/customer:{uuid}` |
| 7-8 | `budget:dirty_campaigns`, `budget:dirty_customers` |
| 9 | Stream name (`ad:events:stream`) |
| 10 | `budget:daily_spent:campaign:{uuid}:{YYYYMMDD}` |
| 11 | `fcap:c:{camp}:u:{user}` or `fcap:b:{brand}:u:{user}` |
| 12 | `imp_ts:{user}:{campaign}` |

**Execution order inside Lua:** MGET budget batch, idempotency short-circuit, TTC (clicks only), budget sufficiency, even pacing daily cap, fcap, rate limit INCR, dedup SET NX, INCRBY budget + sync deltas + dirty SADD, SET idempotency, fcap INCR, impression timestamp SET / stream XADD.

Return `-1` triggers Go-side `tryRecoverBudgetFromRegistry` (in-memory snapshot, `SET NX`) then Postgres `warmBudgetKeyNX`. **Why two-tier recovery:** Registry avoids PG round trip on transient eviction; PG is authoritative when registry is stale.

### Other Lua usage (cold or async paths)

| Script | Location | Purpose |
| :--- | :--- | :--- |
| Budget spend (legacy) | `budget_store.go` inline | Alternate `CheckAndSpend` path |
| Sync prepare/commit | `sync_worker.go` inline | Processor: Redis lock, PG `UpdateSpend`, commit |
| Lockout | `auth/lockout.go` inline | Login brute-force limits on shard 0 |
| Recon adjust | `recon_service.go` inline | Atomic `INCRBY` on `budget:sync:campaign:*` |

Auth lockout previously considered pipelined INCR; kept as Lua for atomic conditional logic across lockout keys.

### Nginx edge Lua

`deploy/nginx/lua/access-check.lua`: **Phase IP** — circuit breaker, local `blacklist_cache` lookup (`b:<ip>` version stamp, synced by `edge-blacklist-sync.lua` every 5s from Redis shard 0). **Phase Body** — Content-Length, `read_body`, JSON/protobuf scan for `campaign_id`, `edge_rl.allow`. No per-request Redis on the hot path. Per-IP `limit_req` (100 r/s) in `nginx.conf`.

`edge-config.lua` (worker 0, 5s poll) mirrors `config:values` fields `rate_limit_per_min` and `rate_limit_window_ms` from Redis shard 0 into `lua_shared_dict edge_config`. `edge-rl.lua` applies a fixed-window counter per `campaign_id` before proxying; returns 429 when exceeded. Conn limits (`limit_conn` 200/IP, 8192 global) remain the OOM backstop.

**Why edge blacklist:** Reject abusive IPs before they reach tracker TCP stack. Trade-off: stale cache up to TTL; emergency block propagation depends on outbox replication latency. Blacklist sets must be replicated to every shard before edge Lua is authoritative; run `scripts/redis-reconcile-post-deploy.sh` after deploy.

**Planned edge hardening:** Lua pipeline fix (IP before body, no per-request Redis blacklist), optional XDP on public NIC. Tracker SLA (p95 < 50 ms, p99 < 80 ms, 100 ms ceiling) is measured on gnet; edge work protects it under abuse. Full plan: [edge-hardening-plan.md](edge-hardening-plan.md).

---

## Filter Pipeline (Go then Lua)

`FilterEngine.Check` (`internal/ads/filters.go`) runs before unified Lua:

1. **EmergencyBreakerFilter** — reads `config:values` from `SettingsWatcher` (replicated hash).
2. **FraudFilter** — MaxMind anonymous IP (DC/VPN/proxy). Fail-open on GeoIP error.
3. **GeoFilter** — campaign country targeting. Fail-open on lookup error.
4. **ScheduleFilter** — daypart and delivery window. Pure Go; uses registry snapshot.
5. **UnifiedFilter** — Redis Lua (budget, pacing, dedup, rate, fcap, TTC, enqueue).

Shared monotonic deadline via `filter_context.go`: `attachFilterDeadline` stores `runtime.nanotime` deadline; Redis client timeouts shrink to remaining budget.

**Why split Go/Lua this way:** GeoIP and schedule logic change often and are not atomic with budget keys. Co-locating them in Lua would require either stale embedded data or blocking RPC from Redis, both unacceptable at ingestion RPS.

### Time-to-click (TTC)

On click events, Lua reads `imp_ts:{user}:{campaign}` set by prior impression.

| Mode | Env | Behavior |
| :--- | :--- | :--- |
| Fail-open (default) | `TTC_FAIL_CLOSED=false` | Missing `imp_ts` accepts, return 10, increment `ad_ttc_bypass_total` |
| Fail-closed | `TTC_FAIL_CLOSED=true` | Missing `imp_ts` rejects (return 7, fraud reason `missing_imp_ts`) |

Enable fail-closed only after measuring bypass rate during Redis incidents.

---

## Ingestion Internals

### gnet handler

- Connection-local `connContext` pool (no global `sync.Pool` on hot path).
- DFA HTTP/1.1 scanner: zero-copy header mapping from ring buffer.
- `PinnedWorkerPool`: MPSC ring, cache-line padded, dispatches parse+filter to pinned goroutines.
- Health: background 2s probe per shard; `/health` returns `DEGRADED` if any shard fails.

### Budget cache warmer

`BudgetCacheWarmer` (`budget_warmer.go`): on startup and registry sync, pipelined `SET NX` for `budget:campaign:*` across shards. Seeds from registry snapshot, not Postgres.

**Why SET NX:** Avoids overwriting live decrements from concurrent ingestion. Trade-off: stale NX value if PG reconciliation has advanced budget without updating Redis; sync worker and recon correct drift.

### Creative routing

`BrandCreativeStore`: `atomic.Value` map, lock-free reads. FNV-1a over `userID + brandID` for deterministic weighted segment. Changes propagate via `SYNC_BRAND_CREATIVES` outbox to Redis `brand:creatives:{id}` on all shards.

### Telemetry (lossy by design)

| Component | Mechanism | Overflow behavior |
| :--- | :--- | :--- |
| `LatencyRing` | Power-of-2 ring, async Prometheus flush | Overwrites oldest |
| `FraudStreamWriter` | MPSC ring, 4096 slots, fixed arrays | Drop + metric |
| Audit log | mmap segments, sampled `AUDIT_LOG_SAMPLE_RATE` | Drop via sampling |
| Metrics | Pre-bound labels at startup; histogram sample mask | Reduced fidelity |

---

## Settlement Pipeline

### Stream consumption (processor)

Per shard, three consumer groups on `ad:events:stream`:

- `{group}_pg` to Postgres `events` partition + `campaign_stats` + `sync_idempotency`
- `{group}_ch` to ClickHouse batch insert
- `{group}_fraud` on `ad:fraud:stream` to ClickHouse `fraud_events`

Batch ordering: `ORDER BY campaign_id, event_date` in CTE before `ON CONFLICT DO UPDATE` prevents `campaign_stats` deadlocks.

Janitor: `XAutoClaim` on idle pending messages; exceeds `MAX_RETRIES` moves to `ad:events:dlq`.

### Postgres idempotency

`sync_idempotency` + `ON CONFLICT DO NOTHING`. At-least-once from Redis streams; exactly-once writes to relational aggregates.

### ClickHouse idempotency

`insert_deduplicate=1` on `ReplicatedMergeTree` with SHA-256 block token over click ID. Windowed dedup, not infinite.

### Budget sync worker

Per shard `SyncWorker`: Lua prepare (lock + inflight), PG `UpdateSpend`, Lua commit. Dirty sets (`budget:dirty_campaigns`) drive polling.

**Why async sync:** Keeps Lua path sub-millisecond; PG write latency isolated to background worker. Trade-off: brief window where Redis budget and PG ledger diverge; recon worker detects and adjusts.

---

## Control Plane Workflows

### Transactional outbox (management)

1. Business mutation + `outbox_events` insert in one PG transaction.
2. `OutboxWorker` (20ms poll): `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1000`, status to `PROCESSING`.
3. Commit PG, execute Redis pipelines (per-shard or global replicate).
4. Batch update status to `PROCESSED`.

**Why SKIP LOCKED over NOTIFY:** NOTIFY buffers are process-local and lost on consumer lag; under load they cause PG memory pressure. Trade-off: 20ms minimum propagation latency.

### Pacing controller

`PacingControllerWorker` (default 5min): compares actual spend vs even/ASAP profile in PG micro-units, emits outbox invalidation to Redis pacing keys.

### Schedule worker

Minute loop: `ClaimScheduledCampaignForUpdate`, transition pause/resume, emit `PAUSE_CAMPAIGN` / `RESUME_CAMPAIGN` outbox events.

### Reconciliation

`ReconWorker` (default 1h): compares PG ledger spend vs Redis `budget:sync:*` deltas, writes `recon_discrepancies`, optional Lua atomic adjust.

---

## Payment and Settlement

Separate concern from ingestion. Money truth is Postgres `balance_ledger`.

### Payment service

- Schema: `payment.payment_intents`, `webhook_events`, `payment_outbox`
- `CreatePaymentIntent`: idempotent on `idempotency_key`, `pg_advisory_xact_lock` on amount
- Provider: mock when `STRIPE_SECRET_KEY` empty; Stripe stub exists but checkout API returns `ErrProviderNotConfigured`
- Webhook: `POST /webhooks/stripe` on `PAYMENT_WEBHOOK_PORT` (8187)

### Settlement path

1. Webhook success enqueues `payment_outbox` row (`SETTLE_BALANCE`).
2. Payment `OutboxWorker` (100ms poll) calls gRPC `SettlementService.ApplyPaymentCredit` on management (:51053).
3. Management inserts `balance_ledger` row (type `PAYMENT_TOPUP`, `payment_intent_id`).
4. `ON CONFLICT (payment_intent_id) DO NOTHING` guards duplicate settlement.

Management initiates intents via `PaymentClient` when `PAYMENT_INTERNAL_TOKEN` is set (`POST /admin/customers/{id}/payment-intent`).

**Why separate service:** Payment PCI scope, schema isolation, and failure containment. Trade-off: gRPC hop for settlement; compose has circular `depends_on` between payment and management.

---

## Log Broker (optional)

`cmd/broker` + `pkg/broker/`: mmap append-only segments, sparse index, CRC32-framed wire protocol, configurable durability (`async`|`group`|`sync`), Redis coordinator for leader election with fencing epochs. Ingest path stores **raw** segment bytes (no compression on the broker hot path). Not in default compose; used with `log-shipper` and optional `deploy/broker-ha/` for HA produce/fetch.

**Why mmap without sync fsync on append:** Keeps produce latency low; durability is explicit via `-durability` flag (default async 100ms flush). Trade-off: `status=0` in async mode is not a durable-until-crash ACK.

**Compression elsewhere:** Tracker `pkg/logger` applies async zstd + AES-GCM when rotating **tracker** segment files (`.log.zst.ready`). That pipeline is separate from `pkg/broker/` mmap logs. `log-evacuator` uploads compressed tracker artifacts to S3 with checkpointed exactly-once delivery.

---

## API Contracts

Protobuf definitions in `api/` (buf generate to vtproto):

| Proto | Package | Consumers |
| :--- | :--- | :--- |
| `events.proto` | `ads.v1` | Tracker, processor (AdEvent, AdStreamEvent, TrackResponse) |
| `auth.proto` | `auth` | Auth gRPC |
| `payment.proto` | `payment` | Payment gRPC |
| `settlement.proto` | `settlement` | Management settlement gRPC |

---

## Database Schemas

Single Postgres database `ad_event_processor`:

| Migration tree | Domain |
| :--- | :--- |
| `internal/ads/migrations/` (26 files) | Campaigns, events (partitioned), ledger, outbox, templates, creatives, recon, fraud config |
| `internal/auth/migrations/` (8 files) | Users, sessions, API keys |
| `internal/payment/migrations/` (2 files) | Payment intents, webhooks, outbox |

Money columns: BIGINT micro-units (migration 00020). Ledger type `PAYMENT_TOPUP` + `payment_intent_id` unique index (00024, 00025).

ClickHouse (`deploy/clickhouse/init.sql`): `impressions`, `clicks`, `conversions`, `fraud_events` with `ReplacingMergeTree`, monthly partitions, TTL 90-180d. Recon MVs (`recon_materialized_views.sql`): `mv_campaign_hourly_impressions` and `mv_campaign_hourly_clicks` for hourly volume checks without full scans.

---

## Observability and Alerts

Prometheus (`deploy/monitoring/prometheus.yml`) scrapes:

| Job | Targets | Metrics path |
| :--- | :--- | :--- |
| `tracker` | :8181–8184 | `/metrics` and `/health` on gnet `SERVER_PORT` (Prometheus default in compose) |
| `processor` | :8186 | `/metrics` |
| `management` | :8188 | `/metrics` |
| `auth` | :9091 (`AUTH_METRICS_PORT`) | `/metrics` |
| `payment` | :8187 (`PAYMENT_WEBHOOK_PORT`) | `/metrics` on webhook mux |

Rule file: `deploy/monitoring/prometheus.rules.yml`. Grafana provisioning under `deploy/monitoring/grafana/`.

| Alert | Condition |
| :--- | :--- |
| CircuitBreakerOpen | Consumer group breaker open >5min |
| DatabaseWriteErrors | Batch persistence failures |
| DeadLetterQueueSpike | DLQ length >100 |
| HighRequestLatency | p99 tracker >15ms |
| RedisLuaNoScriptFallback | `ad_redis_lua_noscript_total` rate >0 |
| BudgetCacheMissPG | Budget reload from Postgres (should be rare) |
| TTCBypassRateHigh | Bypass >1% of /track (pre fail-closed) |
| ManagementOutboxLagHigh | `ad_management_outbox_oldest_pending_seconds` >30 |
| TrackerHealthDegraded | `ad_tracker_health_degraded` == 1 |

Telegram proxy (`cmd/telegram`): Alertmanager webhook to HTML message to Bot API.

### Graceful shutdown

All long-running binaries honor `SHUTDOWN_TIMEOUT_MS` (default 15000) via `config.LifecycleShutdownTimeout()`. On SIGTERM: stop accepting work, drain in-flight requests or stream batches, flush ClickHouse buffers, then exit. Tracker and processor respect `DRAIN_TIMEOUT_MS` for connection drain. Tune both in `.env` for rolling deploy windows.

---

## Known Limitations

| Area | Limitation |
| :--- | :--- |
| `campaigns:update` pub/sub | Shard 0 only; registry also polls PG on interval |
| Global config writes | 4x amplification; partial shard failure leaves lag until next sync |
| Blacklist replication | 4x `SADD` per block; required for edge shard-local lookup |
| gnet worker pool | Copies request buffer and re-parses HTTP when offloading from event loop |
| ClickHouse dedup | `insert_deduplicate=1` is windowed, not permanent |
| Auth Redis outage | Returns 401 fail-closed; optional future: 503 for infra vs 401 for invalid token |
| Shard resize | StaticSlot remaps ~85% keys on N+1; blue/green + `scripts/redis-migrate-campaign.sh` required |

**Conscious non-goals:** Redis Cluster (Sentinel covers failover); JumpHash on tracker (~84% divergence with StaticSlot management); removing gnet (perf gate dependency).
