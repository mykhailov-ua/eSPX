# eSPX Architecture

Subsystem topology, data flows, and operational contracts. Detailed deep-dives:

| Document | Topic |
| :--- | :--- |
| [EDGE.md](./EDGE.md) | L4/L7 ingress, Redis, Lua, UDP control (Part III) |
| [DATABASE.md](./DATABASE.md) | PostgreSQL ledger/outbox, ClickHouse analytics |
| [GO.md](./GO.md) | gnet, zero-alloc, filter engine |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | Setup, runbooks, CI, infra |
| Appendices A–B (below) | Design patterns, microservices scoring |

## Topology

Five layers. All application services use host networking in compose; stateful stores publish ports on a bridge network.

1. **Ingress** (Nginx :8180): `/admin/*` to management, `/track/*` to tracker upstream. OpenResty Lua: per-campaign rate limit, edge blacklist, shard pick (CRC32). Optional XDP/eBPF L4 filter on the public NIC (see Ingress section below).
2. **Ingestion** (tracker x4, :8181-8184): gnet, PinnedWorkerPool, `processTrack()` shared core.
3. **Edge state** (Redis x4, :6479-6482, plus replicas and Sentinel x3): client-sharded; Lua atomicity per shard.
4. **Application**: Processor (:8186); Management (:8188, settlement gRPC :51053, UDP control :8190); Auth (:51051), Payment (:51052, :8187), Billing (:51054), Notifier (:8085) gRPC; IVT detector and Fraud scoring (batch, cold path).
5. **Persistence**: Postgres 16 and ClickHouse 24 for ads, auth, payment, billing, and notifier schemas.

### Control plane

- **Management** (`cmd/management`): REST admin API, settlement gRPC (`SETTLEMENT_SERVER_PORT`, default 51053), UDP control publisher (`UDP_CONTROL_PORT`, default 8190), HTMX cookie auth gateway, background workers (outbox, drain, schedule, pacing, recon, quota repair, `QuotaManager` when `QUOTA_MODE=shadow|live`, credit scoring, slot migration).
- **Auth** (`cmd/auth`): gRPC for registration, login, PASETO tokens, API keys, email verification. Redis on shard 0 only (lockout, revocation).
- **Payment** (`cmd/payment`): gRPC intents, Stripe webhook HTTP, settlement outbox worker. Separate `payment` schema in Postgres.
- **Billing** (`cmd/billing`): gRPC invoice generation from `balance_ledger` aggregates; `billing` schema (invoices, tax profiles). Management proxies HTMX billing UI when `BILLING_INTERNAL_TOKEN` is set.
- **Notifier** (`cmd/notifier`): gRPC notification enqueue + background worker (Telegram, Slack, SMTP, SMS). `notifier` schema; starts when `NotifierConfigured()` is true.
- **IVT detector** (`cmd/ivt-detector`): ClickHouse batch scan → fraud blacklist and ML threat enqueue via management gRPC/outbox. When `FRAUD_SCORER_STANDALONE=true`, embedded scorer is disabled; scoring runs in `fraud-scorer`.
- **Fraud scoring** (`cmd/fraud-scorer`): Standalone cold-path worker — Postgres model registry watcher, ClickHouse feature reads, LightGBM/Isolation Forest ensemble scoring, management outbox for `ML_SCORE_BOOST`, `ML_GHOST_IVT`, `ML_BLACKLIST_ADD`, and `ML_MODEL_VERSION` sync. Optional ONNX scorer behind `-tags fraudscoring_onnx`. See [Fraud scoring](#fraud-scoring-cold-path) below.

### Ingestion plane

- **Tracker** (`cmd/tracker`): gnet `AdsPacketHandler`, 2 event loops per instance, `gnet.WithLockOSThread(false)`. UDP ingress quota gate when `UDP_CONTROL_ENABLED=true` (`:8191`). Filter pipeline then `processTrack()` in `track_core.go` (shared with deprecated stdlib router for tests).
- **Processor** (`cmd/processor`): Per-shard `StreamConsumer` (Postgres group, ClickHouse group, fraud group), `SyncWorker`, `PartitionManager`. When `FRAUD_SCORING_ENABLED=true`, `MicroBatcher` aggregates stream events and writes `ml:score:boost:{campaign_id}` (TTL 30s) directly to Redis.

---

## Data plane (summary)

| Layer | Role | Details |
| :--- | :--- | :--- |
| **L4/L7 ingress** | OpenResty :8180, optional XDP | [EDGE.md](./EDGE.md) Part I |
| **Tracker** | gnet parse + Go filters + one Lua `EVALSHA` | [GO.md](./GO.md), [EDGE.md](./EDGE.md) Part II |
| **Redis** | 4 shards, StaticSlot CRC32, tiered Lua | [EDGE.md](./EDGE.md) Part II |
| **Streams** | `ad:events:stream` per shard → processor | Below §Settlement |
| **Postgres** | Ledger truth, outbox, quotas | [DATABASE.md](./DATABASE.md) Part I |
| **ClickHouse** | Telemetry, fraud, ML features | [DATABASE.md](./DATABASE.md) Part II |

**Request path:** edge blacklist/rate limit → tracker ingress quota (UDP) → Go filters (geo, schedule, fraud, ML boost snapshot) → Lua budget/fcap/TTC → stream XADD. Money moves only inside single-shard Lua; PG sync is async.

**TTC:** Lua reads `imp_ts:{user}:{campaign}` on clicks. Default fail-open (`TTC_FAIL_CLOSED=false`); enable fail-closed after reviewing `ad_ttc_bypass_total`.

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

`sync_idempotency` + `ON CONFLICT DO NOTHING`. Details: [DATABASE.md](./DATABASE.md) Part I.

### ClickHouse idempotency

`insert_deduplicate=1` with SHA-256 block token. Windowed dedup. Details: [DATABASE.md](./DATABASE.md) Part II.

### Budget sync worker

Per shard `SyncWorker`: Lua prepare (lock + inflight), PG `UpdateSpend`, Lua commit. Dirty sets (`budget:dirty_campaigns`) drive polling.

Async sync keeps the Lua path sub-millisecond; Postgres write latency is isolated to the background worker. Trade-off: brief window where Redis budget and PG ledger diverge; recon worker detects and adjusts.

---

## Control Plane Workflows

### Transactional outbox (management)

1. Business mutation + `outbox_events` insert in one PG transaction.
2. `OutboxWorker` (20ms poll): `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1000`, status to `PROCESSING`.
3. Commit PG, execute Redis pipelines (per-shard or global replicate).
4. Batch update status to `PROCESSED`.

### Outbox polling (SKIP LOCKED)

NOTIFY buffers are process-local and lost on consumer lag; under load they increase Postgres memory pressure. Trade-off: minimum propagation latency 20 ms (poll interval).

### Pacing controller

`PacingControllerWorker` (default 5min): compares actual spend vs even/ASAP profile in PG micro-units, emits outbox invalidation to Redis pacing keys.

### Schedule worker

Minute loop: `ClaimScheduledCampaignForUpdate`, transition pause/resume, emit `PAUSE_CAMPAIGN` / `RESUME_CAMPAIGN` outbox events.

### Reconciliation

`ReconWorker` (default 1h): compares PG ledger spend vs Redis `budget:sync:*` deltas, writes `recon_discrepancies`, optional Lua atomic adjust. When `QUOTA_MODE=shadow|live`, `ReconcileQuotas` runs every 10s and releases stale `reserved_amount` on dead shards. See [EDGE.md](./EDGE.md) Part III.

### QuotaManager (`QUOTA_MODE=shadow|live`)

Drains `budget:refill_needed`, reserves chunks in Postgres, credits `budget:quota` in live mode. See [EDGE.md](./EDGE.md) Part II §5 and Part III.

### UDP control plane (`UDP_CONTROL_ENABLED`)

Management publishes per-shard ingress RPS epochs over UDP (`:8190` → tracker `:8191`). Recovery: [EDGE.md](./EDGE.md) Part III §1.1, [DEVELOPMENT.md](./DEVELOPMENT.md) (game day).

---

## Fraud scoring (cold path)

Batch fraud scoring and async enforcement for GIVT/SIVT patterns (bot farms, CTR spikes, geo-velocity). No synchronous ML inference on `/track`; trackers read precomputed Redis keys only.

### G2 dependency constraint

`go list -deps ./cmd/tracker` must not import `internal/fraudscoring`, `go-lgbm`, or `onnxruntime`. Hot path uses `GetFraudScoreBoosts()` in the fraud accumulator (`filter_layer`) — 0 allocs/op, no scorer import. Processor `MicroBatcher` writes boost keys when stream lag < 30 s.

### CAP under partition

| Path | Behavior |
| :--- | :--- |
| **ML state (AP)** | Trackers serve with last-known `ml:model:version` and threat sets; new blocks may lag |
| **Fail-closed tighten** | On epoch gap (&gt; 2× `ML_SYNC_INTERVAL_MS`), suspect-tier RL tightens only — block thresholds never loosen without signed snapshot (same policy as UDP quota in [EDGE.md](./EDGE.md) Part III §1) |
| Money (CP) | Budget, idempotency, ledger unchanged; ML does not write `budget:*` or `current_spend` |

### Data flow

| Layer | Components | Data path |
| :--- | :--- | :--- |
| Hot path | Tracker (gnet), `FraudStreamWriter`, Redis ×4 | Tracker → Redis; fraud telemetry → Redis stream |
| Warm path | Processor, ClickHouse | Redis `ad:events:stream` → Processor → ClickHouse |
| Cold path ML | `ivt-detector`, `fraud-scorer`, Postgres | ClickHouse features → scorers; `ml_model_versions` in Postgres → `fraud-scorer` |
| Control plane | Management outbox | Scorers → management → Postgres `outbox_events` → Redis all shards |

**Stages:** ingest (0 ms ML overhead) → processor lands events in ClickHouse (`ml_features_1m` MV) → batch scorer runs every `FRAUD_SCORING_SCAN_INTERVAL_MS` → management enqueues outbox → all shards updated → `SettingsWatcher` reload on tracker.

### Components

| Stage | Component | Output |
| :--- | :--- | :--- |
| Scoring | `cmd/fraud-scorer` or embedded `ivt-detector` scorer | Batch inference on `ml_features_1m` |
| Enforcement | ivt-detector ML rules → management gRPC | `ML_GHOST_IVT`, `ML_BLACKLIST_ADD` |
| Boost propagation | outbox `ML_SCORE_BOOST` or processor `MicroBatcher` | `ml:score:boost:{campaign_id}` (TTL 30s default) |
| Model sync | `FraudModelSyncOrchestrator` + `ml_model_versions` | Canary replay, per-shard cutover, rollback |
| Hot-path read | `SettingsWatcher` + fraud accumulator | Boost multiplier, L3 blacklist |

`FRAUD_SCORER_STANDALONE=true` disables embedded scorer in `ivt-detector`; scoring runs only in `cmd/fraud-scorer`.

### Redis keys (serving artifacts)

| Key | Content |
| :--- | :--- |
| `ml:model:version` | Monotonic int, matches `ml_model_versions.id` |
| `ml:model:hash` | SHA-256 of artifact bundle |
| `ml:model:applied_at` | Unix timestamp for stale-epoch detection |
| `ml:score:boost:{campaign_id}` | Temporary fraud score offset (TTL) |
| `ml:threat:ip:{bucket}` | Elevated-risk IP buckets (optional) |
| `ml:threat:asn` | SET of ASNs with elevated fraud prior |
| `blacklist:fraud` | L3 IP block (existing, replicated all shards) |

Trackers do not load model files. O(1) SET membership and atomic config reload — same pattern as `blacklist:fraud`.

### Outbox events and priority

Priority 0 (safety-critical, drains first): `UPDATE_BLACKLIST`, `ML_MODEL_VERSION`, `BUDGET_FREEZE`, `PAUSE_CAMPAIGN`, `QUOTA_REPAIR`.

Priority 1: `ML_SCORE_BOOST`, `ML_GHOST_IVT` (reversible; may lag under load).

Priority 2: bulk pacing/sync (deferred during incident).

Idempotency: `sync_idempotency` keys `ml:enforce:{ip}:{model_version}:{reason}` — claim before enqueue, release on gRPC failure.

### Ensemble scoring and enforcement tiers

Primary models: **LightGBM** (supervised, `go-lgbm` pure Go) + **Isolation Forest** (unsupervised, optional ONNX via `-tags fraudscoring_onnx`). Ensemble: `w1·lgbm_prob + w2·iforest_norm + w3·rule_score`.

| Score range | Action |
| :---: | :--- |
| 0–30 | No action (`FraudRLTierPass`) |
| 30–60 | `ml:score:boost` + suspect RL |
| 60–80 | Ghost IVT (`GhostIVTEnabled` per campaign) |
| 80–100 | Outbox `blacklist:fraud` + `fraud:quarantine` pub/sub |

Weights and thresholds are per-campaign via `CampaignFraudConfigDTO`.

### Model sync (rolling deploy)

`FraudModelSyncOrchestrator` reuses slot-migration mental model ([EDGE.md](./EDGE.md) Part III §3): one shard in `SYNC` at a time, canary replay on 10k ClickHouse events, FP ceiling check, then cutover or rollback to `V-1`. Postgres tables: `ml_model_versions` (`DRAFT` → `SYNCING` → `ACTIVE` → `RETIRED`), `ml_shard_sync_state` per shard.

### Recovery (summary)

| Fault | Hot-path impact | Recovery |
| :--- | :--- | :--- |
| ML worker crash | None — last Redis keys served | k8s restart; resume at next scan tick |
| ClickHouse down | None | Skip cycle; `fraud_scoring_errors_total` |
| Management gRPC fail | None | Release idempotency claim; retry next cycle |
| Outbox backpressure | Delayed propagation | Pause enforcement when `PENDING ≥ ML_OUTBOX_PENDING_LIMIT` |
| Model sync timeout | None on other shards | Auto-rollback to `V-1`; alert `fraud_model_sync_stuck_total` |
| ML epoch STALE | Stricter suspect RL only | Tighten via `UPDATE_SETTINGS`; restore only after hash match |

Financial recovery stays `ReconWorker` + `AssertBudgetInvariant` (±1 micro-unit) — not ML scope.

### Service score

`cmd/fraud-scorer` scores 14/18 on the microservices matrix (appendix B): independent deploy, own migrations, batch load profile; H=1 (indirect hot-path via Redis only). H=2 (ML runtime in tracker) is not permitted.

Default: `FRAUD_SCORING_ENABLED=false`. Operator runbook, env vars, training: [DEVELOPMENT.md](./DEVELOPMENT.md#fraud-scoring-cold-path).

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

### Payment service isolation

PCI scope, schema isolation, and failure containment. Trade-off: gRPC hop to management for ledger credit; payment `depends_on` management for settlement gRPC.

---

## Billing

Monthly invoice generation from Postgres ledger truth, not Redis.

### Billing service

- Schema: `billing.invoices`, `billing.invoice_lines`, `billing.customer_tax_profiles`
- `GenerateInvoice`: aggregates `balance_ledger` by type for one customer and calendar month; one invoice per `(customer_id, billing_month)` via unique index
- Tax: `customer_tax_profiles` + `TaxCalculator` (scheme/rate in basis points)
- gRPC on `BILLING_SERVER_PORT` (default 51054); `x-internal-token` metadata required

### Management integration

When `BILLING_INTERNAL_TOKEN` and `BILLING_SERVER_HOST` are set:

- `GET /admin/customers/{id}/billing` — HTMX invoice list
- `POST /admin/customers/{id}/billing/invoices` — generate invoice for `YYYY-MM`

Returns `503 BILLING_UNAVAILABLE` when the billing client is not configured.

---

## Notifier

Async outbound messaging decoupled from alert routing.

### Notifier service

- Schema: `notifier.notifications` (status `PENDING` → `SENT` | `FAILED`)
- `SendNotification`: inserts row, returns id; worker delivers asynchronously
- Providers: Telegram, Slack webhook, SMTP, SMS (configured via env; `NotifierConfigured()` gates startup paths)
- gRPC on `NOTIFIER_PORT` (default 8085); background worker polls pending rows (`NOTIFIER_WORKER_INTERVAL_MS`, default 1000ms)
- Per-provider circuit breaker (`NOTIFIER_BREAKER_*`)

### Notifier isolation

Channel credentials and retry/backoff isolated from the management HTTP pool. Trade-off: callers dial gRPC directly; `cmd/alertmanager-telegram` remains the Alertmanager webhook path.

---

## Log Broker (optional)

`cmd/broker` + `pkg/broker/`: mmap append-only segments, sparse index, CRC32-framed wire protocol, configurable durability (`async`|`group`|`sync`), Redis coordinator for leader election with fencing epochs. Ingest path stores **raw** segment bytes (no compression on the broker hot path). Not in default compose; used with `log-shipper` and optional `deploy/broker/` for HA produce/fetch and coordination chaos drills.

### mmap broker durability

Produce latency stays low; durability is explicit via `-durability` flag (default async 100 ms flush). Trade-off: `status=0` in async mode is not durable across process crash.

**Compression elsewhere:** Tracker `pkg/logger` applies async zstd + AES-GCM when rotating **tracker** segment files (`.log.zst.ready`). That pipeline is separate from `pkg/broker/` mmap logs. `log-evacuator` uploads compressed tracker artifacts to S3 with checkpointed exactly-once delivery.

---

## API Contracts

Protobuf definitions in `api/` (buf generate to vtproto):

| Proto | Package | Consumers |
| :--- | :--- | :--- |
| `events.proto` | `ads.v1` | Tracker, processor, DLQ (`AdEvent`, `AdStreamEvent`, `TrackResponse`, `AdDLQEvent`) |
| `auth.proto` | `auth` | `cmd/auth` gRPC; management auth gateway client |
| `payment.proto` | `payment` | `cmd/payment` gRPC; management `PaymentClient` |
| `settlement.proto` | `settlement` | Management settlement gRPC; payment outbox client |
| `billing.proto` | `billing` | `cmd/billing` gRPC; management `BillingClient` |
| `notifier.proto` | `notifier` | `cmd/notifier` gRPC |

---

## Database Schemas

Postgres topology:

| Instance | Compose service | Port (default) | Schemas |
| :--- | :--- | :--- | :--- |
| Core | `db` | 5430 | `public` (ads), `auth`, `billing`, `notifier` |
| Payment | `db-payment` | 5431 | `payment` only |

`cmd/payment` connects via `PAYMENT_DB_DSN` (falls back to `DB_DSN` when unset). Migrations run on payment startup from embedded `internal/payment/migrations/`.

| Migration tree | Database | Domain |
| :--- | :--- | :--- |
| `internal/ingestion/migrations/` (41 files) | `db` | Campaigns, events (partitioned), ledger, outbox, quotas, slot map, ML enforcement, control-plane epochs |
| `internal/processor/migrations/` | `db` | ClickHouse stream DDL (`ml_features_1m`) |
| `internal/auth/migrations/` (8 files) | `db` | Users, sessions, API keys |
| `internal/payment/migrations/` (2 files) | `db-payment` | Payment intents, webhooks, outbox |
| `internal/billing/migrations/` (1 file) | `db` | Invoices, invoice lines, customer tax profiles |
| `internal/notifier/migrations/` (1 file) | `db` | Notification queue and delivery status |

Money columns: BIGINT micro-units (migration 00020). Ledger type `PAYMENT_TOPUP` + `payment_intent_id` unique index (00024, 00025).

ClickHouse (`deploy/clickhouse/init.sql`): `impressions`, `clicks`, `conversions`, `fraud_events`, recon MVs, `ml_features_1m`. See [DATABASE.md](./DATABASE.md) Part II.

---

## Observability and Alerts

Prometheus (`deploy/monitoring/prometheus.yaml`) scrapes:

| Job | Targets | Metrics path |
| :--- | :--- | :--- |
| `tracker` | :8181–8184 | `/metrics` and `/health` on gnet `SERVER_PORT` (Prometheus default in compose) |
| `processor` | :8186 | `/metrics` |
| `management` | :8188 | `/metrics` |
| `auth` | :9091 (`AUTH_METRICS_PORT`) | `/metrics` |
| `payment` | :8187 (`PAYMENT_WEBHOOK_PORT`) | `/metrics` on webhook mux |
| `billing` | :51054 (`BILLING_SERVER_PORT`) | gRPC only (no HTTP metrics yet) |
| `management` | :8188 (`MANAGEMENT_PORT`), :51053 (`SETTLEMENT_SERVER_PORT`) | `/metrics` on admin port; settlement gRPC only |
| `notifier` | :8085 (`NOTIFIER_PORT`) | gRPC only |

Rule file: `deploy/monitoring/prometheus.rules.yaml`. Grafana provisioning under `deploy/monitoring/grafana/`.

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
| UDPControlStale | `ad_udp_control_stale_total` rate >0 with `UDP_CONTROL_ENABLED` |
| ProcessorStreamLagHigh | `ad_processor_stream_lag_seconds` >30 (pauses ML micro-batch) |
| FraudModelVersionStale | Active model epoch >24h without sync |

Telegram proxy (`cmd/alertmanager-telegram`): Alertmanager webhook to HTML message to Bot API.

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
| Shard resize | StaticSlot remaps ~85% keys on N+1; blue/green + `scripts/redis-ops/redis_migrate_campaign.sh` required |

### Non-goals

Redis Cluster (Sentinel covers failover); JumpHash on tracker (~84% divergence with StaticSlot management); removing gnet (perf gate dependency).

---

# Appendix A — Design patterns

Transactional Outbox, Pinned Worker Pool, Circuit Breaker, SettingsWatcher, and budget sync — request lifecycles.

## 1. Transactional Outbox

When an admin changes campaign parameters (e.g. daily budget), the system must persist to Postgres and update the Redis hot cache. Writing to both stores directly risks divergence on network or node failure.

**Mutation lifecycle:**
1. Admin UI POST → Management API starts a Postgres transaction, updates `campaigns`, inserts `outbox_events` with status `PENDING`, commits.
2. `OutboxWorker` polls every 20 ms (`SELECT … FOR UPDATE SKIP LOCKED`), marks batch `PROCESSING`, pipelines HSET/SET to all Redis shards, marks `PROCESSED`.

## 2. Pinned Worker Pool

At high RPS, random goroutine scheduling evicts L1/L2 cache lines. Each CPU core should process a stable subset of campaigns.

**Hot-path lifecycle:**
1. gnet reads TCP via `epoll` (inbound socket).
2. DFA HTTP/1.1 scanner extracts the request (zero alloc).
3. Hash `campaign_id` → MPSC ring buffer for a pinned worker (64-byte cache-line padding).
4. Pinned worker runs `FilterEngine.Check`, then Redis/memory operations on the same core.

## 3. Circuit Breaker

External dependency failure (Notifier SMS/email, Redis shard) can block connection pools and cascade.

**States** (`REDIS_BREAKER_FAIL_THRESHOLD = 150`, open timeout 5 s):

| State | Entry | Exit |
| :--- | :--- | :--- |
| Closed | Initial / recovered | Error count ≥ threshold → Open |
| Open | Threshold exceeded | 5 s elapsed → Half-Open |
| Half-Open | Open timeout | Probe success → Closed; probe fail → Open |

## 4. SettingsWatcher

Hot-path services cannot query Postgres per request; operators still need instant global toggles.

**Lifecycle:** Admin writes `config:values` + bumps `config:version` in Redis (shard 0). `SettingsWatcher` polls version every 5s; on change, loads hash and swaps pointer via `atomic.Value`. Filters read lock-free from memory.

## 5. Budget sync (SyncWorker & reconciliation)

Spend is decremented in Redis Lua for sub-ms latency; Postgres ledger updates async.

**Lifecycle:** Lua increments `budget:sync:campaign:ID` and adds ID to `budget:dirty_campaigns`. `SyncWorker` `SPOP`s dirty IDs, writes `balance_ledger` in Postgres, then Lua subtracts committed delta from Redis (preserving concurrent increments).

---

# Appendix B — Microservices topology

Service boundaries by hot-path isolation, compliance (PCI DSS), and blast radius.

## B.1 Hot-path vs cold-path

- **Hot-path (tracker ×4, processor):** No blocking DB I/O, external APIs, or heavy SDK GC on ingest. Parse, filter via Redis/local cache, emit raw events.
- **Cold-path:** gRPC/HTTP, Postgres, tens-of-ms latency — idiomatic Go.

## B.2 Database: schemas vs instances

One Postgres + one ClickHouse for most services; isolation via schemas (`public`, `auth`, `payment`, `billing`, `notifier`). `payment` uses dedicated `db-payment` for PCI audit. Effects: lower dev/k3s overhead; local FK integrity without 2PC.

## B.3 Service scoring matrix (0–2 per criterion)

| Criterion | 0 | 1 | 2 |
| :--- | :--- | :--- | :--- |
| **H** Hot-path isolation | No hot-path tie | Indirect Redis keys | Direct tracker risk |
| **E** External ingress | Internal only | Rare outbound | Public webhooks/OAuth |
| **S** Secrets/compliance | None | DSN | PCI, Stripe, SMS tokens |
| **F** Blast radius | Isolated failure | Admin-only impact | Cascade/OOM risk |
| **L** Load profile | Admin-like | Periodic peaks | Polling/heavy analytics |
| **C** Caller count | 0–1 | 2 | Many gRPC dependents |
| **D** Data ownership | Shared tables | Own tables, shared schema | Own schema + migrations |
| **O** Operational independence | Host-bound | Optional in dev | System runs without it |
| **T** Team lifecycle | Single release | Shared release | Independent deploy |

**Score:** 0–5 → package in `internal/`; 6–10 → modular monolith; 11+ → `cmd/<name>` with gRPC port and `x-internal-token`.

## B.4 Current topology

| Service | Score | Rationale |
| :--- | :---: | :--- |
| **management** | 10/18 | Admin hub, outbox, settlement gRPC co-located to avoid extra hop |
| **auth** | 14/18 | PASETO/API secrets, independent scale |
| **payment** | 16/18 | Stripe webhooks :8187, PCI, dedicated Postgres |
| **billing** | 11/18 | Heavy ledger aggregation, schema `billing` |
| **notifier** | 12/18 | External Telegram/Slack/SMTP latency isolated |
| **ivt-detector** | 7/18 | Batch ClickHouse scan, no RPC; pushes blocks via management HTTP |
| **log-evacuator** | 8/18 | Node-local log rotation, no DB |
| **fraud-scorer** | 14/18 | Cold-path batch scorer; see [Fraud scoring](#fraud-scoring-cold-path) |
