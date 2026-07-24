# Data Layer

Redis (hot state), PostgreSQL (ledger, config), ClickHouse (telemetry). Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md). Open gaps: [BACKLOG.md](./BACKLOG.md).

---

## Part I — Redis

## 1. Topology

*   **Shard count:** 4 (Standalone Master + Replicas + Sentinel x3).
*   **Model:** Isolated masters without Redis Cluster. This avoids `MOVED` / `CROSSSLOT` redirects. Each `EVALSHA` command targets exactly one master.
*   **Failover:** Sentinel quorum — 2. Failure detection takes ~5s; promotion to a replica takes ~10–15s.
*   **Circuit Breaker:** Opens after 150 consecutive errors; transitions to half-open after 5s.
*   **Routing:** `campaign_id` → `CRC32C & 1023` (slot) → `rdbs[shard]`. All keys in a single Lua request must belong to one shard.

---

## 2. Global and Local Replication

### Shard 0 (Global State)
Used for shared state:
*   Registry update notifications (Pub/Sub) — primary path; broker fallback opt-in (`CAMPAIGN_UPDATE_BROKER_FALLBACK`).
*   User lockout markers and session revocation.
*   Creative structures (also fan-out to all shards via outbox).

### Global Keys (Replicated)
Data copied to all shards via `outbox` / `redis_global.go` (M14-01):
*   Configuration values (`config:values` / `config:version`).
*   Blacklists (`blacklist:manual|auto|fraud`).
*   Fraud-score boosts (`ml:score:boost:{campaign_id}`).
*   Placement pause hashes (`{uuid}blacklist:placement:{uuid}` — not `placement:blocklist:*`).
*   Brand creatives.

Tracker reads **local shard copies** when shard 0 is circuit-open (SettingsWatcher prefers shards 1..N; Go L3/placement filters use `pickLocalGlobalShard`).

### Shard-0 ingest blast radius (M14-04)

| Campaign home | During shard-0 outage |
| :--- | :--- |
| StaticSlot → shards 1–3 | Unaffected (debit on home shard) |
| StaticSlot → shard 0, no `campaign_routing` triplet | Fail-fast `503 shard_unavailable` — never silent zombie accept |
| StaticSlot → shard 0, HasTriplet | Reroute debit to healthy reserve / primary A/B from `campaign_routing` |
| Registry unknown + stale-serve | `503 registry_stale` |

Control-plane keys (pub/sub) remain shard-0 SPOF until Sentinel promote (~10–15 s) or broker fallback.

### Local Keys (Shard-Local)
Data stored strictly on one shard:
*   Campaign budgets (`{uuid}budget:campaign:{uuid}`), quotas (`{uuid}budget:quota:{uuid}`).
*   Deduplication data (`{uuid}dup:*`, `{uuid}idempotency:*`), impression timestamps (`{uuid}imp_ts:*`).
*   Campaign-hash-tagged ingress counters (`{uuid}ingress:day:*`) — consolidated into Lua (M9-02).
*   Event streams (`ad:events:stream`).
*   Migration barriers (`budget:migration_fence:{uuid}`) — source shard only during COPY (default path).
*   Dual-write delta stream (`slot_migration:delta`) — source shard during hot-slot migration when `SLOT_MIGRATION_DUAL_WRITE_ENABLED=true`.

### CampaignRedisKeyCatalog (M1)

`internal/ingestion/redis_key_catalog.go` is the single source for slot-migration COPY/DRAIN key lists. Hash-tagged keys colocate per campaign (HR-KEYS). The migrator, PG re-warm cutover, and [DEVELOPMENT.md](./DEVELOPMENT.md) rollback playbook all reference this catalog.

---

## 3. Lua Scripts

### Processing Tiers
*   **Tier B (`budget-fast.lua`):** For impressions. Budget debit, consolidated pre-checks (fraud blocklist signal, placement blocklist, daily ingress quota), and stream write in one `EVALSHA`. Skips fcap and pacing. Default on (`LUA_FAST_PATH_ENABLED=true`).
*   **Tier C (`unified-filter.lua`):** For clicks and impressions that need fcap, even pacing, TTC, or quota-refill probes. Same consolidated pre-checks. IP rate limits are **not** in Lua (M9-03) — edge XDP PPS and nginx `limit_req` only.
*   **Refill (`local-quota-refill.lua`):** Cold path only. Atomically debits a chunk from `budget:quota` into the tracker's `LocalQuantaLedger`.
*   **Tier degradation (M9-04):** When the filter monotonic deadline has &lt; 2 ms remaining, Tier C skips non-critical gates inside Lua (fcap, pacing, TTC, imp_ts write, quota-refill side effects) and returns code `20`. Metric: `filter_tier_degraded_total`.

### Elastic triplet 40/40/20 (canary only)

Per-campaign **40/40/20** routing is a **canary migration mode** during elastic triplet cutover — not steady-state. See §7. Do not enable triplet routing for new campaigns outside orchestrator-driven migrations.

### Constraints
Lua scripts must use only non-blocking commands (`GET`, `SET`, `INCR`, `XADD`). Execution time (p99) must be < 10 ms per shard.

---

## 4. Script Lifecycle

*   **Load:** Scripts are embedded in the tracker binary. On startup, `SCRIPT LOAD` runs on all shards.
*   **Execution:** The hot path uses `EVALSHA`. If the script is missing (`NOSCRIPT`), it falls back to sending the full script body via `EVAL`.
*   **Sticky eval pins:** The tracker opens one `redis.Conn` per pinned worker × shard (`redis_eval_pin.go`). Filter `EVALSHA` runs on the worker's sticky conn to skip go-redis pool `connCheck` allocations. `FilterWorkerIdx` on the event selects the row (set only on worker-pool offload). Unset or out-of-range worker IDs fall back to the pooled `UniversalClient`. Dead conns reopen once per eval on retryable errors (`EOF`, `closed`, `bad state`). `ConnectRedisShards` reserves `StickyPinWorkers` (`MAX_WORKERS`) extra pool slots per shard. Shutdown: `CloseFilterEvalPins()` before closing shard clients.
*   **Risks:** Script eviction (`SCRIPT FLUSH`) or Redis restart under load causes latency spikes from mass script-body resubmission.

---

## 5. Lua Validation Risks

### P0 Risks (Security and Finance)
*   **R-LUA-01 (TOCTOU):** State between the Go check and Lua execution may change. Resolved by atomicity inside Lua.
*   **R-LUA-03 (Double debit during migration):** Risk of active keys on both old and new shards. Resolved by migration fence (default) or dual-write delta replication + lag catch-up (hot slots).
*   **R-LUA-04 (Master thread blocking):** Long-running Lua operations increase latency for all requests to that shard.

### P1 Operational Risks
*   **R-LUA-08 (NOSCRIPT):** Loss of preloaded scripts on restart.
*   **R-LUA-09 (Slot drift):** Mismatched slot-map update timing between tracker and edge.

---

## 6. Fail Policy

*   **GeoIP / Blacklists (Tracker):** Fail-open (allow traffic on error).
*   **TTC (click check):** Configurable (`TTC_FAIL_CLOSED`); default is fail-open.
*   **Blacklists (Edge):** Fail-closed (block, HTTP 503).
*   **Redis Circuit Breaker:** Fail-closed (HTTP 503).
*   **Lua error / Filter timeout:** Fail-closed (no debit or impression).

---

## 7. Elastic Triplets (opt-in dynamic sharding)

Per-campaign triplet routing with PostgreSQL control plane (`campaign_routing`, global `routing_epoch`), capacity-aware `ShardOrchestrator`, and TCP snapshot + HMAC-SHA256 + tracker ACK cutover. **Prerequisite:** [DEVELOPMENT.md](./DEVELOPMENT.md) §Slot migration (M1). Summary: [CAPABILITIES.md](./CAPABILITIES.md) §M2.

Production default remains fixed `StaticSlot` (N=4). Enable via env flags below.

### Control plane

| Component | Detail |
| :--- | :--- |
| `campaign_routing` | Per-campaign home slot + primary A/B + reserve shards + `routing_epoch` — `00052_campaign_routing.sql` |
| Global epoch | `redis_slot_map_meta.routing_epoch` → `StaticSlotSharder.MigrationGen` on reload |
| Hot-path triplet | 40/40/20 split on composite hash (`unified_filter.go`, `budget_fast.go`) |
| Lua fence | `LuaRoutingEpoch()` = max(`routing_epoch`, `migration_gen`) in budget-fast / unified-filter ARGV |

Micro-migration reuses M1 `CampaignKeyMigrator` COPY/DRAIN and `BumpMigrationFences` so `AssertBudgetInvariant` is preserved.

### Shard orchestrator

`ShardOrchestrator` (`internal/management/shard_orchestrator.go`) EWMA-tracks shard capacity; when a shard stays above threshold, it migrates the hottest campaign to the least-loaded peer:

1. Upsert `campaign_routing` with bumped `routing_epoch`
2. Bump `campaigns.migration_gen` + fence source shard
3. COPY → DRAIN campaign keys
4. `BumpGlobalRoutingEpoch` + broker/TCP cutover publish

Enable with `SHARD_ORCHESTRATOR_ENABLED=true`.

### TCP routing cutover (GAP-SHARD-05)

| Role | Port / env | Behavior |
| :--- | :--- | :--- |
| Management | `TCP_MGMT_BIND_ADDR` (`:8192`) | HMAC-signed `TCPMsgSnapshot`; records tracker ACK |
| Tracker | `TCP_MGMT_ADDR` | Pulls snapshot on poll interval; ACKs applied epoch |

Frame format: `internal/ingestion/tcp_control_codec.go`. Shared secret: `TCP_CONTROL_HMAC_SECRET`.

### Configuration

| Env | Default | Purpose |
| :--- | :--- | :--- |
| `ELASTIC_SHARDING_ENABLED` | `false` | Feature flag |
| `SHARD_ORCHESTRATOR_ENABLED` | `false` | Background orchestrator |
| `TCP_CONTROL_ENABLED` | `false` | TCP cutover plane |
| `TCP_CONTROL_HMAC_SECRET` | — | Required when TCP enabled |

### Verification

```bash
go test ./internal/management/... -run 'SO_|TCP_Snapshot' -short
```

**Chaos:** `TestChaos_SO_NoFalseMigrate`, `TestChaos_SO_CampaignRoutingMigration`, `TestChaos_TCP_SnapshotHMACACK`.

**Closed:** [GAP-SHARD-04](./BACKLOG.md) — shard-0 pub/sub outage survival (M14-01..05). See [DEVELOPMENT.md](./DEVELOPMENT.md) §Shard-0 outage.

---

## Part II — PostgreSQL

PostgreSQL is the primary source of truth for financial operations, user accounts, and campaign configuration. Writes are optimized for strict consistency and minimal locking.

## 1. Balances and Ledger (Micro-Units)

All financial amounts are stored as `BIGINT` in **micro-units** (1 unit = 1,000,000 micro-units). This avoids rounding errors when using `INCRBY` in Lua and aggregating in Postgres.

### `balance_ledger`

Immutable append-only records of every balance change. Current balance is derived from ledger sums (or `customers.balance` maintained by sync), not ad-hoc mutable floats.

**Write amplification:** each FEE row touches `idx_ledger_customer`, `idx_ledger_campaign`, partial `idx_ledger_fee_created`, partial `idx_ledger_topup_recent`, and optional `idx_ledger_payment_intent`. Plan for `VACUUM (ANALYZE)` and monitor `pg_stat_user_tables.n_dead_tup` on ledger and `campaigns`.

## 2. Idempotency Strategy

The system uses a **claim → side effect → ack** pattern. On retry, the side effect is not executed again.

### Idempotency Key Catalog

| Layer | Store | Key Generation | Problem Solved |
| :--- | :--- | :--- | :--- |
| **Hot `/track`** | Redis | `idempotency:click:` + `click_id` | Prevent double debit on client retries |
| **Hot `click_id`** | Tracker RAM | `NewFastUUID()` (ts + seq + nodeID) | Generate unique IDs without a global coordinator |
| **Budget Sync (D3 v2)** | Postgres | `dedup_claim_confirm` → `sync_idempotency(id = dedup_key)` | Deterministic SSID + payload hash; survives worker restart |
| **Budget Sync (legacy)** | Postgres | UUID v4 per batch (`budget:txid:*` in Redis) | Superseded by D3 when `DedupAdapter` wired |
| **Region relay (D3 v2)** | Postgres + Redis NX | Per-event SSID + `dedup/v2:{dedup_key}` | Claim-before-apply for outbox delivery |
| **Stream → PG** | Postgres PK | `(click_id, created_date)` | Prevent duplicate rows on stream redelivery |
| **Broker → PG (D3 v2)** | Postgres | Batch SSID from partition offsets + click_id hash | Broker redelivery without duplicate events |
| **Stream → CH** | ClickHouse | `insert_deduplication_token` per batch | CH path keeps token dedup (no D3) |
| **Admin API** | Postgres | `SHA256(customer_id + canonical_json(body))` | Idempotent administrative HTTP requests |
| **Payments** | Postgres | Client `idempotency_key` | Prevent duplicate transactions |
| **Quota / IVT** | Postgres | Prefix keys (`quota:`, IVT sync) | **Out of D3 scope** — separate idempotency namespaces |

## 3. Transactional Outbox (`SKIP LOCKED`)

The Outbox pattern synchronizes configuration and side effects to Redis.

Workers use `SELECT … FOR UPDATE SKIP LOCKED` inside a **single transaction** that also marks rows `PROCESSING` or `PROCESSED`. Parallel workers claim disjoint row sets without blocking.

**Rules:**

- Never run `FOR UPDATE` outside an explicit transaction — locks are released at statement end in autocommit mode.
- Claim and status transition must share one `BEGIN … COMMIT` boundary.
- Use `SKIP LOCKED`, not blocking `FOR UPDATE`, for worker queues.
- Poll interval must be configurable; default **20 ms** in management (`service.go`) is aggressive — consider **100–250 ms** when idle to cut WAL churn.

### Reconciliation authority (M3)

| Layer | Source of truth | Used for |
| :--- | :--- | :--- |
| Hot debit | Redis Lua (`budget:campaign` / `budget:quota`) | Real-time accept/reject |
| In-flight settlement | Redis `budget:sync` + `budget:inflight` | Pending PG flush |
| Local quanta (M8) | Tracker `LocalQuantaLedger` + broker `budget-deltas` | Amortized debit; recon via `BrokerPendingDeltaReader` |
| Quanta return (M14) | `local-quota-return.lua` + broker negative delta | Pause, SIGTERM, strict-enter flush unused RAM chunk back to `budget:quota` |
| Financial ledger | Postgres `balance_ledger` + `campaigns.current_spend` | Billing, pause, admin |
| Corrections | Outbox (`QUOTA_REPAIR`, `RECONCILIATION_ADJUST`) only | Never direct Redis `SET` under load |

Per-campaign invariant (QUOTA_MODE off):  
`budget_limit - current_spend_pg ≈ budget_remaining_redis + budget_sync_redis + budget_inflight_redis + broker_pending_deltas`  
Tolerance ε = max(1 micro-unit, 0.01% of `budget_limit`). Grace window = `LEDGER_BATCH_FLUSH_MS + BUDGET_SYNC_INTERVAL_MS` while `inflight > 0`.

## 4. Partitioning

The `events` table is partitioned by month (`internal/database/partition_manager.go`):

- Compact index size per child partition.
- Fast removal via `DROP PARTITION`.
- Partition pruning on time-bounded queries.

## 5. Transaction Isolation

All application code uses the Postgres default **Read Committed** (no explicit `Serializable` / `Repeatable Read`).

| Pattern | Where | Rule |
| :--- | :--- | :--- |
| Row locks | `GetCampaignForUpdate`, `GetCustomerForUpdate` | Use inside short transactions; avoid external I/O while holding locks |
| Transaction-scoped advisory lock | `pg_advisory_xact_lock` in autoscale | Released automatically at commit — preferred over session locks |
| Session advisory lock | `CreatePaymentIntent` | **Anti-pattern:** holds pool connection across `CreateCheckout()` HTTP — use txn-scoped lock or idempotency-only path |
| Unique constraints | `balance_ledger.idempotency_hash`, `sync_idempotency` | Primary correctness backstop under Read Committed |

**DDIA rule:** prefer **single-object atomic writes** (one row or one txn) over distributed transactions. Cross-service effects go through the outbox.

## 6. Index and Bloat Discipline

| Table | Hot update columns | Risk |
| :--- | :--- | :--- |
| `campaigns` | `current_spend`, `updated_at`, `status` | Every sync flush rewrites heap + `idx_campaigns_pacing`, `idx_campaigns_cust_status`, `idx_campaigns_draining_updated` |
| `outbox_events` | `status`, `processing_started_at` | High churn from 20 ms polling + claim batches |
| `campaign_stats` | counter columns via `ON CONFLICT DO UPDATE` | Row-level contention on busy campaigns |
| `balance_ledger` | append-only | Index bloat from volume, not UPDATE |

**Rules:**

- Add partial indexes only when `EXPLAIN (ANALYZE, BUFFERS)` proves seq scan at expected row counts (`00048_explain_audit_indexes.sql`).
- Do not index columns that change on every hot-path write unless the read path is SLA-critical.
- Run `EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit` before adding indexes.
- Schedule `VACUUM` on `balance_ledger`, `outbox_events`, `campaigns`; watch `pgstattuple` or `n_dead_tup` in production.

---

## Part III — ClickHouse

ClickHouse stores telemetry in near real time. Writes use large batches to minimize part count (LSM-style merge tree).

## 1. Table Engines

Production raw events: `ReplacingMergeTree(created_at)` (or `ReplicatedReplacingMergeTree`):

- Sort key: `ORDER BY (campaign_id, CreatedAt, ClickID)`.
- Partitioning: `toYYYYMM(CreatedAt)`.
- Duplicate collapse: eventual, on background merge — not at read time unless `FINAL`.

SummingMergeTree rollups (`campaign_hourly_*`, `placement_stats_hourly`, `audit_log_rollups`, `fraud_aggregate_spikes`) use monthly partitions where configured.

## 2. Batch Inserts

Processor path (`internal/ingestion/clickhouse_store.go`, `cmd/processor/main.go`):

- Default `CH_BATCH_SIZE=50000`, flush every `CH_FLUSH_INTERVAL_MS` (10 s).
- Up to **five** `PrepareBatch` calls per flush (`impressions`, `clicks`, `conversions`, `fraud_events`, `fraud_aggregate_spikes`).
- Each insert triggers **materialized view fan-out** (campaign hourly, ML features, placement stats).

**Rules:**

- Never insert single rows on the hot path — batch or use server `async_insert` buffer.
- Cap poison-pill decomposition: `processor.go` falls back to per-event `StoreBatch` on non-retriable errors — monitor and alert; prefer DLQ over 50k single-row inserts.
- Tune `PROCESSOR_CH_GATE_SLOTS` with `CH_MAX_CONNS` so shard consumers do not stampede.

## 3. Async Insert Settings

`ConnectClickHouse` (`internal/database/clickhouse_connect.go`):

```go
async_insert: 1
wait_for_async_insert: 0
```

**Trade-off:** client returns before server flush — lower latency, higher risk of invisible failures and many small parts under overload.

**Rules:**

- Production processor: set `wait_for_async_insert=1` or `async_insert_busy_timeout_ms` unless you monitor `system.asynchronous_inserts` and `system.parts`.
- **Reads must use `ConnectCHReadonly`** (`CH_READONLY_DSN`) — write connection must not serve management/IVT/report queries. `ConnectCHReadonly` today reuses write settings; split DSNs in deployment.
- Route cold-path queries through `CHQuery` (`internal/database/chquery.go`) — `readonly=1`, `max_memory_usage`, `max_execution_time`.

## 4. Parts, Merges, and Disk I/O

| Mechanism | File | Rule |
| :--- | :--- | :--- |
| Partition janitor | `ch_partition_janitor.go` | Drops old monthly partitions; `OPTIMIZE TABLE … PARTITION … FINAL` off-peak when `parts >= CH_RECOMPRESS_PARTS_THRESHOLD` (default 8) |
| ZSTD codec | `00006_raw_zstd_codec.sql` | Applied on merge — fragmented partitions stay large until janitor runs |
| `ALTER TABLE … DELETE` | `service_erasure.go` | **Heavy mutation** — blocks merges; prefer TTL/tombstone columns for GDPR where legal allows |
| MV `uniqExact` | `00003_ml_features_1m.sql` | **Expensive on insert path** — prefer `uniqCombined` or pre-aggregate outside MV |

**Tanenbaum rule:** bound concurrent writers (gate semaphores, shard count) to avoid thrashing the merge scheduler.

Monitor:

- `system.parts` per table/partition (alert when active parts > 100 per partition).
- `ad_ch_janitor_recompress_total`, `ad_processor_write_acquire_wait_seconds{backend=clickhouse}`.
- Disk usage; janitor emergency drop at `CH_EMERGENCY_DROP_PERCENT`.

## 5. Materialized Views

Aggregates for reconciliation and ML:

- `mv_campaign_hourly_impressions` / `clicks` / `conversions`
- `mv_ml_features_1m_*` → `ml_features_1m` (no partition — add `PARTITION BY toYYYYMM(window_start)` at scale)
- `mv_placement_stats_*`

**DDIA rule:** MVs are **derived data**. Postgres ledger remains authoritative for money; CH is rebuildable from streams/archives.

## 6. ClickHouse System Logs

`deploy/clickhouse/config.yaml` disables `query_log`, `trace_log`, `metric_log`, `text_log` — correct for IOPS. Do not re-enable on production without dedicated disk.

---

## Part IV — Reliability and Durability

## Durability Boundaries

1. **Postgres.** Sole source of truth for finance and accounts. Requires: sync standby, WAL archiving (PITR), async DR replica.
2. **Redis.** Ephemeral hot state. Protection: Sentinel, AOF, backups.
3. **ClickHouse.** Derived telemetry. Rebuildable from Postgres `events` partition archive or Redis stream replay (expensive).

### End-to-End Rule

A Redis stream message is `XAck`'d only after persistence in long-term storage (Postgres commit or ClickHouse batch acknowledgment / WAL spool in `clickhouse_store.go`).

### Single-Site Postgres HA (G1)

**Topology:** Primary in AZ-a; synchronous standby in AZ-b (`synchronous_commit = remote_apply`). WAL archive to object storage every 60 s.

**Failover:** Promote standby → repoint `DB_DSN` on processor, management, payment → verify `pg_is_in_recovery()` false → smoke `AssertBudgetInvariant`.

**RTO:** ≤ 120 s. **RPO:** 0 on sync path.

### Write-path durability (shipped)

| ID | Status | Mechanism |
| :--- | :--- | :--- |
| D0/D1 | Done | `ClickHouseStore.StoreBatch` blocks until CH `batch.Send()`; `StreamConsumer` XAcks only after `StoreBatch` returns nil |
| D2 | Done | Rotating mmap WAL (`events.wal` + `events.wal.NNNN`), lazy FD, `CH_SPOOL_*` env; `RecoverSpool` on startup |
| D4 | Done | PG/CH pool limits + `ProcessorPgGate` / `ProcessorChGate`; `pauseStreamReads` backpressure |
| H1 | Done | `SyncWorker` sole `UpdateCampaignSpend` writer; `syncMu` serializes flushes |
| B2 | Done | `TRACKER_PG_FALLBACK=0` in production; `UnifiedFilter.SetPGFallbackAllowed` |
| G1 | Documented | Single-site sync standby + WAL archive runbook above |

**Deferred:** separating `XADD` from `unified-filter.lua` — bench evidence shows an extra Redis round-trip would dominate shard lock time. Stream writes stay atomic with budget debit.

**Open backlog (cold path):**

| ID | Target | Notes |
| :--- | :--- | :--- |
| SEM-P3 | Management PG pool | `mgmtPgSem` on background workers |
| SEM-P4 | Per-campaign spend mutex | Only if a second `current_spend` writer is added |
| INST-P1 | Interactive installer | `espx-install` wizard for first-run `.env` |
| M1 | Probabilistic budget reconciliation | Optional tuning beyond M3 `ReconWorker` |

---

## Part V — Known Bottlenecks

Severity for production at scale. Seq scans on small tables are not listed.

## PostgreSQL — High

| Issue | Location | Impact | Mitigation |
| :--- | :--- | :--- | :--- |
| Dual PG+CH stream consumers | `cmd/processor/main.go` | 2× persistence per event when PG consumer enabled | Disable PG consumer if CH is canonical for events; or batch larger |
| Per-campaign sync commits | `sync_worker.go` → `campaign_repo.UpdateSpend` | N txns per flush window × active campaigns | Already rolled up (M12); consider multi-campaign batch txn |
| `admin_audit_log` on every ledger flush | `campaign_repo.go` `LEDGER_BATCH_FLUSH` | PG INSERT ∝ campaigns × flush rate | Sample, aggregate, or ship to CH/broker like `writeAuditLog` |
| `FOR UPDATE` outside txn | `payment/crypto_hold_worker.go` | Race: duplicate hold processing | Wrap claim+process in one `BeginFunc`; see fix below |
| Advisory lock over HTTP | `payment/service.go` `CreatePaymentIntent` | Pool exhaustion, serializes creates | `pg_advisory_xact_lock` inside txn after idempotency check; call provider after commit |

## PostgreSQL — Medium

| Issue | Location | Impact | Mitigation |
| :--- | :--- | :--- | :--- |
| 20 ms outbox poll | `management/service.go` | Idle WAL/fsync pressure | Back off to 100–250 ms; `LISTEN/NOTIFY` optional |
| 32-way parallel customer sync | `sync_worker.go` | WAL spikes | `ProcessorPgGate` already caps processor; tune `maxConcurrency` |
| Long pacing txn | `service_pacing.go` | Holds `FOR UPDATE` on many campaigns | Shorter txns; batch pacing in memory then single UPDATE |
| Full `campaigns` scan hourly | `volume_meter_worker.go` | O(campaigns) read | Cursor pagination or incremental watermark |

## ClickHouse — High

| Issue | Location | Impact | Mitigation |
| :--- | :--- | :--- | :--- |
| `wait_for_async_insert=0` | `clickhouse_connect.go` | Part explosion, silent write loss | `wait_for_async_insert=1` + alerts |
| MV `uniqExact` on insert | `00003_ml_features_1m.sql` | CPU per block on every impression/click | `uniqCombined` or async rollup job |
| Poison-pill single-row fallback | `processor.go` | Part storm on bad batch | DLQ + metric `ad_ch_single_row_inserts_total` |
| Per-row `ml_shadow_scores` | `ivtdetector/fraud_scoring_rule.go` | Many parts per IVT scan | `PrepareBatch` |
| Read path uses write DSN | `cmd/management/main.go`, `cmd/ivt-detector/main.go` | Async insert on read conn; no `CHQuery` caps | Wire `CH_READONLY_DSN` + `CHQuery` everywhere |

## ClickHouse — Medium

| Issue | Location | Impact | Mitigation |
| :--- | :--- | :--- | :--- |
| `ALTER DELETE` erasure | `service_erasure.go` | Mutation backlog | Tombstone + TTL |
| Raw-table scans bypass `CHQuery` | `service_mab.go`, `ivtdetector/analyzer.go`, `marginguard/worker.go` | Unbounded memory | Route through `CHQuery` |
| Janitor 1 OPTIMIZE/partition/day | `ch_partition_janitor.go` | Slow ZSTD catch-up | `CH_JANITOR_MAX_RECOMPRESS_PER_RUN` |
| `ml_features_1m` unpartitioned | `00003_ml_features_1m.sql` | Wide merges | Add monthly partition + TTL |

## Useless or Misplaced Disk Writes

| Write | Verdict |
| :--- | :--- |
| `LEDGER_BATCH_FLUSH` → `admin_audit_log` every sync flush | **Remove or sample** — duplicates stream/CH telemetry |
| CH `query_log` / `text_log` | **Disabled** — keep off |
| Hot-path `writeAuditLog` → broker/logger | **Correct** — sampled, not PG |
| `control_plane_epochs` INSERT per epoch | Low frequency — acceptable |
| Per-event outbox `PROCESSED` UPDATE | Necessary for at-least-once — batch mark where safe |

---

## Part VI — Implementation Rules

## PostgreSQL

1. **Finance writes:** one logical mutation per txn when possible; idempotency key before side effect.
2. **Locks:** `FOR UPDATE` only inside `Begin`/`BeginFunc`; never across network calls.
3. **Outbox:** claim + mark in same txn; `SKIP LOCKED` for workers.
4. **Indexes:** justify with `EXPLAIN (ANALYZE, BUFFERS)` at seeded scale (`explain_audit_test.go`).
5. **Audit:** hot path → broker/CH sample; cold path → `admin_audit_log`; never per-flush PG audit on sync worker without sampling.
6. **Isolation:** stay Read Committed; use constraints + row locks, not Serializable, unless proven necessary.
7. **Pool:** size for peak `(sync workers + outbox pollers + API)`; advisory locks hold connections.
8. **Partitions:** drop old `events` children via janitor, not `DELETE`.

## ClickHouse

1. **Batch size:** ≥ 10k rows or ≥ 5 s window on hot path (current default 50k / 10 s).
2. **Parts:** alert `system.parts` > 100 per partition; never sustained single-row inserts.
3. **MVs:** no `uniqExact` on insert-triggered MVs; count MVs per raw insert (currently ~8 aggregate paths).
4. **Reads:** `CHQuery` or readonly DSN; set `max_memory_usage` and `max_execution_time`.
5. **Mutations:** avoid `ALTER DELETE` on hot tables; use partition drop or TTL.
6. **Dedup:** `insert_deduplication_token` per batch; rely on merges, not `FINAL`, on hot reads.
7. **Migrations:** single owner (`cmd/processor`); no `POPULATE` on large tables in incremental migrations.
8. **OPTIMIZE:** off-peak UTC only; cap per janitor run.

## Cross-Store

1. **Money:** Postgres ledger wins over CH sums in disputes.
2. **Config:** Postgres + outbox → Redis; never Redis → Postgres without txn.
3. **Telemetry:** CH is derived; OK to rebuild; not OK to lose Postgres ledger.
4. **Ack ordering:** Redis stream ack after durable write (PG commit or CH ack/WAL).

---

## Part VII — Multi-Region Cells

Target topology: unified billing and campaign management with **isolated regional processing nodes**. Traffic routes to the nearest cell via GeoDNS or Anycast. No cross-region I/O on `/track`.

## Core principle

1. **Regional cell (hot path).** Trackers, Redis ×4, and processor run strictly within the region.
2. **Global control plane.** Clients, finance, and anti-fraud policy live in global PostgreSQL. Regions receive updates asynchronously via `outbox`.
3. **Redis isolation.** Each region runs an identical Redis topology. Cross-region Redis sync is forbidden.

## Global PostgreSQL HA

| Requirement | Detail |
| :--- | :--- |
| R1 | Sync standby in another AZ (RPO ≈ 0) |
| R2 | Continuous WAL archive to object storage (PITR) |
| R3 | Async DR replica in another region |
| R4 | Single VIP/cluster DNS for automatic failover |

## Regional quotas

1. Global Postgres stores the campaign total limit.
2. `QuotaManager` reserves a budget chunk for a region/shard.
3. Amount credited to regional `budget:quota` (or tracker `LocalQuantaLedger` when `LOCAL_QUOTA_MODE=live` — [CAPABILITIES.md](./CAPABILITIES.md) §M8).
4. Regional `SyncWorker` reports spend to global Postgres.

## Outbox relay (at-least-once)

1. Event written to `outbox_events` + `outbox_region_delivery` in one transaction.
2. `RegionOutboxRelay` polls events for its region and applies to local Redis shards.
3. Status → `DELIVERED` only after write confirmation on all local shards.

### D3 v2 dedup adapter (M4)

| Step | Detail |
| :--- | :--- |
| SSID | `region_id` + `source_id` + `source_epoch` + `seq_start`/`seq_end` |
| factor_u | SHA-256 prefix of canonical payload bytes |
| factor_d | UUID from Postgres `dedup_claim_confirm` |

Workflow: `dedup_claim_confirm` → apply side-effects on `confirmed` → `sync_idempotency` row. Optional Redis guard: `SET NX dedup/v2:{dedup_key}`.

| Worker | SSID source | factor_u payload |
| :--- | :--- | :--- |
| `SyncWorker` | `(shard, campaign_id, inflight_gen)` | sorted `(campaign_id, amount_micro)` |
| `RegionOutboxRelay` | `outbox_event_id` | `relay\|event_id\|type\|payload` |
| Broker PG consumer | partition offsets + `source_epoch` | sorted `click_id` list |

Detail: [CAPABILITIES.md](./CAPABILITIES.md) §M4 · `pkg/dedupkey`.

---

## Part VIII — Verification Commands

```bash
# PostgreSQL plan audit (seeded Docker, ~50k ledger rows)
EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit_AllApplicationQueries -v

# Existing per-domain explain tests
go test ./internal/management/... -run 'Explain|OutboxExplain' -count=1
go test ./internal/billing/... -run TestM3ExplainQueryPlans -count=1
go test ./internal/marginguard/... -run TestMarginGuardExplainQueryPlans -count=1
go test ./internal/logcompactor/... -run TestAuditLogRollups_Explain_RealCH -count=1

# ClickHouse parts (production)
# SELECT table, partition, count() AS parts, sum(rows) FROM system.parts
#   WHERE active AND database = 'ad_event_processor' GROUP BY table, partition ORDER BY parts DESC;

# Postgres bloat
# SELECT relname, n_live_tup, n_dead_tup, last_autovacuum FROM pg_stat_user_tables
#   WHERE schemaname = 'public' ORDER BY n_dead_tup DESC LIMIT 20;
```

---

## Appendix — Fix Patterns

### Crypto hold race (`crypto_hold_worker.go`)

**Bug:** `FOR UPDATE SKIP LOCKED` in autocommit `Query`, then `rows.Close()`, then separate `BeginFunc` per hold.

**Fix:** single transaction per hold (or batch claim with `UPDATE … RETURNING` inside txn):

```sql
BEGIN;
SELECT … FROM payment.crypto_holds WHERE … FOR UPDATE SKIP LOCKED;
-- fraud gate, UPDATE status, INSERT outbox
COMMIT;
```

### Payment checkout lock (`service.go`)

**Bug:** `pg_advisory_lock` held during `CreateCheckout()` HTTP.

**Fix:** check idempotency → insert intent with `CREATED` → commit → call provider → update intent in new txn. Or `pg_advisory_xact_lock` only around the INSERT.

### Ledger flush audit (`campaign_repo.go`)

**Bug:** `CreateAuditLog(LEDGER_BATCH_FLUSH)` on every `UpdateSpend`.

**Fix:** delete or gate behind `AUDIT_LEDGER_FLUSH_SAMPLE_MASK`; rely on `sync_idempotency` + ledger row for audit trail.
