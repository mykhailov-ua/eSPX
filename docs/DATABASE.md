# Databases: PostgreSQL and ClickHouse

The eSPX data layer is split by role: **PostgreSQL** is the system of record (finance, campaigns, outbox); **ClickHouse** stores telemetry and analytics (events, fraud, ML features). Hot-path state lives in Redis.

This document is the operator and implementer contract for database work. Principles draw from *Designing Data-Intensive Applications* (Kleppmann — consistency boundaries, logs vs tables, batching, derived data) and *Distributed Systems* (Tanenbaum — mutual exclusion, deadlock avoidance, time bounds on locks).

**Milestone & bottleneck plan:** [DATABASE_MILESTONE.md](./DATABASE_MILESTONE.md) — syscall/mmap physics, M-DB-PG/CH items, phased fixes.

---

# Part I — PostgreSQL

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
| **Budget Sync** | Postgres | UUID v4 per batch (`budget:txid:*` in Redis) | Prevent double accounting in `UpdateSpend` |
| **Stream → PG** | Postgres PK | `(click_id, created_date)` | Prevent duplicate rows on stream redelivery |
| **Admin API** | Postgres | `SHA256(customer_id + canonical_json(body))` | Idempotent administrative HTTP requests |
| **Payments** | Postgres | Client `idempotency_key` | Prevent duplicate transactions |

## 3. Transactional Outbox (`SKIP LOCKED`)

The Outbox pattern synchronizes configuration and side effects to Redis.

Workers use `SELECT … FOR UPDATE SKIP LOCKED` inside a **single transaction** that also marks rows `PROCESSING` or `PROCESSED`. Parallel workers claim disjoint row sets without blocking.

**Rules:**

- Never run `FOR UPDATE` outside an explicit transaction — locks are released at statement end in autocommit mode.
- Claim and status transition must share one `BEGIN … COMMIT` boundary.
- Use `SKIP LOCKED`, not blocking `FOR UPDATE`, for worker queues.
- Poll interval must be configurable; default **20 ms** in management (`service.go`) is aggressive — consider **100–250 ms** when idle to cut WAL churn.

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

# Part II — ClickHouse

ClickHouse stores telemetry in near real time. Writes use large batches to minimize part count (LSM-style merge tree).

## 1. Table Engines

Production raw events: `ReplacingMergeTree(created_at)` (or `ReplicatedReplacingMergeTree`):

- Sort key: `ORDER BY (campaign_id, CreatedAt, ClickID)`.
- Partitioning: `toYYYYMM(CreatedAt)`.
- Duplicate collapse: eventual, on background merge — not at read time unless `FINAL`.

SummingMergeTree rollups (`campaign_hourly_*`, `placement_stats_hourly`, `audit_log_rollups`) use monthly partitions where configured.

## 2. Batch Inserts

Processor path (`internal/ingestion/clickhouse_store.go`, `cmd/processor/main.go`):

- Default `CH_BATCH_SIZE=50000`, flush every `CH_FLUSH_INTERVAL_MS` (10 s).
- Up to **four** `PrepareBatch` calls per flush (`impressions`, `clicks`, `conversions`, `fraud_events`).
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

# Part III — Reliability and Durability

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

---

# Part IV — Known Bottlenecks (Audit 2026-07)

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

# Part V — Implementation Rules (Checklist)

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

# Part VI — Verification Commands

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

# Appendix — Fix Patterns

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
