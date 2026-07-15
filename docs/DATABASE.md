# Databases: PostgreSQL and ClickHouse

Persistence layer in eSPX splits by role: **PostgreSQL** is the system of record (money, campaigns, outbox); **ClickHouse** is append-only analytics (telemetry, fraud, ML features). Redis hot state is documented in [EDGE.md](./EDGE.md).

**See also:** [ARCHITECTURE.md](./ARCHITECTURE.md) appendix A (outbox), [EDGE.md](./EDGE.md) Part III (quota).

---

# Part I — PostgreSQL

PostgreSQL is the single source of truth for financial operations, user accounts, billing, and campaign configuration. Writes are optimized for strict consistency, idempotent settlement, and minimal lock contention under concurrent load.

## 1. Balances and ledger (micro-units)

All money columns use `BIGINT` **micro-units** (1 major unit = 1_000_000 micro) to avoid float rounding in Lua `INCRBY` and PG aggregates.

### `balance_ledger`

Immutable rows record every balance change. Customer balance is derived from ledger sums (`PAYMENT_TOPUP` minus spend types), not a mutable balance column — avoids deadlocks and drift.

## 2. Idempotency and race prevention

At-least-once delivery from Redis streams requires exactly-once writes in PG.

### `sync_idempotency`

```sql
INSERT INTO sync_idempotency (event_id, processed_at)
VALUES ($1, NOW())
ON CONFLICT (event_id) DO NOTHING;
```

If the insert is skipped, the transaction aborts — no double charge for the same `click_id`.

### Advisory locks

Heavy or distributed operations (payment intents, manual top-up) use transactional locks:

```sql
SELECT pg_advisory_xact_lock(hashtext('payment_intent_lock_' || customer_id));
```

## 3. Transactional outbox (SKIP LOCKED)

Config mutations and Redis side effects use the **Transactional Outbox** — not `pg_notify` (buffers are process-local and lossy under lag).

```sql
SELECT id, event_type, payload
FROM outbox_events
WHERE status = 'PENDING'
ORDER BY id ASC
LIMIT 1000
FOR UPDATE SKIP LOCKED;
```

`OutboxWorker` polls every 20 ms, marks `PROCESSING`, pipelines Redis writes, then `PROCESSED`.

## 4. Partitioning

Table `events` uses monthly range partitions. Effects: compact indexes, `DROP PARTITION` for retention without `DELETE` scans, partition pruning on spend aggregates.

## 5. Schema topology

| Instance | Port | Schemas |
| :--- | :--- | :--- |
| Core `db` | 5430 | `public` (ads), `auth`, `billing`, `notifier` |
| `db-payment` | 5431 | `payment` only |

| Migration tree | Domain |
| :--- | :--- |
| `internal/ingestion/migrations/` (41 files) | Campaigns, events, ledger, outbox, quotas, slot map, ML |
| `internal/auth/migrations/` | Users, sessions, API keys |
| `internal/payment/migrations/` | Intents, webhooks, outbox |
| `internal/billing/migrations/` | Invoices, tax profiles |
| `internal/notifier/migrations/` | Notification queue |

---

# Part II — ClickHouse

ClickHouse stores real-time telemetry (impressions, clicks, conversions) and fraud events. Writes target large batches to limit LSM part fragmentation and merge pressure.

## 1. Tables and engines

Main tables: `impressions`, `clicks`, `conversions`, `fraud_events`.

**ReplacingMergeTree** (prod: `ReplicatedReplacingMergeTree`):

- `ORDER BY (campaign_id, CreatedAt, ClickID)` — fast per-campaign time-range scans
- Monthly partitions (`toYYYYMM(CreatedAt)`), TTL 90–180 d
- Replacing merge collapses duplicate sort keys on part merge

DDL: `deploy/clickhouse/init.sql`, `recon_materialized_views.sql`.

## 2. Buffered batch inserts

`internal/ingestion/clickhouse_store.go`:

- Non-blocking channel (capacity 1M events)
- `backgroundFlusher` batches per table
- Flush at 20k rows or 5 s (whichever first)
- `sync.Pool` for batch slices — near-zero alloc under steady load

Avoid inserts &lt;1000 rows; causes `Too many parts` merge storms.

## 3. Block deduplication

At-least-once from processor; exactly-once analytics via `insert_deduplicate=1` and SHA-256 block token over click IDs + timestamps. Dedup window is bounded (not infinite).

## 4. Reconciliation materialized views

- `mv_campaign_hourly_impressions`
- `mv_campaign_hourly_clicks`

`ReconWorker` compares hourly CH aggregates to PG ledger without full table scans.

## 5. Fraud feature store (`ml_features_1m`)

Minute aggregates for `cmd/fraud-scorer` and `cmd/ivt-detector`. Migration: `internal/processor/migrations/00003_ml_features_1m.sql` (applied on processor startup).

### Schema

| Column | Type | Role |
| :--- | :--- | :--- |
| `window_start` | DateTime | 1-minute bucket |
| `ip_address` | String | Identity group |
| `campaign_id` | UUID | Campaign dimension |
| `events` | UInt64 | Impression count |
| `clicks` | UInt64 | Click count |
| `spend_micro` | Int64 | Spend in currency micro-units (not float dollars) |
| `budget_limit_micro` | Int64 | Campaign budget ceiling (micro-units) |
| `unique_users` | UInt64 | Distinct users in window |
| `unique_uas` | UInt64 | Distinct user agents |

Engine: `SummingMergeTree`, ordered by `(window_start, ip_address, campaign_id)`. Fed by MVs from `impressions` and `clicks`.

### Feature groups (batch scorer input)

| Group | Examples | Source |
| :--- | :--- | :--- |
| Identity | IP /24, ASN, datacenter flag, TLS JA3 bucket | MaxMind, edge, `DeviceFilter` |
| Velocity | events/min per IP, per campaign; unique campaigns per IP (1h) | `ml_features_1m` rollups |
| Ratio | click/impression, low-TTC rate | CH + existing IVT rules |
| Campaign | spend ratio (`spend_micro / budget_limit_micro`), CTR vs baseline | CH + Postgres |
| Device | UA family, CH-UA mismatch, geo consistency | Event payload |

Money columns must stay `Int64` micro-units in SQL; normalize to ratios or divide by `1e6` only at model export (document scale in `metadata.json`).

### Labels and shadow validation

| Label | Source |
| :--- | :--- |
| Positive | Confirmed `blacklist:fraud`, operator review, `ivt_*` rule hits |
| Negative | Whitelisted ASN / known-good traffic |
| Soft | Ghost IVT events (train without hard block) |

After 24h shadow scoring (`FRAUD_SCORING_ENABLED=true`), run `scripts/fraud-scoring/shadow_precision_report.sql` in ClickHouse before enabling boost enforcement in production.

### Postgres fraud model tables

- `ml_model_versions` — artifact hash, metrics JSON, status (`DRAFT`/`SYNCING`/`ACTIVE`/`RETIRED`)
- `ml_shard_sync_state` — per-shard cutover phase during rolling deploy

See [ARCHITECTURE.md](./ARCHITECTURE.md#fraud-scoring-cold-path) for sync and enforcement flow.
