# Databases: PostgreSQL and ClickHouse

The eSPX data storage layer is split by role: **PostgreSQL** is the system of record (finance, campaigns, outbox); **ClickHouse** stores telemetry and analytics (events, fraud, ML features). Hot-path state lives in Redis.

---

# Part I — PostgreSQL

PostgreSQL is the primary source of truth for financial operations, user accounts, and campaign configuration. Writes are optimized for strict consistency and minimal locking.

## 1. Balances and Ledger (Micro-Units)

All financial amounts are stored as `BIGINT` in **micro-units** (1 unit = 1,000,000 micro-units). This avoids rounding errors when using `INCRBY` in Lua and aggregating in Postgres.

### `balance_ledger`
The table holds immutable records of every balance change. A client's current balance is computed as the sum of ledger entries, not stored in a mutable column. This prevents deadlocks and data drift.

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

## 3. Transactional Outbox (SKIP LOCKED)

The Outbox pattern synchronizes configuration and side effects to Redis.
Workers use `SELECT FOR UPDATE SKIP LOCKED`, allowing parallel workers to process different queue segments without blocking each other.

## 4. Partitioning

The `events` table is partitioned by month. This provides:
*   Compact index size.
*   Fast removal of old data via `DROP PARTITION`.
*   Faster aggregation queries through partition pruning.

---

# Part II — ClickHouse

ClickHouse stores telemetry in near real time. Writes use large batches to minimize data fragmentation (LSM parts).

## 1. Table Engines

`ReplacingMergeTree` is used (in production — `ReplicatedReplacingMergeTree`):
*   Sort key: `ORDER BY (campaign_id, CreatedAt, ClickID)`.
*   Partitioning: by month (`toYYYYMM(CreatedAt)`).
*   Duplicate collapse: during background merges by sort key.

## 2. Batch Inserts

`clickhouse_store.go` implements a buffer:
*   Non-blocking channel with capacity 1,000,000 events.
*   Background worker flushes at 20,000 rows or every 5 seconds.
*   Object pool (`sync.Pool`) to reduce GC pressure.

## 3. Block Deduplication

On batch redelivery, ClickHouse deduplicates using a SHA-256 block token computed from click IDs and timestamps.

## 4. Materialized Views

Aggregates speed up reconciliation:
*   `mv_campaign_hourly_impressions`
*   `mv_campaign_hourly_clicks`
These views let `ReconWorker` compare ClickHouse data with Postgres without full table scans.

---

# Part III — Reliability and Durability

## Durability Boundaries

1.  **Postgres.** Sole source of truth for finance and accounts. Requires: Sync Standby, WAL archiving (PITR), async DR replica.
2.  **Redis.** Ephemeral store (cache). Protection: Sentinel, local AOF, backups.
3.  **ClickHouse.** Derived data. Can be rebuilt from Postgres events or archive logs.

### Single-Site Postgres HA (G1)

**Topology:** Primary in AZ-a; synchronous standby in AZ-b (`synchronous_commit = remote_apply`, `synchronous_standby_names = 'standby_az_b'`). WAL archive to object storage every 60 s for PITR (RPO ~0 on sync replica; archive RPO <= 60 s).

**Failover runbook (operator):**

1. Confirm primary failure (`pg_is_in_recovery()` on primary fails; Patroni/repmgr shows primary down).
2. Promote sync standby: `pg_ctl promote -D $PGDATA` or `patronictl failover --candidate standby_az_b`.
3. Repoint `DB_DSN` on processor, management, tracker registry sync (not hot-path reads) to new primary VIP.
4. Verify `SELECT pg_is_in_recovery()` returns false on promoted node; run `AssertBudgetInvariant` smoke on one campaign.
5. Rebuild old primary as async replica after disk wipe.

**RTO target:** <= 120 s (VIP flip + pool drain). **RPO:** 0 on sync path.

**End-to-End Rule:** An event in the Redis Stream is marked processed (`XAck`) only after persistence in long-term storage (Postgres commit or ClickHouse acknowledgment).
