# Regional Cells and Global Control Plane

Target system topology: unified billing and campaign management combined with **isolated regional processing nodes**. Traffic is routed to the nearest node via GeoDNS or Anycast. Direct cross-region connections on the hot path are forbidden to meet SLA (p95 < 50 ms).

---

## Core Principle

1.  **Regional node (Hot Path).** Traffic processing (trackers, Redis, local processors) runs strictly within the region. No cross-region I/O on the `/track` path.
2.  **Global control plane (Global Control Plane).** Manages clients, finance, and anti-fraud policy. PostgreSQL is the single source of truth. Data is delivered to regions asynchronously via `outbox` queues.
3.  **Redis isolation.** Each region runs an identical Redis topology (4 shards). Synchronization between regional Redis instances is forbidden.

---

## Global PostgreSQL: Replication and Fault Tolerance

A single logical PostgreSQL database holds financial ledgers and configuration. Running without replication in production is forbidden.

### Replication Requirements (Production)
*   **R1.** Synchronous standby in another availability zone (AZ). Provides RPO ≈ 0.
*   **R2.** Continuous WAL archiving to object storage (S3) for point-in-time recovery (PITR).
*   **R3.** Asynchronous replica in another geographic region (DR site) for full disaster recovery.
*   **R4.** A single endpoint (VIP or cluster DNS) for automatic application failover to a new master node.

---

## Unified Billing and Regional Quotas

**Quotas** prevent budget overrun across multiple regions.

1.  Global Postgres stores the campaign's total limit.
2.  `QuotaManager` reserves a budget chunk in Postgres for a specific region and shard.
3.  The reserved amount is credited to the local `budget:quota` key in regional Redis.
4.  The hot path debits only from the local quota in Redis.
5.  The local `SyncWorker` asynchronously reports spend to global Postgres.

---

## Asynchronous Event Delivery (At-Least-Once)

Cross-region data updates are asynchronous only. Transport guarantees **at-least-once** delivery, so regional handlers must be **idempotent**.

### Delivery Flow (Outbox Relay)
1.  An event is written to `outbox_events` and `outbox_region_delivery` in one transaction.
2.  The regional relay (`RegionOutboxRelay`) polls events for its region.
3.  The relay applies changes to local Redis shards.
4.  **Idempotency.** To prevent re-applying the same event, use `region_apply_idempotency` (on the regional side) or `SET NX` keys in Redis.
5.  Event status changes to `DELIVERED` only after write confirmation on all local Redis shards.

---

## Global Identification (Global UUID)

A **Global UUID** supports cross-region analytics and audit.

### Token Structure (128 bits)
*   `[0..5]` — Timestamp (Unix timestamp ms).
*   `[6..7]` — Version and sequence fragment.
*   `[8]` — Region identifier (`region_code`).
*   `[9]` — Tracker instance identifier.
*   `[10..15]` — Remaining sequence.

This identifies the ID source (region and specific pod) without querying a central database.
