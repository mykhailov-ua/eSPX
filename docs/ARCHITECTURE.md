# eSPX Architecture

This document describes subsystem topology, data flows, and operational contracts of the platform.

## Documentation Navigation

| Document | Description |
| :--- | :--- |
| [CONCEPTS.md](./CONCEPTS.md) | Fundamental concepts: syscalls, memory, network, DOD, and mathematical models |
| [EDGE.md](./EDGE.md) | Network ingress (L4/L7), filtering, and quotas |
| [DATABASE.md](./DATABASE.md) | PostgreSQL and ClickHouse: reliability and idempotency |
| [GO.md](./GO.md) | Go runtime, zero allocations, and compiler optimization |
| [REDIS.md](./REDIS.md) | Shard topology, Lua scripts, and risks |
| [SHIPPED.md](./SHIPPED.md) | Delivered capabilities (M1–M3 core, M2, M5) |
| [PROPOSALS.md](./PROPOSALS.md) | System proposals: volume licensing, edge localization, eBPF filter |
| [MILESTONE.md](./MILESTONE.md) | Milestone index, roadmap, and delivery criteria |

## Topology

The system consists of five layers. Application services use host networking (`host networking`); state stores run on an isolated network (`bridge network`) with published ports.

1.  **Ingress (Nginx :8180).** Request proxying: `/admin/*` → management service, `/track/*` → trackers. Nginx OpenResty performs: per-campaign rate limiting, edge blacklist checks, Redis shard selection (CRC32).
2.  **Ingestion (Tracker x4, :8181-8184).** Traffic intake. Uses `gnet`, core-pinned worker pool (`PinnedWorkerPool`), and shared processing core `processTrack()`.
3.  **Edge State (Redis x4, :6479-6482).** Sharded state store. Includes replicas and Sentinel (x3). Sharding is per client. Operation atomicity is provided by Lua scripts.
4.  **Application.**
    *   **Processor (:8186):** Event stream processing.
    *   **Management (:8188):** Admin API, settlement gRPC server (:51053), UDP control publisher (:8190).
    *   **Auth (:51051), Payment (:51052, :8187), Billing (:51054), Notifier (:8085):** gRPC services.
    *   **IVT Detector / Fraud Scorer:** Fraud detectors (cold path).
5.  **Persistence.** PostgreSQL 16 and ClickHouse 24 for financial data, accounts, billing, and telemetry storage.

---

## Control Plane

*   **Management (`cmd/management`).** Central management node. Functions: REST API, settlement gRPC, `outbox` queue management, job scheduler, budget reconciliation (`ReconWorker`), quota management (`QuotaManager`).
*   **Auth (`cmd/auth`).** Authentication: gRPC server (registration, login, PASETO tokens). Uses Redis shard 0 for lockouts and token revocation.
*   **Payment (`cmd/payment`).** Payments: gRPC server, Stripe webhook handling, `outbox` queue for transaction confirmation. Uses a separate `payment` schema in Postgres.
*   **Billing (`cmd/billing`).** Billing: invoice generation based on `balance_ledger` data.
*   **Notifier (`cmd/notifier`).** Notifications: gRPC queue, delivery via Telegram, Slack, SMTP, SMS.
*   **IVT Detector / Fraud Scorer.** Anti-fraud modules. Batch-scan ClickHouse and update Redis blacklists via `outbox`.

---

## Data Plane

1.  **Ingress.** L4/L7 filtering.
2.  **Tracker.** Packet parsing (`gnet`), filter application (geo, schedule, fraud). Single Lua script execution (`EVALSHA`) for budget deduction.
3.  **Redis.** Atomic deduction, frequency cap (fcap), time-between-clicks check (TTC). Event write to `ad:events:stream`.
4.  **Streams.** Per-shard event streams forwarded to Processor.
5.  **Postgres.** Ledger, budget storage, and queues.
6.  **ClickHouse.** Analytics telemetry store.

**Request path:** Ingress (blacklists) → Tracker (UDP limits, geo, ML boosts) → Lua (budget deduction, fcap, TTC) → Stream (event write). Financial operations are atomic in Redis; Postgres sync is async.

---

## Settlement Pipeline

### Stream Processing (Processor)
Each shard has three consumer groups on stream `ad:events:stream`:
*   **PG Group:** Write to Postgres `events` partitions, update `campaign_stats`, check `sync_idempotency`.
*   **CH Group:** Batch insert into ClickHouse.
*   **Fraud Group:** Forward data to fraud stream and write to ClickHouse.

### Budget Sync (SyncWorker)
Background `SyncWorker` per shard:
1.  Lua: move data from `budget:sync` to `inflight` status under lock.
2.  Postgres: update spent amounts via `UpdateSpend`.
3.  Lua: commit — subtract confirmed amount from Redis.

This enables sub-millisecond deductions in Redis while isolating Postgres latency to the background.

---

## Configuration Management

### Transactional Outbox
To synchronize changes between Postgres and Redis, the Outbox pattern is used:
1.  Database change and insert into `outbox_events` happen in one Postgres transaction.
2.  `OutboxWorker` polls the table (every 20 ms), runs `SELECT FOR UPDATE SKIP LOCKED`, sends commands to Redis pipelines on all shards, and marks events as processed.

### Pacing and Schedule
*   **PacingControllerWorker.** Reconciles actual spend against profile (even/fast) in micro-units and updates Redis keys via outbox.
*   **Schedule Worker.** Manages campaign statuses (pause/start) by schedule.

---

## Fraud Scoring

Batch scoring and async sanction application. Direct ML computation on the hot path (Tracker) is forbidden; trackers only read precomputed keys from Redis.

**Data flows:**
1.  **Hot path:** Tracker writes telemetry to Redis fraud stream.
2.  **Warm path:** Processor moves data from Redis to ClickHouse (`ml_features_1m`).
3.  **Cold path:** `ivt-detector` and `fraud-scorer` read features from ClickHouse, run scoring (LightGBM/Isolation Forest), and send commands to management `outbox`.
4.  **Control plane:** Commands from `outbox` are applied across all Redis shards.

---

## Billing and Payments

*   **Money Truth.** The sole source of truth for funds is `balance_ledger` in Postgres.
*   **Payment Service.** Isolated service for payment gateways. Uses dedicated `db-payment` database for PCI compliance.
*   **Billing Service.** Invoice generation based on `balance_ledger` aggregates. Works with tax profiles and basis-point calculations.

---

## Log Broker (Optional)

`cmd/broker` service based on `mmap` segments. Used as a lightweight commit log for resilience when regional data is lost. Not a Kafka replacement; intended for local log segment storage with subsequent evacuation to S3.
