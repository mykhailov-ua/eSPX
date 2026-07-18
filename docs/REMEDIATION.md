# eSPX: Remediation Plan and Tradeoff Balancing

This document outlines a plan to address architectural gaps that emerged while optimizing round-trip time (RTT). The initial decision to run one Lua script per request met SLA targets but introduced budget drift risk and reduced fault tolerance.

---

## 1. Architectural Proposal: Batch Processing

**Core principle:** The hot path performs only append and local validation. State synchronization moves to the cold path, where locks and network latency are acceptable.

### 1.1 System Structure
1.  **Hot path (Lua):** Budget/quota validation and click deduplication. Stream writes are moved out.
2.  **Local buffer:** Telemetry written to a non-blocking queue (MPSC) or mmap log in tracker memory.
3.  **Cold path (Processor):** Read buffers and run operations under a campaign or shard mutex:
    *   Flush spend to Postgres.
    *   Write events to ClickHouse.
    *   Refill quotas and run global reconciliation.

### 1.2 Assessment
*   **Benefits:** Shorter Redis lock time. Lua scripts focus on finance only. Postgres latency does not affect traffic ingestion.
*   **Risks:** A consistency window (hundreds of milliseconds). Requires a reliable local buffer (mmap) for recovery after process crash.

---

## 2. Microservice Separation

To improve stability, remove synchronous dependencies between services:
*   **Asynchronous settlement.** The payment gateway writes events to streams; management processes them asynchronously.
*   **Asynchronous anti-fraud.** Scoring modules write decisions directly to `outbox` or streams, avoiding synchronous gRPC calls.
*   **Auth caching.** Use a local TTL cache for API key verification.
*   **Local blacklists.** `SISMEMBER` checks on the campaign's local shard (data replicated via outbox).

---

## 3. Budget Optimization and Race Condition Elimination

*   **Invariant check.** Enforce: `(limit - Redis remainder) = (PG spend + sync delta)`.
*   **Single writer.** Only the processor `SyncWorker` may change campaign spend in Postgres.
*   **No fallbacks.** Disable direct tracker-to-Postgres queries on cache misses in production.
*   **Mutex synchronization.** Postgres budget updates run under per-campaign lock in `SyncWorker`.

---

## 4. Batch Write Reliability (ClickHouse)

**Critical vulnerability (P0):** In the current implementation, events are acknowledged in the Redis Stream (`XAck`) immediately after entering the Go channel, before actual write to ClickHouse.

**Remediation:**
1.  **D1: Synchronous write.** Block acknowledgment (`XAck`) until ClickHouse responds.
2.  **D2: Local WAL.** Write the batch to a local spool file (mmap) with `fsync` before Redis acknowledgment. An async worker reads the spool and writes to ClickHouse.

---

## 5. Implementation Priorities

| ID | Status | Notes |
| :--- | :--- | :--- |
| D0/D1 | Done | `ClickHouseStore.StoreBatch` blocks until CH `batch.Send()` succeeds; `StreamConsumer.flushBatch` XAcks only after `StoreBatch` returns nil |
| D2 | Done | Rotating mmap WAL (`events.wal` + `events.wal.NNNN`), lazy FD, `CH_SPOOL_*` env; `RecoverSpool` on startup |
| D4 | Done | PG/CH pool limits + `ProcessorPgGate` / `ProcessorChGate`; stream `pauseStreamReads` backpressure |
| H1 | Done | `SyncWorker` sole `UpdateCampaignSpend` writer; `syncMu` serializes flushes |
| B2 | Done | `TRACKER_PG_FALLBACK=0` in production; `UnifiedFilter.SetPGFallbackAllowed` |
| G1 | Documented | Single-site sync standby + WAL archive runbook in `docs/DATABASE.md` section III |
| M1 | Pending | Probabilistic budget reconciliation (Milestone 3+) |

## 6. L1 Lua Decomposition (Deferred Exception)

Separating `XADD` from `unified-filter.lua` remains deferred. Bench evidence (`BenchmarkFilterEngine_Check_noTimeout` ~28 ns, 0 allocs/op; Redis Lua p99 budget < 10 ms in `GUIDE_CHAOS_RELIABILITY.md`) shows the extra stream round-trip would dominate shard lock time. Stream writes stay atomic with budget debit until a broker-side ingest path (Milestone 3 broker consumers) absorbs telemetry without a second Redis hop.

---

## 7. Backlog: User-Space Write Concurrency (Semaphores)

SEM-P1, SEM-P2, and SEM-P5 are **complete** (Milestone 1 §1.1b). Remaining items:

| ID | Target | Proposal | DoD |
| :--- | :--- | :--- | :--- |
| SEM-P3 | Management cold-path pool | `mgmtPgSem` on `DB_TRACKER_MAX_CONNS` (default 4); background workers (quota warm, recon) yield to HTTP admin | p99 admin SQL stable under quota warm load |
| SEM-P4 | Per-campaign spend mutex (optional) | In-process `map[uuid.UUID]*sync.Mutex` only if second `current_spend` writer is added | Concurrent same-campaign flushes serialize; idempotency still holds |
| INST-P1 | Interactive installer | `scripts/install` or `make setup` wizard writes `.env` from prompts (DSNs, worker counts, gate slots, shard topology) | First-run produces working compose stack without hand-editing 40+ keys |

**Not planned:** a single PG+CH semaphore (different backends and failure modes; coordinate each pool independently).

### 7.1 Failure Scenarios (Current Write Strategy)

| Scenario | Mechanism | Current behavior | Risk | Planned chaos proof |
| :--- | :--- | :--- | :--- | :--- |
| FD exhaustion | `ulimit -n`; one TCP conn + one FD per `pgx`/`redis`/spool file | Pool caps limit PG TCP FDs; tracker `ulimit` 100k in compose | Spool + log segments + shard clients exceed NOFILE under leak or misconfig | `write_path_fd_pressure` |
| TCP pool pseudo-deadlock | `MaxWorkers * shards` goroutines vs explicit gates | `ProcessorPgGate` caps logical writers; `pauseStreamReads` on circuit open | Lag grows in Redis stream, not unbounded goroutine pile | `processor_pg_gate_overflow` |
| mmap segment full | `CHSpool` rotates sealed `events.wal.NNNN` segments; lazy mmap on Scan | `errCHSpoolMaxSegments` when `CH_SPOOL_MAX_SEGMENTS` exhausted | Long CH outage stops durable acks after segment budget | `ch_spool_max_segments` |
| SIGTERM (K8s preStop) | `consumer.Close()` + `drainTimeout` | `drainNewMessages` + pending batch flush | Batch in memory only if store fails during drain | `write_path_sigterm_drain` |
| SIGKILL (K8s OOMKill) | No userspace hook | CH spool `fsync` before ack survives; in-flight batch without spool/PG commit is lost | PEL retains unacked messages; recovery on restart | `write_path_sigkill_spool_recovery` (T-ID-05) |
| DB failure before batch complete | `StoreBatch` error before `XAck` | Retriable errors retain PEL; circuit opens; `pauseStreamReads` stops XREADGROUP | DLQ only on non-retriable poison pills | `write_path_db_fail_pre_ack` |
