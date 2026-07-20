# M-DB — Database & Cold-Path Storage Milestone

Engineering milestone for PostgreSQL, ClickHouse, and local durability (mmap WAL, logger, broker). Companion to [DATABASE.md](./DATABASE.md) (operator rules) and [REMEDIATION.md](./REMEDIATION.md) §7 (semaphores).

**Principles (DDIA / Tanenbaum):**

- **Single writer per aggregate** — one authoritative store for money (Postgres ledger); derived stores (CH, Redis) are rebuildable.
- **Log before ack** — Redis stream `XAck` only after durable append (PG commit, CH ack, or mmap `fsync`).
- **Bound concurrency** — semaphores cap in-flight writers; coefficients tune batch size and poll intervals.
- **Mutual exclusion with time bounds** — row locks and advisory locks inside short transactions; never hold locks across network I/O.

This document has two parts:

| Part | Contents |
| :--- | :--- |
| **I — Reference** | Storage physics, syscall cost, mmap contracts, concurrency model, observability signals |
| **II — Tasks** | Acceptance gates, backlog items, delivery phases, implementation status |

---

# Part I — Reference: storage, syscalls, mmap

## §1 Physical model — what actually hits the disk

Every “database write” in eSPX is a stack of layers. Understanding the physics prevents fixing the wrong layer.

```
┌─────────────────────────────────────────────────────────────────┐
│ Application (Go)                                                │
│   batch / txn / mmap copy                                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │ syscall boundary
┌───────────────────────────▼─────────────────────────────────────┐
│ VFS (write, fdatasync, fsync, mmap msync implicit on fsync)     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│ Page cache (buffered) → block device queue (NVMe / EBS)         │
│   IOPS + throughput limits; fsync forces flush queue drain      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│ Postgres WAL / CH parts / ext4 journal                          │
└─────────────────────────────────────────────────────────────────┘
```

| Path | Typical syscall pattern | Dominant cost |
| :--- | :--- | :--- |
| Postgres `COMMIT` | `write()` WAL records + `fdatasync()` per commit (with `synchronous_commit`) | **Commit rate × fsync latency** (~0.1–2 ms/NVMe) |
| CH `PrepareBatch.Send` | TCP send; server async_insert buffers | Network + server merge, not client fsync |
| CH mmap spool (`CHSpool`) | `copy` into `MAP_SHARED` + **`file.Sync()` per batch** | **One fsync per spooled batch** |
| Logger persister | `Write()` batched + **`Fdatasync()` per flush buffer** | fsync every 256 KiB or 50 ms |
| Broker partition log | `mmap` store + periodic **`Sync()`** (mode-dependent) | Group-commit amortizes fsync |
| License spool | `mmap` copy + optional `Sync()` | One fsync per heartbeat token |

**Key insight:** Postgres and cold-path mmap spools are **fsync-bound** under high commit rates. ClickHouse hot path is **batch-size and part-count bound**. Tuning semaphores reduces concurrent fsync pressure; tuning coefficients reduces fsync *count*.

---

## §2 Syscall overhead

### §2.1 Cost model

| Syscall | What it does | Relative cost |
| :--- | :--- | :--- |
| `write()` | Copy userspace → page cache; may allocate, may block if dirty ratio high | Low per call if batched; high if many small calls |
| `fdatasync()` / `fsync()` | Flush file data (+ metadata for fsync) to device; **blocks until durable** | **Dominant** — often 0.05–5 ms on NVMe, 5–50 ms on contended cloud disk |
| `mmap` + userspace `copy` | No syscall per byte; page faults on first touch | Amortized; TLB/page fault cost on cold maps |
| `read()` / `pread()` | Used by logger compressor, logcompactor | Cold path; batch with large buffers |

**Tanenbaum:** crossing the user/kernel boundary (~100 ns–1 µs) is cheap compared to **waiting for the device**. Optimizing syscalls means **fewer fsyncs**, not fewer `mmap` copies.

### §2.2 Where eSPX pays syscall tax today

#### Logger (`pkg/logger/flush_persist.go`) — fsync-per-batch

```go
n, err := l.activeFile.Write(data)      // one write() per AlignedBuffer
err = syscall.Fdatasync(fd)             // durable before returning
```

| Parameter | Default | Physics |
| :--- | :--- | :--- |
| `FlushBufferSize` | 256 KiB | Larger → fewer fsyncs/sec, higher loss window on crash |
| Drainer tick | 5 ms | Wakes to coalesce ring → buffer |
| Flush deadline | 50 ms | Max latency before fdatasync even if buffer unfilled |
| Rotation | size / time | New `active.log` → rename; no fsync on rotate until next batch |

**Bottleneck:** At high audit volume, **fsync rate ≈ min(events/batch_size, 20/sec)**. NVMe EMA > `DiskLatencyLimit` → `diskDegraded` → priority-0 logs shed.

#### CH spool (`internal/ingestion/ch_spool.go`) — fsync per WAL record

```go
copy(seg.mmap[pos:...], record)   // no syscall
seg.file.Sync()                   // fsync every AppendDurably
```

| Parameter | Default | Physics |
| :--- | :--- | :--- |
| Segment size | 512 MiB | Pre-truncated `MAP_SHARED` file |
| Max segments | 8 | ~4 GiB max spool; then `errCHSpoolMaxSegments` |
| Record max | 16 MiB | One processor batch per record |

**Bottleneck:** CH outage → every `StoreBatch` failure → **one fsync per batch** (up to 50k events still one fsync). Under sustained outage, **fsync IOPS ≈ batch flush rate** (e.g. 0.1–1/s per shard). Acceptable for failover; dangerous if spool used as primary write path.

Replay compaction (`ReleaseRecord` copy + fsync) is necessary for safety and runs off the hot path.

#### Broker (`pkg/broker/log/log.go`) — mmap write, deferred fsync

```go
// Append: unsafe copy into mmap — zero write() syscalls per record
Segment.Write() → mmap store
// DurabilitySync: fsync every append
// DurabilityGroupCommit: fsync every N records or flush loop
```

**Bottleneck:** `DurabilitySync` on audit broker → fsync per produce. **GroupCommit** amortizes (default for throughput). Production audit path uses group-commit; chaos tests validate RPO bound (`chaos_durability_lab_test.go`).

#### Postgres — implicit syscalls via libpq

Each `COMMIT` → WAL `write` + `fdatasync` (with `synchronous_commit=on`). Not visible in Go code but dominates processor PG path.

| Anti-pattern | fsync multiplier |
| :--- | :--- |
| Per-campaign `UpdateSpend` txn | 1 fsync × active campaigns per flush window |
| 20 ms outbox poll (idle claim txn) | Up to 50 empty-ish txns/sec |
| Per-event outbox `PROCESSED` update | 2× txn per event (claim + mark) |

### §2.3 Syscall rules (normative)

1. **Hot path (tracker):** no `write()`/`fsync` on request path — ring buffer only.
2. **Warm path (processor):** prefer **large batches** before any fsync boundary (CH, PG, spool).
3. **Cold path (logger, spool):** fsync is required for durability; **batch before fsync**, never fsync per log line.
4. **Measure:** `ad_log_nvme_write_duration_seconds`, `ad_broker_fsync_duration_seconds`, PG `pg_stat_bgwriter` / `wal_sync`.

---

## §3 mmap — data safety on the cold path

### §3.1 Why mmap (not repeated `write()`)

| Approach | Pros | Cons |
| :--- | :--- | :--- |
| `write()` loop | Simple | Syscall per buffer; kernel may split IO |
| `mmap MAP_SHARED` + `copy` | Zero syscalls on append; fixed segment size; crash recovery by scan | Must `fsync` explicitly; `ReleaseRecord` compaction copies in userspace |
| `mmap` read-only lazy map | Scan sealed CH spool segments without FD leak | `Mmap`/`Munmap` per scan path |

eSPX uses mmap for: **CH spool**, **license spool**, **broker log segments**. Postgres/CH network clients do not use mmap.

### §3.2 Durability contract per mmap WAL

| WAL | File | fsync when | Crash recovery | Ack rule |
| :--- | :--- | :--- | :--- | :--- |
| **CHSpool** | `events.wal` | Every `AppendDurably` | Magic + CRC scan from offset 0 | `StoreBatch` OK after CH insert **or** spool fsync → then `XAck` |
| **LicenseSpool** | `license.wal` | Every token append (`fsync=true`) | Scan valid records | Heartbeat can proceed after spool fsync |
| **Broker log** | `*.log` mmap | `Segment.Sync()` per durability mode | `findActualIndexSize` trims torn index | Produce OK after durability mode satisfied |

### §3.3 CH spool lifecycle

```
StoreBatch fails (CH down)
    → marshalCHSpoolPayload (heap alloc, vtproto)
    → appendLocked: copy → mmap
    → file.Sync()                    ← durability point
    → return nil to processor
    → XAck Redis

CH recovers
    → replaySpoolOnce (2s ticker)
    → Scan: lazy mmap sealed segments
    → insertToClickHouse
    → ReleaseRecord: memmove + Sync OR delete segment
```

**Safety invariants:**

1. **No ack without durable spool** — `clickhouse_store.go` returns error if spool append fails.
2. **CRC32 per record** — torn write stops scan at first bad magic/CRC (`errCHSpoolCorrupt`).
3. **Segment rotation** — `Sync` before `rename`; sealed segments immutable except `ReleaseRecord` head compaction.
4. **Max segments** — bounded disk; processor stops acking when full → PEL grows → operator alert.

**Risks:**

| Risk | Scenario | Mitigation |
| :--- | :--- | :--- |
| Double insert | Replay succeeds, `ReleaseRecord` fails | CH `insert_deduplicate_token` per batch |
| Partial segment after crash | fsync incomplete | CRC scan truncates at first invalid record |
| FD exhaustion | Many lazy maps during scan | One active FD; sealed segments mapped only during `Scan` |
| memmove on `ReleaseRecord` | Large sealed segment compaction | Delete whole segment when fully replayed |

### §3.4 Logger — not mmap, but same durability class

Logger uses **append + fdatasync** (not mmap) for billing audit segments:

- Hot: MPSC ring (`Write` / `WriteToShard`) — **no disk**.
- Warm: drainer batches to `AlignedBuffer`.
- Cold: single persister goroutine → `active.log` → fdatasync.
- Archive: rotate → zstd + AES-GCM → `.zst.ready` → fdatasync on archive file.

**Data safety priority:** billable events use priority 1; degraded disk sheds priority 0. Aligns with “data safety primary on cold path.”

### §3.5 mmap rules (normative)

1. **`MAP_SHARED` only** for writable WALs — `MAP_PRIVATE` breaks durability.
2. **Always `Sync()` before rename/rotate** — CH spool rotation does this.
3. **Lazy map sealed segments** — avoid N open FDs (`ch_spool.go` design).
4. **Never mmap on gnet hot path** — processor spool runs in consumer goroutine only.
5. **SIGKILL** — only fsync-completed records survive; PEL retains the rest.

---

## §4 Concurrency model — semaphore + coefficient

```
                    ┌─────────────────────────────────────┐
                    │         HARD CEILING                │
                    │  Semaphore (channel / gate slots)   │
                    │  ProcessorPgGate, ProcessorChGate   │
                    │  MgmtPgGate (HIGH / LOW tiers)      │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │      SOFT TUNING                    │
                    │  Coefficient model                  │
                    │  • batch N campaigns per txn        │
                    │  • outbox poll backoff              │
                    │  • logger/group-commit fsync window  │
                    │  • CH_BATCH_SIZE × flush interval   │
                    └─────────────────────────────────────┘
```

**Semaphore** caps concurrent writers (hard ceiling). **Coefficient** tunes batch size, poll interval, and fsync window (soft tuning within the ceiling). Use both — not either/or.

**Not planned:** unified PG+CH semaphore (`REMEDIATION.md` §7).

---

## §5 Observability signals

| Fault | Subsystem | Proof test / metric |
| :--- | :--- | :--- |
| PG gate overflow | processor | `TestChaos_ProcessorPgGate_Overflow` |
| CH spool rotation | processor | `TestChaos_CHSpool_Rotation` |
| SIGKILL spool recovery | processor | `write_path_sigkill_spool_recovery` |
| Broker slow fsync | broker | `chaos_durability_lab_test.go` |
| Dual outbox race | management | `TestChaos_DualOutboxWorkerRace` |
| NVMe degraded | logger | `diskDegraded` + priority shed |
| CH max segments | processor | `errCHSpoolMaxSegments` → PEL growth alert |

### Appendix A — Syscall vs mmap decision tree

```
Need durability before ack?
├─ NO  → ring buffer / Redis only (hot path)
├─ YES → network DB (PG/CH)?
│        ├─ YES → batch + txn/commit or CH PrepareBatch
│        └─ NO  → local WAL
│                 ├─ High throughput, scan recovery → mmap + periodic fsync
│                 └─ Simple append → buffered write + fdatasync (logger)
```

### Appendix B — fsync budget estimator

```
PG WAL fsyncs/sec ≈ commits/sec
                 ≈ (campaigns_per_flush / batch_N) / flush_interval
                   + outbox_polls/sec × txns_per_poll
                   + API_txns/sec

CH client fsyncs/sec ≈ 0 (async insert) on happy path
CH spool fsyncs/sec ≈ CH_outage_batches/sec (per shard)

Logger fsyncs/sec ≈ 1 / min(50ms, buffer_fill_time)
```

Use this before adding workers or lowering poll intervals.

---

# Part II — Task backlog

## §0 Acceptance gates

| Gate | Command / metric | Target |
| :--- | :--- | :--- |
| PG plan audit | `EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit` | No warn-level seq scans at seeded scale |
| CH spool bench | `go test -bench=BenchmarkCHSpoolAppendDurably ./internal/ingestion/...` | Regression tracked in CI |
| Processor gates | `ad_processor_write_acquire_wait_seconds` | p99 < 100 ms under load |
| Logger NVMe | `ad_log_nvme_write_duration_seconds` | EMA < `DiskLatencyLimit`; degraded mode sheds priority-0 |
| Broker fsync | `ad_broker_fsync_duration_seconds` | Group-commit p99 within SLA |
| Budget invariant | `AssertBudgetInvariant` | Always |

---

## §1 PostgreSQL (M-DB-PG)

### M-DB-PG-1 — Per-campaign ledger flush (HIGH) ✅

| | |
| :--- | :--- |
| **Where** | `sync_worker.go` → `campaign_repo.UpdateSpendBatch` |
| **Physics** | Each campaign = 1 txn = 1 WAL fsync + 4–6 index updates (`balance_ledger`, `campaigns`, `sync_idempotency`, quota, optional `admin_audit_log`) |
| **Symptom** | WAL IOPS ∝ active campaigns / `LEDGER_BATCH_FLUSH_MS` (default 10 s) |
| **Fix** | **Coefficient batching:** one txn per ≤32 campaigns; single gate acquire per batch |
| **Semaphore** | Keep `ProcessorPgGate` flat — do not prioritize stream vs sync |
| **DoD** | `ad_sync_ledger_batch_size` p50 ≥ 8; gate wait p99 < 100 ms |
| **Status** | ✅ `UpdateSpendBatch` + `flushCampaignRollupBatched` (N=32); metric `ad_sync_ledger_batch_size`; `TestLedgerBatch_MultiCampaignSingleTxn` |

**EXPLAIN proof** (`EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit`, 50k seeded ledger rows, 0 warn-level seq scans on hot-path queries):

| Query | Plan (seeded scale) | Findings |
| :--- | :--- | :--- |
| `GetCampaignBudget` | `Index Scan campaigns_pkey` → `Index Scan customers_pkey` (Nested Loop) | 0 |
| `INSERT sync_idempotency ON CONFLICT DO NOTHING` | `Insert` PK conflict arbiter | 0 |
| `CreateLedgerEntry` | `Insert balance_ledger` (FK triggers only) | 0 |
| `UpdateCampaignSpend` | `Index Scan campaigns_pkey` → `Update` | 0 |
| `DecreaseCampaignQuotaReserved` | `Index Scan campaign_quotas_pkey` → `Update` | 0 |

### M-DB-PG-2 — `LEDGER_BATCH_FLUSH` audit (HIGH) ✅

| | |
| :--- | :--- |
| **Where** | `campaign_repo.go` → `applySpendFlush` |
| **Physics** | Extra INSERT + index on `admin_audit_log` with zero compliance value (ledger row already exists) |
| **Fix** | Remove or `AUDIT_LEDGER_FLUSH_SAMPLE_MASK` (mirror `writeAuditLog`) |
| **DoD** | Zero `LEDGER_BATCH_FLUSH` rows in steady state or < 0.1% sample |
| **Status** | ✅ Default `AUDIT_LEDGER_FLUSH_SAMPLE_MASK=-1` (disabled); optional mask via `ConfigureAuditLedgerFlush`; `TestLedgerBatch_AuditSampling` |

**EXPLAIN proof:** audit INSERT removed from steady-state path (0 rows). When enabled, sampling uses the same `shouldSampleHistogram` mask as `writeAuditLog`; no additional hot-path PG query in default config.

### M-DB-PG-3 — Outbox 20 ms poll (MEDIUM) ✅

| | |
| :--- | :--- |
| **Where** | `management/service.go:85` |
| **Physics** | Idle `FOR UPDATE SKIP LOCKED` + status UPDATE → WAL traffic ~50 Hz/worker |
| **Fix** | **Coefficient backoff:** 20 ms when work found, decay to 250 ms idle; already resets on `processed > 0` |
| **DoD** | `ad_outbox_poll_interval_ms` p50 > 50 ms at low queue depth |
| **Status** | ✅ `outbox_poll.go` backoff (20→250 ms); metric `ad_outbox_poll_interval_ms`; `TestOutboxPollBackoff_IdleMedianAboveDoD` |

### M-DB-PG-4 — Management pool contention (MEDIUM) — SEM-P3 ✅

| | |
| :--- | :--- |
| **Where** | All `management` workers share `DB_TRACKER_MAX_CONNS` |
| **Physics** | Recon + volume meter + outbox + HTTP compete for connections |
| **Fix** | **Priority semaphore:** `MgmtPgGate` — HIGH (HTTP, outbox, drain), LOW (recon, pacing, volume) |
| **DoD** | p99 admin SQL stable under quota warm; `ad_mgmt_pg_gate_rejected_total{tier=low}` bounded |
| **Status** | ✅ `mgmt_pg_gate.go`; HTTP/outbox/drain=HIGH; recon/pacing/volume=LOW; `TestMgmtPgGate_LowRejectedWhenBudgetExhausted` |

### M-DB-PG-5 — Payment advisory lock over HTTP (HIGH) ✅

| | |
| :--- | :--- |
| **Where** | `payment/service.go:58–84` |
| **Physics** | Holds connection + lock during Stripe HTTP (100 ms–2 s) |
| **Fix** | Idempotency check → INSERT `CREATED` → commit → provider call → UPDATE |
| **DoD** | Chaos test `TestChaos_PaymentConcurrentCreateIdempotencyKey` green; pool wait flat |
| **Status** | ✅ `claimPaymentIntent` / `finalizePaymentIntent`; advisory lock only during claim txn; chaos test green |

**EXPLAIN proof** (`EXPLAIN_AUDIT=1 go test ./internal/payment/... -run TestExplainAudit_PaymentIntentQueries`):

| Query | Plan |
| :--- | :--- |
| `GetPaymentIntentByIdempotencyKey` | `Index Scan` on `uq_payment_intents_idempotency` |
| `INSERT payment_intents (CREATED)` | PK insert |
| `UPDATE payment_intents` (finalize) | `Index Scan payment_intents_pkey` → `Update` |

### M-DB-PG-6 — Index write amplification (MEDIUM) ✅

| | |
| :--- | :--- |
| **Where** | `campaigns`, `balance_ledger`, `outbox_events` |
| **Physics** | Every INSERT/UPDATE touches multiple B-trees; autovacuum lag → bloat |
| **Fix** | Fewer writes (PG-1, PG-2); `00048` partial indexes; monitor `n_dead_tup` |
| **DoD** | `EXPLAIN_AUDIT` green at 50k ledger rows |
| **Status** | ✅ `00048_explain_audit_indexes.sql`; `ad_pg_dead_tuples` collector; audit fails on warn |

**EXPLAIN proof** (`EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit`, 50k ledger rows, 0 warnings):

| Query | Plan |
| :--- | :--- |
| `partial.idx_ledger_fee_created` | Bitmap/Index Scan on `idx_ledger_fee_created` |
| `partial.idx_ledger_topup_recent` | Index Scan on `idx_ledger_topup_recent` |
| `partial.idx_campaigns_draining_updated` | Index Scan on `idx_campaigns_draining_updated` |

### M-DB-PG-7 — Crypto hold race (FIXED) ✅

| | |
| :--- | :--- |
| **Where** | `crypto_hold_worker.go` |
| **Fix** | Claim + process in one `BeginFunc` with `FOR UPDATE SKIP LOCKED` inside txn |
| **Status** | ✅ `ProcessHolds` claim+release in single txn; `TestChaos_CryptoHold_DualWorkerRace` |

**EXPLAIN proof** (`EXPLAIN_AUDIT=1 go test ./internal/payment/... -run TestExplainAudit_PaymentIntentQueries`):

| Query | Plan |
| :--- | :--- |
| `ClaimCryptoHoldForUpdate` | Index Scan on `idx_crypto_holds_status_release` → `FOR UPDATE SKIP LOCKED` |

---

## §2 ClickHouse (M-DB-CH)

### M-DB-CH-1 — `wait_for_async_insert=0` (HIGH) ✅

| | |
| :--- | :--- |
| **Physics** | Client returns before server persists; hidden failures; small parts under load |
| **Fix** | `wait_for_async_insert=1` on write DSN; alert `system.parts` |
| **Semaphore** | Keep `ProcessorChGate` flat |
| **Status** | ✅ `clickhouse_connect.go` write DSN `wait_for_async_insert=1`; `ad_ch_active_parts_max` from partition janitor |

### M-DB-CH-2 — MV insert amplification (HIGH) ✅

| | |
| :--- | :--- |
| **Physics** | Each raw insert triggers ~8 aggregate paths; `uniqExact` in `mv_ml_features_1m_*` is CPU-heavy |
| **Fix** | `uniqCombined`; partition `ml_features_1m`; drop duplicate MV DDL |
| **Status** | ✅ `00007_ml_features_1m_fix.sql` (partition + `uniqCombined` MVs); `deploy/clickhouse/recon_materialized_views.sql` deduped |

### M-DB-CH-3 — Poison-pill single-row fallback (HIGH) ✅

| | |
| :--- | :--- |
| **Where** | `processor.go:448–474` |
| **Physics** | 50k single-row inserts → part explosion |
| **Fix** | DLQ after K failures; metric `ad_ch_single_row_inserts_total`; binary split not per-row |
| **Status** | ✅ `processor_poison_split.go` binary split; metric `ad_ch_single_row_inserts_total`; `TestSplitStoreBatch_BinarySplitNotPerRow` |

### M-DB-CH-4 — Read/write DSN conflation (HIGH) ✅

| | |
| :--- | :--- |
| **Fix** | `CH_READONLY_DSN` + `CHQuery` for management, IVT, margin-guard |
| **Status** | ✅ `ConnectCHReadonly` (no async insert); management/IVT/margin-guard wired; write DSN only for migrations/erasure/shadow scores |

### M-DB-CH-5 — IVT per-row shadow scores (MEDIUM) ✅

| | |
| :--- | :--- |
| **Fix** | `PrepareBatch` for `ml_shadow_scores` |
| **Status** | ✅ `fraud_scoring_shadow_batch.go`; one `PrepareBatch`+`Send` per scan; `TestInsertShadowScores_UsesSinglePrepareBatch` |

---

## §3 Cold-path & syscall tuning (M-DB-SYS)

Tasks derived from Part I §2 syscall analysis.

### M-DB-SYS-1 — Logger fsync batching (OPTIONAL)

| | |
| :--- | :--- |
| **Where** | `pkg/logger/flush_persist.go` |
| **Physics** | fsync rate ≈ min(events/batch_size, 20/sec); NVMe EMA breach triggers priority shedding |
| **Fix** | Raise `FlushBufferSize` to 1–4 MiB on dedicated log volumes; add group-commit (`Fdatasync` every N ms OR M bytes) — same pattern as broker `DurabilityGroupCommit` |
| **DoD** | `ad_log_nvme_write_duration_seconds` EMA stable under peak audit load; priority shedding unchanged |

### M-DB-SYS-2 — CH spool group-commit (OPTIONAL)

| | |
| :--- | :--- |
| **Where** | `internal/ingestion/ch_spool.go` |
| **Physics** | One fsync per `AppendDurably`; acceptable for CH outage failover, not as primary write path |
| **Fix** | Buffer ≤100 ms of batches in memory, one fsync — only if Redis PEL retains unacked messages (crash loses only unspooled window). **Do not** disable spool fsync. |
| **DoD** | Chaos proof: SIGKILL during group-commit window → PEL replays unacked; no duplicate CH inserts |

### M-DB-SYS-3 — Postgres txn batching (covered by M-DB-PG-1/2/3)

| | |
| :--- | :--- |
| **Physics** | Per-campaign txn, idle outbox poll, and per-event outbox updates multiply WAL fsyncs (see Part I §2.2) |
| **Fix** | Batch txns (coefficient N=32), adaptive outbox poll (coefficient backoff), sample/remove ledger flush audit |
| **DoD** | See M-DB-PG-1, PG-2, PG-3 |

---

## §4 Semaphore implementation status

| Layer | Mechanism | eSPX status |
| :--- | :--- | :--- |
| Processor PG | Flat `ProcessorPgGate` | ✅ SEM-P1/P2 |
| Processor CH | Flat `ProcessorChGate` | ✅ SEM-P5 |
| Management PG | Priority `MgmtPgGate` | ✅ SEM-P3 → M-DB-PG-4 |
| Ledger batching | Coefficient N=32 | ✅ M-DB-PG-1 |
| Outbox poll | Coefficient backoff | ✅ M-DB-PG-3 |
| Logger fsync | Group-commit coefficient | 🔲 M-DB-SYS-1 |
| CH spool group-commit | Optional coefficient | 🔲 M-DB-SYS-2 |
| CH weighted gate | Optional Phase 4 | 🔲 only if metrics justify |

---

## §5 Delivery phases

### Phase 0 — Correctness (1 week)

| ID | Item | Files |
| :--- | :--- | :--- |
| 0.1 | Crypto hold txn | `crypto_hold_worker.go` ✅ M-DB-PG-7 |
| 0.2 | Payment lock scope | `payment/service.go` → M-DB-PG-5 ✅ |
| 0.3 | CH readonly DSN | `clickhouse_connect.go`, `cmd/*/main.go` → M-DB-CH-4 |
| 0.4 | `wait_for_async_insert` flag | `config/env.go` → M-DB-CH-1 |

### Phase 1 — IOPS reduction (1–2 weeks)

| ID | Item | Task |
| :--- | :--- | :--- |
| 1.1 | Remove/sample ledger flush audit | M-DB-PG-2 ✅ |
| 1.2 | Multi-campaign ledger txn | M-DB-PG-1 ✅ |
| 1.3 | Outbox adaptive poll | M-DB-PG-3 ✅ |
| 1.4 | PG partial indexes | M-DB-PG-6 ✅ |

### Phase 2 — Management semaphore (1 week)

| ID | Item | Task |
| :--- | :--- | :--- |
| 2.1 | `MgmtPgGate` | M-DB-PG-4 ✅ |
| 2.2 | Wire workers to tiers | `service.go`, recon, volume meter ✅ |

### Phase 3 — ClickHouse hardening (1 week)

| ID | Item | Task |
| :--- | :--- | :--- |
| 3.1 | Poison-pill DLQ cap | M-DB-CH-3 |
| 3.2 | IVT shadow batch insert | M-DB-CH-5 |
| 3.3 | MV `uniqExact` migration | M-DB-CH-2 |

### Phase 4 — Optional coefficients (metrics-driven)

| ID | Item | Task |
| :--- | :--- | :--- |
| 4.1 | `ProcessorPgGate.AcquireWeighted` | metrics-driven |
| 4.2 | Logger group-commit fsync | M-DB-SYS-1 |
| 4.3 | CH spool group-commit | M-DB-SYS-2 |

---

## §6 Alerts to add

```promql
# PG
rate(ad_processor_write_acquire_wait_seconds_sum[5m]) / rate(ad_processor_write_acquire_wait_seconds_count[5m]) > 0.1

# CH parts (external query)
ad_ch_active_parts_max > 100

# Logger
histogram_quantile(0.99, ad_log_nvme_write_duration_seconds) > 0.05

# Spool
rate(ad_ch_spool_append_total[5m]) > 0  # sustained → CH outage
```

---

## §7 Cross-reference

| Document | Role |
| :--- | :--- |
| [DATABASE.md](./DATABASE.md) | Day-to-day rules, index catalog, verification commands |
| [REMEDIATION.md](./REMEDIATION.md) §7 | SEM-P* semaphore backlog |
| [GAPS.md](./GAPS.md) GAP-ENG-02 | Management sem gap |
| [MILESTONE.md](./MILESTONE.md) §0 | D2 mmap spool, SEM-P*, H1 single-writer |
| [GUIDE_CHAOS_RELIABILITY.md](./GUIDE_CHAOS_RELIABILITY.md) | Write-path chaos matrix |
