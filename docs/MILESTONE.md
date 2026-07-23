# Project Milestones — Open Work (Session 2026-07)

This document describes the backlog of unimplemented work across RTB, Lua/sharding, Redis key migration, budget micro-quantization, fraud telemetry, HTTP/1–3 ingress, parsing consolidation, hot path / broker test coverage, XDP/L4 protection, and multi-region deduplication. Only incomplete stages are listed. Baseline production-shipped code is documented in [RTB.md](./RTB.md) §2, [EDGE.md](./EDGE.md), and [GAPS.md](./GAPS.md).

---

## Execution Order

Milestones are ordered **by descending complexity** (XL → S). Within each tier, ordering follows dependencies and platform risk. Parallel tracks are marked `||`.

### Complexity Matrix

| Tier | Milestone | Size | Theme |
| :--- | :--- | :--- | :--- |
| **XL** | M1 | XL | Redis slot migration, key catalog, zero-downtime cutover |
| **XL** | M2 | XL | Elastic triplets (after M1) — see [GAPS.md](./GAPS.md) GAP-SHARD-* |
| **L** | M3 | L | Budget Integrity (Reconciliation) & Write Contention |
| **L** | M4 | L | Multi-region dedup adapter (UUID key, DB ↔ userspace two-factor) |
| **L** | M5 | L / XL (C–D) | HTTP/1–3 ingress: edge terminate + table-DFA + optional H2/H3 |
| **L** | M6 | L | Hot path + broker test coverage, CI gates |
| **L** | M7 | L | RTB exchange surface, OpenRTB 2.6, measurement |
| **L** | M8 | XL | Budget micro-quantization, broker deltas, adaptive quanta |
| **M** | M9 | M | Lua consolidation / Redis RTT reduction |
| **M** | M10 | M | XDP L4 anti-fraud (Tier A–C) |
| **M** | M11 | M | Adaptive fraud telemetry aggregation |
| **M** | M12 | M | Parsing consolidation (DFA / vtproto); OpenRTB ingress refactor |
| **S** | M13 | S | Runtime tuning, installer rollback |

**M2 (elastic triplets)** — no dedicated section. Definition of Done:

- [ ] GAP-SHARD-01..03 closed in GAPS.md.
- [ ] Dynamic `campaign_routing` in PostgreSQL; `ShardOrchestrator` with `routing_epoch`.
- [ ] Capacity-aware slot assign and micro-migration without `AssertBudgetInvariant` violation.
- [ ] **Prerequisite:** M1-02 and M1-04 complete.

### Recommended Sequence

```text
[Tier XL — scale and infrastructure]
M1 (Redis slot migration)  →  M2 (Elastic triplets)
M4 (Multi-region dedup adapter)  — after M1-01; parallel with M2 prep

[Tier L — major architectural tracks]
M5 (HTTP/1–3: A edge H2/H3 → B H1 table-DFA → C H2 optional → D deferred)
  || M6 (Hot path + broker test coverage) — parallel with M5-B / M12
M7 (RTB exchange surface & measurement)
M8 (Budget micro-quantization)  — after M9-02; before M3 reconcile extensions
M3 (Budget integrity & contention)  — extends ReconWorker; after M8-04 broker deltas

[Tier M — medium improvements, partially parallel]
M9 (Lua RTT consolidation)  ||  M10 (XDP)  — hot path relief
M11 (Fraud aggregation)  — after broker produce path
M12 (Parsing consolidation)  — parallel with M5-B

[Tier S — background, non-blocking]
M13 (Runtime tuning + installer rollback) — anytime after baseline

Deferred / optional:
* M5-C (H2 frame FSM on tracker) — only if perf-gate shows gain
* M5-D (H3 end-to-end on tracker) — terminate at edge
* M6-P2 (broker client, load-test, nightly throughput) — after M6-P0/P1
```

---

## Unified Development, Performance, and Testing Standards

All milestone work must comply with platform standards in `docs/` and the root `GUIDE_*.md` files. Authoritative checklists live in those guides; this section summarizes bindings for milestone work.

| Guide | Applies to |
| :--- | :--- |
| [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md) | Package layout (R1), naming (R2), errors (R8), PR verification (R10), lint/codegen (R11) |
| [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) | Ingestion, RTB auction, broker wire — 0 allocs, BCE, padding, DFA vs vtproto |
| [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) | Write paths, budget, Lua, sharding — `chaos_proof`, R10 redundancy rules |
| [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) | Edge, XDP, blocklists — defensive only (§1), no hack-back (§2) |

### 1. Architecture and Code Standards (`GUIDE_STYLE_CODE.md`, `GO.md`, `GUIDE_HOT_PATH_ZERO_ALLOC.md`)

* **Flat package layout:** Do not create nested domain packages (`clean`/`hexagonal` layouts: `domain/`, `usecase/`, `repository/`). Each service lives in a single flat package `internal/<service>/`. Subdirectory exceptions are generated code only (`db/`, `pb/`, `queries/`, `migrations/`).
* **File naming:** snake_case with a domain prefix: `<domain>_<rest>.go` (e.g. `track_core.go`, `service_campaigns.go`, `handler_billing.go`).
* **Type and entity separation:**
  * **Hot-path model:** `internal/domain` (no `json` or `db` struct tags).
  * **SQL strings:** `internal/<svc>/db` (sqlc-generated).
  * **API/DTO:** `DTO` suffix, `json:"snake_case"` tags. Mapping happens strictly at I/O boundaries in one step (no intermediate layers).
* **Error handling:**
  * **Cold path (idiomatic Go):** Named `Err*` in `errors.go`, wrap with `fmt.Errorf("...: %w", err)`, explicit `errors.Is(err, pgx.ErrNoRows)` for 404. Do not hide DB errors or silence them with `_ =`.
  * **Hot path (zero-alloc):** `uint8` enums (`NoBidReason`, `filterRejectKind`) and unformatted `Err*` without `fmt.Errorf`. Client responses use precompiled byte buffers `filterRejectSpecs`.

### 2. Performance Standards (Hot Path) (`GO.md`, `GUIDE_HOT_PATH_ZERO_ALLOC.md`, `RTB.md`, `REDIS.md`)

* **Zero allocations (0 allocs/op):** No heap allocations on parse, filter, and auction hot paths. Verified via `go test -benchmem`. Checklist: `GUIDE_HOT_PATH_ZERO_ALLOC.md` §11.
* **BCE / branch / padding:** Patterns from `GUIDE_HOT_PATH_ZERO_ALLOC.md` §2–4 are mandatory for new parse/filter code.
* **Memory management:** `vtproto` with `appendReuseBytes` patch for byte fields, struct field ordering by descending size, channels and mutexes replaced with atomics and ring buffers.
* **Bounds check elimination (BCE):** Explicit compiler hints at loop entry (e.g. `_ = slice[len-1]`).
* **Time deadlines:** No per-request `context.WithTimeout`. Use monotonic time `evt.FilterDeadlineMono`.
* **Network path:** At most 1 Redis RTT per request via one atomic `EVALSHA` script. **Exception (M8):** in quanta mode budget debit may be local; Redis RTT is amortized over a quantum; dedup/fcap still ≤ 1 RTT (or consolidated Lua M9-02).

### 3. Chaos Testing Standards (`GUIDE_CHAOS_RELIABILITY.md`)

* **Steady-state hypothesis:** Before fault injection, record metrics: RPS on `/track`, p95 < 50 ms, p99 < 80 ms, error rate < 0.1%, no budget drift (`Σ Redis spend + Σ sync deltas = Postgres spend`).
* **Real faults without mocks:** Faults are tested on real containers (`testcontainers-go`, `docker compose`) — process termination (SIGKILL), network delay and broken connections, clock skew. Database mocks are forbidden.
* **`chaos_proof` logging protocol:** Every successful chaos test must print to stdout: `chaos_proof fault=<fault_name> ...`. CI script `scripts/chaos-drills/test_chaos.sh` checks the `CHAOS_MIN_PROOFS` threshold (minimum 52 proofs).
* **Chaos redundancy (R10):** Pure functions, UI, and docs do not need new chaos tests. New write paths, budget debit logic, Lua scripts, and sharding require `fault_*` or `*_chaos_test.go` with `chaos_proof` — see [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) R10 before adding tests.

### 4. Hot Path and Broker Test Coverage Standards (`M6`, `GUIDE_STYLE_CODE.md` R2)

* **Inventory (current state):** `internal/ingestion` — ~120 `*_test.go`, ~68 benchmarks; `tests/e2e` — 5 tests; `pkg/broker` — 18 `*_test.go` + chaos suite; ingestion↔broker — 2 files (`broker_payload_test.go`, `broker_consumer_test.go`).
* **Required levels for new hot-path code:** (1) unit table-driven wire/parser tests, (2) handler integration with prebuilt `resp*`, (3) `go test -benchmem` 0 allocs/op, (4) on write paths — `fault_*` or `*_chaos_test.go` with `chaos_proof` (R2, R10).
* **E2E must match prod topology:** single-shard e2e uses `StaticSlotSharder`, not `JumpHashSharder` (see M6-10).
* **Broker cutover:** live consumer path and reconcile worker are tested before enabling non-shadow mode in production (M6-03, M6-04).
* **CI gates:** `make test-alloc-gate` — ingestion/rtb parse+fraud; nightly broker — `pkg/broker/protocol` only; `BenchmarkBrokerThroughput` and ingestion broker path are **not** in alloc-gate (see M6-14, M6-15).

### 5. Milestone Definition of Done (common)

Every milestone is **closed** only when **all** items below are satisfied, in addition to its milestone-specific checklist. References: [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md), [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md), [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md), [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md).

**Scope and tracking**

- [ ] Every row in the milestone open-task table is done, or moved to backlog with a new gap ID in [GAPS.md](./GAPS.md).
- [ ] Closed gaps removed from GAPS.md; shipped summary added to [ARCHITECTURE.md](./ARCHITECTURE.md) or the relevant domain doc (REDIS, EDGE, RTB).

**Tests and CI** ([GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md) R10, R11)

- [ ] `go test ./... -short` passes for touched packages.
- [ ] `make lint` passes (Staticcheck, errcheck on non-test code per R11.2).
- [ ] `scripts/ci/check_comments.sh` passes for changed files (R9.1).
- [ ] Hot-path milestones: `make test-alloc-gate` passes; `0 allocs/op` on affected benchmarks (R8.3, R10).
- [ ] Hot-path milestones: no new heap escapes on parse/filter/respond (`go test -gcflags='-m' …` per R8.7).
- [ ] Perf-sensitive changes: `bash scripts/perf-gate/perf_gate_run.sh` passes; `docs/hot_path_baseline.md` updated if baseline intentionally shifts (R11.3).

**Chaos** ([GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md))

- [ ] New write paths, budget debit logic, Lua scripts, or sharding: dedicated `fault_*` or `*_chaos_test.go` with `chaos_proof` stdout (R10 — not redundant).
- [ ] `CHAOS_MIN_PROOFS=52 ./scripts/chaos-drills/test_chaos.sh` passes after new chaos scenarios land.

**Hot-path invariants** ([GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) — when milestone touches ingestion, RTB auction, or broker wire)

- [ ] No new `interface{}` / closures in request loops; no `fmt.Sprintf` / `encoding/json` on request path.
- [ ] BCE hints at parse-loop entry; contended atomics cache-line padded.
- [ ] New critical-path benchmarks added to `scripts/perf-gate/perf_gate_bench.sh` when introduced (R11.3).

**Compliance** ([GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) §6 — when milestone touches edge, XDP, blocklists, or autoban)

- [ ] Change is defensive (§1) or neutral — not offensive (§2).
- [ ] Block paths respect `allowlist.IsProtected` before deny; no outbound traffic to visitor/source IPs from workers.
- [ ] `scripts/ci/check_compliance.sh` passes.

**Observability and ops**

- [ ] New metrics and alerts listed in the milestone are wired and visible on the ops dashboard.
- [ ] Runbooks / DEVELOPMENT.md updated when operator behavior changes.

Optional phases (e.g. M5-C/D, M1-08) do **not** block milestone closure unless explicitly listed in that milestone's Definition of Done.

---

## Milestones (Descending Complexity)

## M1 — Slot Migration and Redis Key Catalog

**Size:** XL · **References:** GAP-SHARD-01..05, GAP-HOT-04

### Already shipped (out of scope)

`StaticSlot` sharding, `redis_migrate.go` data move (`DUMP`/`RESTORE`), `SlotMigrationOrchestrator`, PostgreSQL slot migration schema, `edge-bpf-sync` synchronizer, `MIGRATION_FENCE_ENABLED` barrier (default **false**, **true** in staging k8s).

### Open critical tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M1-01 | Unified `CampaignRedisKeyCatalog` | Single key list for migration, warm-up, and documentation, aligned with hash tags in `redis_keys.go`. | Single catalog used by migrator and docs |
| M1-02 | Fix migrator key namespace | Copy keys like `{uuid}budget:campaign:{uuid}`, not `budget:campaign:{uuid}`. Include dedup keys, idempotency keys, rate-limit, impression timestamps, placement blocklists, and quotas. | `EXISTS` check for hash-tagged key after COPY |
| M1-03 | Post-copy `EXISTS` verification | Block slot activation if required keys are missing on the target shard. | Orchestrator rejects activation on missing keys |
| M1-04 | Cutover via PG re-warm | Cutover flow: set fence → pause activity → warm target shard from PostgreSQL → bump epoch → clean old shard. | `AssertBudgetInvariant` passes during migration |
| M1-05 | Enable `MIGRATION_FENCE_ENABLED` in production | Default **true** in production. Call `BumpMigrationFences` on orchestrator start. | Debit attempt during COPY rejected by fence |
| M1-06 | Key delta sync | Sync changes between COPY and activation, or document PG re-warm-only migration policy. | Documented policy; no stale data on COPY |
| M1-07 | Rollback playbook | Document slot map rollback and target shard cleanup/re-warm from PostgreSQL. | Procedure added to DEVELOPMENT.md |
| M1-08 | Zero-downtime cutover (dual-write / lag catch-up) | **Phase 2, after M1-04/05.** During slot migration, debit continues on source shard; deltas are replicated to an async catch-up queue on target (broker topic or Redis stream). `StaticSlotSharder.SwapSnapshot` only when `replication_lag_messages < ε` and `AssertBudgetInvariant` on a slot campaign sample. Fence remains fallback when lag > threshold or COPY without PG re-warm. | Chaos SO-02: no drop storm; p99 `/track` degrades ≤ 10% during hot-slot migration |
| M1-09 | Lag catch-up metrics | `ad_slot_migration_lag_messages`, `ad_slot_migration_dual_write_total`, `ad_slot_migration_cutover_blocked_total`. | Dashboard + alert when lag > 30 s |

**Dependency:** Blocks M2 elastic triplets until M1-02 and M1-04 tests pass. M1-08 does not replace the fence-first path (M1-04/05); it adds a mode for high-traffic slots after proven consistency.

### Definition of Done

- [x] M1-01: `CampaignRedisKeyCatalog` is the single source for migrator, warm-up, and REDIS.md.
- [x] M1-02: migrator copies hash-tagged keys (`{uuid}budget:campaign:{uuid}`); dedup, idempotency, rate-limit, impression timestamps, placement blocklists, and quotas included.
- [x] M1-03: orchestrator rejects slot activation when required keys are missing on target (`EXISTS` gate).
- [x] M1-04: fence → pause → PG re-warm → epoch bump → old-shard cleanup; `AssertBudgetInvariant` passes during staged migration.
- [x] M1-05: `MIGRATION_FENCE_ENABLED=true` in production; `BumpMigrationFences` on orchestrator start.
- [x] M1-06: COPY vs activation delta policy documented (sync or PG re-warm-only); no stale keys on cutover.
- [x] M1-07: rollback playbook in DEVELOPMENT.md (slot map revert, target cleanup/re-warm).
- [x] M1-09: `ad_slot_migration_lag_messages`, `ad_slot_migration_dual_write_total`, `ad_slot_migration_cutover_blocked_total` on dashboard; alert when lag > 30 s.
- [x] Debit during COPY returns code `11 debit fenced`.
- [x] Chaos LUA-10 and SO-02 emit `chaos_proof`; `CHAOS_MIN_PROOFS` threshold still met.
- [x] M2 prerequisite satisfied: M1-02 and M1-04 verification green in CI.
- [x] GAP-SHARD-01..05 and GAP-HOT-04 closed or updated in GAPS.md.
- [ ] *Optional phase 2 (M1-08):* zero-downtime dual-write cutover for hot slots — p99 `/track` degrades ≤ 10% during migration.

### Verification

```bash
go test ./internal/ingestion/... -run 'Migrate|Slot|RedisKey' -short
go test ./internal/management/... -run SlotMigration -short
./scripts/chaos-drills/test_chaos.sh   # LUA-10, SO-02 scenarios
```

---

## M3 — Budget Integrity (Reconciliation) and Write Contention ✅

**Size:** L · **References:** GAP-HOT-02, M8-04 · **Depends on:** broker delta path (M8-04) for local-quanta visibility · **Extends:** existing `ReconWorker`, `ReconService`, `quota_repair.go` — **do not** add a parallel reconciler.

**Context:** Budget state is split across Redis (operational), Postgres (financial ledger), and — after M8 — worker-local quanta and broker deltas. Drift is expected during the async sync window. Today `ReconService.ReconcileWindow` compares hourly ledger totals to `budget:sync:campaign` only; `quota_repair` compares PG `campaign_quotas` to `quota + sync + inflight`. This milestone unifies the reconciliation model, fixes false positives, and reduces Postgres row contention on `SyncWorker` flushes.

**Scope:** Cold path only. No impact on `/track` SLA (p95 < 50 ms).

### Reconciliation authority (single hierarchy)

| Layer | Source of truth | Used for |
| :--- | :--- | :--- |
| Hot debit | Redis Lua (`budget:campaign` or `budget:quota`) | Real-time accept/reject |
| In-flight settlement | Redis `budget:sync` + `budget:inflight` | Pending PG flush |
| Local quanta (M8) | Worker RAM + broker `budget-deltas` topic | Amortized debit |
| Financial ledger | Postgres `balance_ledger` + `campaigns.current_spend` | Billing, pause, admin |
| Corrections | Outbox events (`QUOTA_REPAIR`, `RECONCILIATION_ADJUST`) only | Never direct Redis `SET` under load |

**Invariant (per campaign, QUOTA_MODE off):**

```text
budget_limit - current_spend_pg
  ≈ budget_remaining_redis + budget_sync_redis + budget_inflight_redis + broker_pending_deltas
```

Tolerance `ε` = max(1 micro-unit, 0.01% of `budget_limit`). Grace window = `LEDGER_BATCH_FLUSH_MS + BUDGET_SYNC_INTERVAL_MS` before flagging drift.

### Risks and mitigations

| ID | Risk | Edge case | Mitigation | Task |
| :--- | :--- | :--- | :--- | :--- |
| R3-01 | **Duplicate reconcilers** | New worker fights `ReconService` / `quota_repair` | Extend `ReconWorker`; shared `recon_discrepancies` table | M3-00 |
| R3-02 | **False drift** | Reconcile runs while `inflight` > 0 | Grace window; skip campaigns with `budget:lock:*` or active inflight | M3-07 |
| R3-03 | **Non-atomic Redis read** | Pipeline GET interleaved with Lua debit | M3-02 Lua snapshot script (single `EVALSHA`) | M3-02 |
| R3-04 | **Wrong formula** | Compare to `budget:campaign` as "target" | Use invariant above; branch on `QUOTA_MODE` | M3-08 |
| R3-05 | **Local quanta blind spot** | M8 RAM spend invisible to Redis snapshot | Include broker delta lag in formula; M8-04 prerequisite | M3-08, M8-04 |
| R3-06 | **Conflicting authority** | M3 adjusts PG while `ReconService` adjusts Redis | Document hierarchy; corrections via outbox only | M3-06, M3-10 |
| R3-07 | **Over-correction under load** | ADJUSTMENT while traffic active | Enqueue `RECONCILIATION_ADJUST` outbox; chunk cap like `autoAdjustChunkMicro` | M3-04, M3-10 |
| R3-08 | **SKIP LOCKED starvation** | Hot campaign skipped every tick → longer inflight | Fair round-robin dirty-set scan; alert `sync_lag_seconds` | M3-03, M3-09 |
| R3-09 | **SKIP LOCKED extended overspend** | Delayed flush widens PG lag window | Do not SKIP on campaigns within `STRICT_THRESHOLD`; semaphor + SKIP are complementary | M3-03 |
| R3-10 | **Full PG scan cost** | 100k+ campaigns per tick | Dirty-set driven (`budget:dirty_campaigns`) + sampled hourly ledger recon | M3-09 |
| R3-11 | **Dead shard false positive** | Reconciler reads empty Redis on dead shard | Reuse `ShardQuorumTracker` skip policy from `quota_repair` | M3-12 |
| R3-12 | **Slot migration** | Keys on source and target shard during COPY | Skip campaigns with `migration_fence` or `MIGRATION_FENCE_ENABLED`; coordinate M1 | M3-11 |
| R3-13 | **Customer-level drift** | `budget:sync:customer` not in formula | Optional customer rollup in sampled recon | M3-13 |
| R3-14 | **`budget:lock` TTL expiry** | Lock expires (60s) during slow PG txn | Extend lock TTL to PG p99 flush latency; metric `sync_lock_expired_total` | M3-14 |

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M3-00 | **Extend `ReconWorker`** | Add `ReconcileBudgetSnapshot` to existing worker; no second reconciler process. | Single worker owns all recon paths |
| M3-01 | **Unified snapshot reconcile** | Per-campaign invariant check; write `recon_discrepancies`; respect grace window. | False positive rate < 0.1% in chaos |
| M3-02 | **Snapshot Lua script** | Atomic read: `budget:campaign`, `budget:sync`, `budget:inflight`, optional `budget:quota` in one `EVALSHA`. | `BenchmarkReconcileSnapshot` < 2 ms |
| M3-03 | **Contention: SKIP LOCKED + gate** | `SyncWorker` batch claim: `SKIP LOCKED` on campaign row where safe; keep `ProcessorPgGate` for pool cap. Strict-band campaigns use blocking lock. | `pg_stat_activity` lock wait −50% |
| M3-04 | **Outbox corrections** | Drift > ε → enqueue `RECONCILIATION_ADJUST` (not inline PG write during RPS). Reuse chunk cap from `ReconService`. | Audit trail + idempotent apply |
| M3-05 | **Metrics & alerts** | `ad_reconciliation_drift_micro`, `ad_reconciliation_corrections_total`, `ad_sync_lag_seconds`. | Dashboard; alert drift > $50 / 1 h |
| M3-06 | **Authority doc** | DATABASE.md §Reconciliation hierarchy (table above). | No conflicting auto-fix paths |
| M3-07 | **Grace window** | Skip reconcile when `inflight > 0` and `now - last_flush < grace`. | Zero false drift in SyncWorker chaos |
| M3-08 | **QUOTA_MODE + broker terms** | Formula branches: quota path adds `campaign_quotas.reserved`; broker adds pending delta sum. | M8 shadow diff included |
| M3-09 | **Dirty-set scan** | Primary loop from `budget:dirty_campaigns` + SSCAN; full PG scan nightly sample only. | O(dirty) not O(campaigns) per tick |
| M3-10 | **Outbox-only apply** | Corrections applied by `OutboxWorker` / existing repair handlers. | No direct Redis mutation from recon goroutine |
| M3-11 | **Migration fence skip** | Skip campaigns under `budget:migration_fence` or orchestrator COPY state. | SO-02 chaos: no false correction |
| M3-12 | **Dead shard policy** | Skip reconcile when `ShardQuorumTracker.DeadShardConfirmed`. | Align with `quota_repair` |
| M3-13 | **Customer sync (optional)** | Sample `budget:sync:customer` vs customer ledger. | Documented scope |
| M3-14 | **Lock TTL audit** | `budget:lock` EX ≥ p99 `UpdateSpendBatch` + buffer; metric on expiry. | Chaos: no double prepare |
| M3-15 | **Chaos: recon under load** | Reconcile during active `/track` RPS + mid slot migration. | `chaos_proof fault=recon_under_load` |

### Definition of Done

- [x] M3-00: `ReconcileBudgetSnapshot` on existing `ReconWorker`; no parallel reconciler process.
- [x] M3-01: unified per-campaign invariant check → `recon_discrepancies`; grace window skips inflight false positives.
- [x] M3-02: atomic Lua snapshot (`FetchBudgetReconSnapshot`); `BenchmarkReconcileSnapshot` ≪ 2 ms.
- [x] M3-03: `FOR UPDATE SKIP LOCKED` on campaign flush when outside strict band; `ProcessorPgGate` unchanged.
- [x] M3-04: drift > ε → `RECONCILIATION_ADJUST` outbox with chunk cap from `ReconService`.
- [x] M3-05: `ad_reconciliation_drift_micro`, `ad_reconciliation_corrections_total`, `ad_sync_lag_seconds`; Prometheus alert `ReconciliationDriftHigh` (> $50 / 1 h).
- [x] M3-06: reconciliation authority table in DATABASE.md §Reconciliation authority.
- [x] M3-07: grace window skips reconcile when `inflight > 0` and `updated_at` within `LEDGER_BATCH_FLUSH_MS + BUDGET_SYNC_INTERVAL_MS`.
- [x] M3-08: QUOTA_MODE reads `budget:quota` in snapshot Lua; broker pending hook on `Service.brokerDeltas` (zero until M8-04).
- [x] M3-09: primary loop scans `budget:dirty_campaigns` with fair SSCAN cursor; hourly `ReconcileWindow` for ledger sample.
- [x] M3-10: corrections applied only by `OutboxWorker.ApplyReconciliationAdjust`; recon goroutine never mutates Redis.
- [x] M3-11: skip campaigns with `budget:migration_fence` present in snapshot.
- [x] M3-12: skip dead shards via `ShardQuorumTracker.DeadShardConfirmed` (aligned with `quota_repair`).
- [ ] *Optional (M3-13):* customer-level `budget:sync:customer` sample recon — deferred; not required for M3 closure.
- [x] M3-14: `budget:lock` TTL = `BudgetLockTTLSeconds(flush + sync + buffer)`; `sync_lock_expired_total` metric.
- [x] M3-15: `TestChaos_ReconUnderLoad` emits `chaos_proof fault=recon_under_load`.
- [x] Reconciliation authority documented in DATABASE.md; single `ReconWorker` owns all recon paths.
- [x] Risks R3-01..R3-12, R3-14 covered by unit/integration/chaos tests; R3-13 optional deferred.
- [x] `AssertBudgetInvariant` passes after snapshot recon + outbox apply (`TestChaos_ReconUnderLoad`).
- [x] `CHAOS_MIN_PROOFS` threshold still met (`recon_under_load` proof added).
- [x] GAP-HOT-02 updated in GAPS.md (broker term remains on M8-04).

### Verification

```bash
go test ./internal/management/... -run 'Recon|QuotaRepair' -short
go test ./internal/ingestion/... -run 'Sync|Ledger' -short
./scripts/chaos-drills/test_chaos.sh   # recon_under_load
```

---

## M4 — Multi-Region Dedup Key Adapter (DB ↔ Userspace) ✅

**Size:** L · **References:** GAP-GEO-01, [MULTI_REGION.md](./MULTI_REGION.md), [DATABASE.md](./DATABASE.md), M1-01 · **Depends on:** unified key catalog (M1-01); **blocks:** safe at-least-once apply for `RegionOutboxRelay` and cross-region `sync_idempotency`

**Context:** Regional cells deliver control-plane and settlement events **at-least-once**. Today `sync_idempotency` keys are often non-deterministic (`uuid.New()` per prepare cycle in `SyncWorker`), so a worker restart mid-batch can mint a new `txID` and bypass `ON CONFLICT DO NOTHING`. This milestone adds a **Deterministic Dedup Adapter (D3 v2)** that binds idempotency to **logical scope** (stable source + sequence range) and a **payload hash** (`factor_u`), with a DB receipt (`factor_d`) before any durable side-effect.

**Scope:** Cold path only — `SyncWorker`, `RegionOutboxRelay`, processor stream settlement, `QuotaRepo` chunk reserve (optional). **Forbidden on `/track` hot path.**

### Dedup key layout (D3 v2)

The dedup key must be **identical on retry** for the same logical batch.

```text
                    DEDUP KEY v2  (cold path)
  +-----------------------------------------------------------------------+
  | STABLE SCOPE                                                          |
  |   region_id    UUID    regional cell (REGION_CODE registry)           |
  |   source_id    UUID    worker group / stream partition / relay lane   |
  |   source_epoch UINT32  topic generation / routing epoch (anti-reset)    |
  |   seq_start    BIGINT  first offset, inflight gen, or min_event_id    |
  |   seq_end      BIGINT  last offset or max_event_id in batch (inclusive)|
  +-----------------------------------------------------------------------+
  | TWO-FACTOR PROOF                                                      |
  |   factor_u     UUID    SHA-256 prefix of canonical batch payload      |
  |   factor_d     UUID    DB receipt at Confirm (random UUID v4)         |
  +-----------------------------------------------------------------------+
  | dedup_key (TEXT) = FormatCanonical(scope, factor_u, factor_d)         |
  | sync_idempotency.id = dedup_key                                       |
  | relay Redis (opt): SET NX dedup/v2:{dedup_key}                        |
  +-----------------------------------------------------------------------+
```

**Workflow (single PG function preferred — see M4-03):**

1. **Claim:** Userspace computes `factor_u`, calls `dedup_claim_confirm(scope, factor_u)`. PG returns `already_confirmed` | `confirmed` + `factor_d` | `hash_mismatch` | `pending`.
2. **Apply:** On `confirmed`, userspace applies side-effects (Redis / ledger), then inserts `sync_idempotency(id = dedup_key) ON CONFLICT DO NOTHING`.
3. **Replay:** On `already_confirmed`, skip apply if `sync_idempotency` row exists; otherwise **resume apply** (crash recovery).

### Risks and mitigations

| ID | Risk | Edge case | Mitigation | Task |
| :--- | :--- | :--- | :--- | :--- |
| R4-01 | **Unstable SSID** | `sequence_id` = start only; retry merges different amounts (`retainCampaignRollup`) | SSID = `(source, source_epoch, seq_start, seq_end)`; `factor_u` = hash of sorted `(campaign_id, amount)` pairs | M4-01, M4-10 |
| R4-02 | **Hash mismatch** | Same SSID, different `factor_u` (corruption or topic reuse) | `status = rejected`; alert `ad_dedup_mismatch_total`; no apply | M4-10 |
| R4-03 | **Pending deadlock** | Worker dies after Propose, before Confirm | TTL 24 h → `rejected`; partial index + janitor | M4-07 |
| R4-04 | **Confirmed, apply incomplete** | PG `confirmed` but Redis/ledger not updated | Replay: `already_confirmed` → resume apply idempotently | M4-04, M4-09 |
| R4-05 | **Relay apply-before-claim** | Today `region_apply_idempotency` inserted **after** Redis apply | Claim in PG **before** Redis mutation; or Redis `SET NX` then PG confirm | M4-06, M4-14 |
| R4-06 | **Partial relay batch** | `ProcessPendingWithCount(500)` — some events applied, some failed | Per-event SSID (`outbox_event_id`), not batch-min-id only | M4-05 |
| R4-07 | **Broker offset reset** | Topic recreated; same offset, different payload | `source_epoch` from `routing_epoch` or broker topic generation | M4-11 |
| R4-08 | **Wrong SSID source for SyncWorker** | SyncWorker has no broker partition; uses Redis dirty sets | SSID = `(shard_id, campaign_id, inflight_generation)` from `budget:txid:*` + rollup window | M4-04 |
| R4-09 | **Customer sync omitted** | `syncCustomers` uses same `prepareSyncScript` + random `txID` | Extend adapter to customer prefix or document out-of-scope + separate gap | M4-13 |
| R4-10 | **Scope overlap** | `quota:` / ivt `sync_idempotency` prefixes coexist | Document boundary: D3 for settlement batches only; keep prefix keys for quota/IVT | M4-13 |
| R4-11 | **CH settlement** | Processor CH path has separate dedup token | Processor PG group uses D3; CH keeps `insert_deduplicate` token — no double-count | M4-15 |

### Already shipped (out of scope)

`sync_idempotency` + `ON CONFLICT DO NOTHING` in `campaign_repo.go`, `quota_repo.go`, ivt-detector; `RegionOutboxRelay` + `region_apply_idempotency`; `control_plane_epochs`; per-region ingress keys in `region_keys.go`.

### Tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M4-01 | **`pkg/dedupkey` v2** | `Scope` with `seq_start`/`seq_end`/`source_epoch`; `FormatCanonical` / `ParseCanonical`; `factor_u` from canonical payload bytes. No `pgx` import. | Golden-vector: same batch → same SSID + `factor_u` |
| M4-02 | **PG `dedup_key_proposals`** | `UNIQUE(region_id, source_id, source_epoch, seq_start, seq_end)`; statuses `pending`/`confirmed`/`rejected`. sqlc: claim, confirm, get. | Migration applies |
| M4-03 | **`dedup_claim_confirm` SQL** | Single round-trip: verify `factor_u`, mint `factor_d`, or return `already_confirmed` / `hash_mismatch`. Mirrors Go `FormatCanonical`. | Go == PG golden vectors |
| M4-04 | **`DedupAdapter` + SyncWorker** | SSID from `(shard, campaign, inflight_gen)`; integrate before `UpdateSpendBatch`. | SIGKILL mid-flush → 0 extra ledger rows |
| M4-05 | **RegionOutboxRelay** | Per-event SSID = `outbox_event_id`; claim-before-apply. | Duplicate delivery → single Redis mutation |
| M4-06 | **Relay Redis NX** | Optional `SET NX dedup/v2:{dedup_key}` on regional shard; align M1 key catalog. | Second apply → NX miss, no op |
| M4-07 | **Proposal TTL** | `pending` > 24 h → `rejected`; janitor partial index. | DEVELOPMENT.md runbook |
| M4-08 | **Observability** | `ad_dedup_proposal_total{status}`, `ad_dedup_mismatch_total`, `ad_dedup_confirm_latency_seconds`. Alert mismatch > 0. | Dashboard |
| M4-09 | **Chaos: crash recovery** | SIGKILL after Confirm, before apply; replay resumes without double spend. | `chaos_proof fault=dedup_crash_recovery` |
| M4-10 | **Hash mismatch policy** | `rejected` row + metric; never overwrite `confirmed` with different `factor_u`. | Table-driven PG test |
| M4-11 | **`source_epoch` in scope** | Wire `routing_epoch` / broker topic generation into SSID. | Offset-reset fixture passes |
| M4-13 | **Scope boundary doc** | Customer sync, quota, IVT: in or out of D3; no silent overlap. | DATABASE.md §Idempotency updated |
| M4-14 | **Claim-before-apply (relay)** | PG claim or Redis NX **before** `handleOutboxEvent` side-effects. | Chaos: crash mid-apply → no double write |
| M4-15 | **Processor PG path** | Stream consumer batch SSID from `(partition, offset_start, offset_end)` + `source_epoch`. | Broker redelivery → 0 duplicate events |

### Definition of Done

- [x] M4-01: `pkg/dedupkey` — `Scope`, `FormatCanonical`, `ParseCanonical`, `FactorU`; golden-vector tests pass.
- [x] M4-02: `dedup_key_proposals` migration `00049` with SSID unique constraint and status column.
- [x] M4-03: `dedup_claim_confirm` + `dedup_format_key`; Go == PG golden vector (`TestDedupFormatKey_SQLGoldenVector`).
- [x] M4-04: `DedupAdapter` + `SyncWorker.SetDedupAdapter`; SSID from `(shard, campaign, inflight_gen)`; claim before `UpdateSpendBatch`.
- [x] M4-05: `RegionOutboxRelay` per-event SSID = `outbox_event_id`; claim-before-apply.
- [x] M4-06: `SET NX dedup/v2:{dedup_key}` on regional shard; `dedup/v2:` in `CampaignRedisKeyCatalog`.
- [x] M4-07: `pending` > 24 h → `rejected` in PG function; hourly `RejectStaleDedupProposals` janitor on management.
- [x] M4-08: `ad_dedup_proposal_total{status}`, `ad_dedup_mismatch_total`, `ad_dedup_confirm_latency_seconds` registered.
- [x] M4-09: `TestChaos_DedupCrashRecovery`, `TestChaos_DedupResumeApply` emit `chaos_proof fault=dedup_crash_recovery` / `dedup_resume_apply`.
- [x] M4-10: `hash_mismatch` policy in PG; `TestDedupClaimConfirm_GoMatchesPGFormat` table-driven.
- [x] M4-11: `source_epoch` from `control_plane_epochs` via `LoadRoutingEpoch`.
- [x] M4-13: customer sync, quota, IVT out of D3 scope — `DATABASE.md` §Idempotency updated.
- [x] M4-14: relay PG claim + Redis NX **before** `handleOutboxEvent`; `TestChaos_DedupMultiRegionDuplicate`.
- [x] M4-15: broker PG consumer batch SSID from `(partition, offset_start, offset_end)` + `source_epoch`.
- [x] All rows in **Risks and mitigations** (R4-01..R4-11) covered by implementation and chaos/integration tests.
- [x] [MULTI_REGION.md](./MULTI_REGION.md) §Idempotency updated; GAP-GEO-01 partial close in [GAPS.md](./GAPS.md).
- [x] Adapter **not imported** from `/track` handler or `FilterEngine`.
- [x] Technical report: [M4_TECHNICAL_REPORT.md](./M4_TECHNICAL_REPORT.md).

### Verification

```bash
go test ./pkg/dedupkey/... -count=1
go test ./internal/dedup/... -count=1
go test ./internal/ingestion/... -run 'Dedup|Sync' -short
go test ./internal/management/... -run 'RegionOutbox|Dedup' -short
go test ./internal/ingestion/... ./internal/management/... -run 'Chaos_Dedup' -count=1
./scripts/chaos-drills/test_chaos.sh   # dedup_crash_recovery, dedup_multi_region_duplicate
```

---

## M5 — HTTP/1.1–3 Ingress: DFA Parsers and Multi-Protocol Edge

**Size:** L (phases A–B) / XL (phases C–D) · **References:** GAP-EDGE-HTTP (new), [EDGE.md](./EDGE.md), [GO.md](./GO.md)

### Diagnosis (current code state)

| Layer | Fact | Problem |
| :--- | :--- | :--- |
| **Edge** (`deploy/nginx/nginx.conf`) | `listen 8180` plain HTTP; `proxy_http_version 1.1` to upstream | No TLS, no `http2`/`http3`/`quic` on listener; H2/H3 clients not served at edge |
| **Tracker** (`handler.go:parseHTTP`) | `bytes.Index` on `\r\n`, manual header parsing | **Not** table-driven DFA (contrary to GO.md); no chunked TE, no upgrade; works only for narrow nginx POST `/track` subset |
| **Body DFA** | `ParseTrackRequestJSON` (schema FSM), `edge-parse-dfa.lua` (proto varint FSM) | Works; reuse pattern for H2/H3 binary frames |
| **H2/H3 in repo** | Absent | gnet accepts TCP byte stream only; QUIC/H3 needs a separate transport stack |

**Key conclusion:** "DFA for HTTP/2/3" is **not** a text byte-scanner like HTTP/1.1. H2/H3 are binary frame FSM + HPACK/QPACK. Reasonable strategy: **terminate H2/H3 at edge**; on tracker — either H1.1 (phase A) or a thin H2 frame layer (phase C). H3 end-to-end on gnet is impractical (needs QUIC; see quic-go).

### Architecture model (4 phases)

```text
Client -> TLS ALPN (h3|h2|http/1.1) -> Edge (OpenResty) -> Tracker (gnet)
              [M5-A]                    [M5-A/B/C]
```

| Phase | Where | Protocol | Parser |
| :--- | :--- | :--- | :--- |
| **A** | Edge | H2/H3 terminate → H1.1 upstream | nginx/http3 module (no custom DFA) |
| **B** | Tracker | H1.1 only | Table-driven request-line + header FSM (replaces `parseHTTP`) |
| **C** | Tracker | H2 cleartext (h2c) or TLS+h2 from edge | 9-byte frame header FSM + subset HPACK |
| **D** | Tracker | H3 | **Deferred** — quic-go sidecar or edge-only |

### DFA / FSM ideas (practice and RFC)

#### HTTP/1.1 — true table-DFA

Reference: [llhttp](https://github.com/nodejs/llhttp) (llparse → C, zero-copy span callbacks). For Go hot path — two options:

1. **Subset table-FSM in Go** (recommended): generate `[256][state]next` only for expected `/track` headers (`content-type`, `content-length`, `x-forwarded-for`, `user-agent`, …). States: `REQ_LINE → HEADER_NAME → HEADER_VALUE → BODY`. Incremental feed from gnet ring buffer. 0 allocs via `connContext` scratch.
2. **CGO llhttp** (fallback): battle-tested, but CGO breaks 0-alloc/inlining gate — cold path or benchmark comparison only.

Speculative fast-path from `IDEAS_MICROSERVICES_EXPANSION.md` §7.1.2: if first 8 bytes match `POST /tr` — skip to known-header offset; mismatch → full FSM.

#### HTTP/2 — frame FSM (not text DFA)

RFC 9113. Layers:

| Layer | FSM | Details |
| :--- | :--- | :--- |
| **Wire** | `decode_frame` | 24-bit length + 8-bit type + flags + 31-bit stream_id (9 bytes). Table lookup `type → handler`. Reference: [zerodds-http2](https://github.com/zero-objects/zero-dds/tree/main/crates/http2) (no_std, zero-copy payload `&[u8]`). |
| **Connection** | preface → SETTINGS → ACK | `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` check; reject unknown frame types |
| **Stream (server)** | idle → open → half-closed | One request per stream (like H3); for `/track`, HEADERS + DATA is enough |
| **HPACK** | static table index + integer FSM | Tracker subset: whitelist 6–8 pseudo/regular headers (`:method`, `:path`, `content-type`, `content-length`); dynamic table — fixed size 0 or 32 entries; **not** full HPACK |

Variable-length HPACK uses the same varint-DFA as `edge-parse-dfa.lua` (`decode_varint`).

Tracker-H2 constraints: `ENABLE_PUSH=0`, `MAX_CONCURRENT_STREAMS` cap, `MAX_FRAME_SIZE=16384`, POST `/track` and `/openrtb/bid` only.

#### HTTP/3 — varint frame FSM + QPACK

RFC 9114. Differences from H2:

| Aspect | H2 | H3 |
| :--- | :--- | :--- |
| Transport | TCP (+ TLS) | QUIC (UDP) |
| Frame header | Fixed 9 bytes | Varint type + varint length |
| Headers | HPACK | QPACK (two unidirectional streams) |
| Flow control | WINDOW_UPDATE frame | QUIC stream credit (transport layer) |

**Varint decoder FSM** (reuse from protobuf):

```text
state 0: read byte b
  if b < 0x40  → value=b, done
  if b < 0x80  → acc=(b&0x3f), shift=6, state=1
  ...
```

For eSPX: **H3 terminate at edge** (nginx `http3 on` / `listen 443 quic`). Tracker does not parse QUIC. If H3 to tracker is ever needed — separate `quic-go` process with the same varint frame FSM, proxying to `processTrack` (not gnet).

### Already shipped (out of scope)

`ParseTrackRequestJSON` (schema DFA), `edge-parse-dfa.lua` (proto wire FSM), prebuilt `resp*` byte buffers for H1.1 responses, nginx keepalive upstream pool.

### Open tasks

#### Sprint A — Edge H2/H3 (P0)

| ID | Task | Details | Done when |
| :--- | :--- | :--- | :--- |
| M5-A1 | TLS listener `443 ssl http2` | `listen 443 ssl http2;`, certificates, `ssl_protocols TLSv1.2 TLSv1.3;` | `curl --http2 https://edge/track` → 202 |
| M5-A2 | HTTP/3 on edge | nginx ≥ 1.25: `listen 443 quic reuseport; http3 on;`, Alt-Svc header | `curl --http3-only` succeeds |
| M5-A3 | ALPN + downgrade | `ssl_alpn` h3, h2, http/1.1; upstream stays `proxy_http_version 1.1` | Backend sees H1.1 only; metric `edge_ingress_protocol` |
| M5-A4 | Fraud header forwarding | `:method`/`:path` → `X-Original-*`; TLS JA3/JA4 hash in `X-TLS-Hash` for tracker | FraudFilter gets same signals over H2/H3 |

#### Sprint B — H1.1 table-DFA on tracker (P0)

| ID | Task | Details | Done when |
| :--- | :--- | :--- | :--- |
| M5-B1 | `http1_fsm.go` | Table-driven incremental parser; replaces `parseHTTP`; conn-scratch for header slots | `BenchmarkHTTP1Parse` 0 allocs/op; nginx proxy request corpus |
| M5-B2 | Pipelining / partial | Correct `OnTraffic` loop: N requests in one read buffer | Chaos: 10 pipelined POST → 10× 202 |
| M5-B3 | Chunked TE (optional) | If edge ever sends chunked — FSM state `CHUNK_SIZE` | Unit test chunked body |
| M5-B4 | GO.md sync | Remove "DFA scanner" until B1 ships; or document real FSM | GO.md matches code |

#### Sprint C — H2 frame FSM on tracker (P1, optional)

| ID | Task | Details | Done when |
| :--- | :--- | :--- | :--- |
| M5-C1 | `h2_frame.go` | `decode_frame` / `encode_frame`, connection preface, SETTINGS exchange | 0 allocs/op on 9-byte header decode |
| M5-C2 | Subset HPACK decoder | Static table only + indexed/literal FSM for `/track` headers | Decode typical POST without dynamic table |
| M5-C3 | gnet TLS + ALPN | `crypto/tls` listener on gnet or reverse-proxy h2c Unix socket | perf-gate: H2 upstream nginx → tracker h2c |
| M5-C4 | H2 response encoder | HEADERS + DATA frames for prebuilt `resp*` (no `fmt`) | 0 allocs/op on 202 response |

#### Sprint D — H3 end-to-end (P3, deferred)

| ID | Task | Details | Done when |
| :--- | :--- | :--- | :--- |
| M5-D1 | quic-go sidecar evaluation | `cmd/tracker-quic` on quic-go, same `processTrack`, not gnet | Spike doc; not in prod without perf-gate |
| M5-D2 | QPACK subset decoder | Reuse varint FSM from M5-C | — |

**Default decision:** A + B solve "H2/H3 clients, H1 tracker". C — only if perf-gate shows gain from H2 edge→tracker (unlikely with keepalive pool). D — do not build on gnet.

### Definition of Done

**Phase A — edge (required)**

- [ ] M5-A1: TLS `443 ssl http2` listener; `curl --http2 https://edge/track` → 202.
- [ ] M5-A2: HTTP/3 on edge (`http3 on`); `curl --http3-only` succeeds.
- [ ] M5-A3: ALPN h3/h2/http/1.1; upstream H1.1 only; `edge_ingress_protocol` metric.
- [ ] M5-A4: fraud headers reach tracker (`X-TLS-Hash`, `X-Original-*`); passive TLS metadata only per [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) §1.B.

**Phase B — tracker H1 FSM (required)**

- [ ] M5-B1: `http1_fsm.go` replaces `parseHTTP`; `BenchmarkHTTP1Parse` 0 allocs/op.
- [ ] M5-B2: pipelining — 10 POST in one read buffer → 10× 202.
- [ ] M5-B4: GO.md matches table-FSM implementation (no misleading "DFA scanner" wording).
- [ ] BCE hints at FSM loop entry per [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §2.
- [ ] No `context.WithTimeout` per request; conn-scratch via `connContext` (§8).
- [ ] No CGO llhttp on production hot path (§1 — benchmark comparison only).
- [ ] New bench registered in `scripts/perf-gate/perf_gate_bench.sh` (R11.3).

**Regression and docs**

- [ ] `handler_validation_test` and `make test-alloc-gate` pass.
- [ ] EDGE.md documents terminate-H2/H3-at-edge, H1.1-to-tracker topology.
- [ ] `scripts/ci/check_compliance.sh` passes (no new offensive surface).

**Does not block closure:** M5-C (H2 on tracker), M5-D (H3 end-to-end), M5-B3 chunked TE unless edge sends chunked bodies in production.

### Verification

```bash
go test ./internal/ingestion/... -run 'HTTP|ParseHTTP|H2' -short
go test ./internal/ingestion/... -bench='BenchmarkHTTP1|BenchmarkH2' -benchmem
make test-alloc-gate
# edge: curl --http2 / --http3-only against :443
nginx -t -c deploy/nginx/nginx.conf
```

---

## M6 — Hot Path and Broker Test Coverage

**Size:** L · **References:** `GUIDE_HOT_PATH_ZERO_ALLOC.md`, `GUIDE_CHAOS_RELIABILITY.md`, M5-B, M12, M8-04 · **Parallel:** M5-B, M12

**Context:** Ingestion unit/chaos coverage is strong (Lua/unified filter, StaticSlot, RTB, UDP control logic, migration fence). Broker server is strong (durability, HA, backpressure, torn write). System gaps are at **wire → handler → broker consumer**, CI alloc-gates, and e2e alignment with prod sharder.

### Coverage map (audit 2026-07)

| Layer | Strengths | Weaknesses |
| :--- | :--- | :--- |
| `internal/ingestion` | StaticSlot, unified Lua, RTB auction/budget, fraud layer, UDP chaos (unit) | `parseHTTP`, full FilterEngine chain, handler↔UDP quota 429 |
| `tests/e2e` | flow JSON+proto, idempotency, multishard (StaticSlot×4), RTB budget, shutdown | no broker path, no UDP ingress, `flow_test` on JumpHash(1) |
| `pkg/broker/server` | segment I/O, torn tail, HA/failover, concurrent produce/fetch | index partial write, fetch `maxBytes` boundary |
| `pkg/broker/client` | — | no `client_test.go` |
| ingestion↔broker | `ParseBrokerPayload`, shadow consumer | live consumer, reconcile, parity with HTTP handler |
| CI gates | zero-alloc parse/proto/fraud/RTB | broker throughput not in gate; broker ingestion not in alloc-gate |

### Already shipped (out of scope)

~120 ingestion tests, chaos suite (`fault_*`, `lua_fastpath_chaos`, `udp_control_chaos`, `migration_fence`, `shard_outage`), e2e multishard, broker server chaos (`chaos_ha_network`, `chaos_durability_lab`), `TestBrokerE2EAllocs`, `requests_parse` happy-path + zero-alloc, `handler_validation_test` (404/405/413/400 health), `handler_reject_test` (6 of 17 filter kinds).

### GAP-1 — HTTP wire (`parseHTTP` / future `http1_fsm.go`)

**No direct unit tests** for `parseHTTP`, `errIncompleteRequest`, `errInvalidRequest`. Coverage is indirect via `OnTraffic` with a full request in one buffer.

| Edge case | Status |
| :--- | :--- |
| Split TCP / incomplete buffer (two `OnTraffic` calls) | ❌ |
| HTTP keep-alive / pipelining (2+ requests per conn) | ❌ |
| `Content-Length: 0` with body / CL > body / negative / non-numeric CL | ❌ |
| `Transfer-Encoding: chunked` | ❌ (parser requires CL) |
| Query string on path (`/track?x=1`) | ❌ |
| Duplicate headers (two `Content-Length`, two `X-Forwarded-For`) | ❌ |
| Oversize before body read (`errPayloadTooLarge` at parser level) | partial (413 via handler) |
| Case-insensitive header keys (systematic matrix) | ❌ |

### GAP-2 — JSON/proto parse

| Edge case | Status |
| :--- | :--- |
| Truncated/malformed JSON, wrong types, missing fields | ❌ |
| Opt vs standard on **error** inputs | ❌ |
| Huge `payload`, unicode escapes | ❌ |
| `ParseTrackRequestJSONOpt` in prod (`handler.go`) | ❌ not wired (M12-01) |

### GAP-3 — FilterEngine (production order)

Filters are tested **individually**; no single chain test: `emergency → geo → schedule → boost → device → Lua`.

| Component | Unit | In engine / handler |
| :--- | :--- | :--- |
| Lua/unified | ✅ | partial |
| Fraud L1/L2 | ✅ | ✅ `track_core_test` |
| Fraud boost snapshot reload | bench only | ❌ concurrent swap |
| Geo allow/deny | bench + error counter | ❌ functional |
| Schedule | isolated | ❌ in engine |
| Placement blacklist | bench only | ❌ handler 403 |
| Consent | `classifyFilterErr` | ❌ handler 204 |
| License / daily quota | unit | ❌ handler 403/429 |
| Emergency breaker | management tests | ❌ ingestion handler 503 |

### GAP-4 — HTTP status mapping (`classifyFilterErr`)

`filter_errors.go` defines **17 rejection kinds**. `handler_reject_test.go` covers only **6**: campaign not found, bid floor, timeout, fraud 202, redis circuit, rate limit.

**Not covered via handler:** emergency breaker 503, duplicate 409, budget 402, pacing 429, freq 403, geo 403, schedule 403, consent 204, license 403, daily quota 429, placement 403, migration fence → infra 503.

### GAP-5 — Ingress quota / UDP control

| Edge case | Status |
| :--- | :--- |
| `TryIngress` unit (per-worker, epoch swap) | ✅ |
| UDP chaos (stale, loss, reorder) | ✅ |
| **Handler 429** when `udpControl.TryIngress` false (`handler.go`) | ❌ wired, no test |
| Live UDP socket receive loop (`UDPControl.Start`) | ❌ |
| RPD limits on hot path | codec only (`region_keys_test.go`), enforcement ❌ |
| Worker ID pinning on connection churn | ❌ |

### GAP-6 — Sharding / routing

| Edge case | Status |
| :--- | :--- |
| StaticSlot correctness, concurrent reload, migration 6→4 | ✅ |
| E2E multishard (4 Redis) | ✅ |
| `flow_test` / `idempotency_test` on `JumpHashSharder(1)` | ⚠️ does not match prod `StaticSlot` |
| In-flight request on slot table swap | stress in management, not in handler |
| Chaos shard outage | ✅ `tests/chaos/shard_outage_chaos_test.go` |

### GAP-7 — OpenRTB (ingest path)

Strong validate/auction/budget coverage; not covered: truncated OpenRTB JSON inside track payload; multi-imp / multi-item; OpenRTB + protobuf wire in one scenario; boost + RTB live combined.

### GAP-8 — Broker → ingestion

| Edge case | Status |
| :--- | :--- |
| `ParseBrokerPayload` (AdLogRecord, AdStreamEvent) | ✅ |
| Consumer **shadow mode** (no store, offset commit) | ✅ only test |
| Consumer **live mode** → EventStore flush | ❌ |
| `BrokerReconcileWorker` | ❌ no `*_test.go` |
| Parity broker consumer vs HTTP handler filters | ❌ |
| Corrupt/truncated vtproto in consumer | ❌ |
| Consumer reconnect / offset resume | ❌ at ingestion level |
| Poison message / DLQ | server only |

### GAP-9 — Broker server / client (infrastructure)

**Well covered:** segment write/read, torn tail truncation, malformed frames, concurrent produce/fetch, roll during fetch, backpressure, HA failover/fencing/split-brain, durability modes, `TestBrokerE2EAllocs`.

| Area | Gap |
| :--- | :--- |
| `pkg/broker/client` | no `client_test.go`: timeout, reconnect, hung server, dial failure |
| Index partial write | `findActualIndexSize` in `log.go` — no dedicated test |
| Fetch `maxBytes` boundary | partial batch at byte limit across records/segments |
| Redis offset store | `offset_redis.go` — no direct tests |
| Multi-partition e2e | routing: 2 unit tests, no server-level cross-partition |
| Group-commit timer flush | threshold tested, interval loop — none |
| Real ENOSPC / disk full | `chmod` emulation, may skip |
| Throughput regression | `BenchmarkBrokerThroughput` not in nightly gate |
| Load tests | `scripts/load-test/` — broker not mentioned |

### GAP-10 — CI / benchmark blind spots

```makefile
# Makefile test-alloc-gate — only:
ZeroAlloc|zeroAlloc_fraudScoring|FraudScoring_LatencySLA|ApplyRtbAuction_shadow_zeroAlloc|RecordRtbShadow
+ BenchmarkAuction (rtb)
```

**Not in alloc-gate:** `BenchmarkSegmentWrite`, `BenchmarkBrokerThroughput`, `BenchmarkUnifiedFilter_Check` (nightly redis job only), `BenchmarkHTTP1Parse` (after M5-B), broker ingestion path.

**Perf gate** (`perf_gate_bench.sh`): ingestion + rtb proto/parse. **Nightly broker** (`nightly_bench_job.sh broker`): `pkg/broker/protocol` micro-benches only.

**E2E** (`tests/e2e/`): Postgres + Redis testcontainers; **no broker**, **no gnet multi-chunk**, **no UDP ingress**.

### Open tasks

#### Sprint 1 — P0 (wire + handler + broker cutover)

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M6-01 | **`parseHTTP` / `http1_fsm` table-driven** | Unit tests: incomplete buffer, invalid request line, bad CL, CL/body mismatch, query path, duplicate headers. After M5-B1 — same corpus on FSM. | `go test -run TestHTTP1Parse` ≥ 20 cases; 0 regressions in `handler_validation_test` |
| M6-02 | **Handler UDP ingress 429** | `TestAdsPacketHandler_UDPIngress_429`: `UDPControl` + exhausted `TryIngress` → `respRateLimit` + metric | 429 + `ad_udp_ingress_blocked_total` (or equivalent) |
| M6-03 | **Broker live consumer** | `TestBrokerStreamConsumer_LiveFlush`: non-shadow → `MockEventStore` flush + offset commit; bad vtproto → skip/DLQ policy | Store flush ≥ 1; offset committed; shadow test remains |
| M6-04 | **`BrokerReconcileWorker` unit** | Mock broker client + Redis stream depth → gauge `ad_broker_ingest_divergence` | Test without testcontainers; divergence > threshold |
| M6-05 | **Keep-alive / pipelining chaos** | 10 pipelined POST in one `GnetHarnessConn` → 10× 202 (linked to M5-B2) | Chaos subtest or dedicated integration |

#### Sprint 2 — P1 (filter matrix + parse errors + e2e alignment)

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M6-06 | **`classifyFilterErr` → handler matrix** | Table-driven: all 17 `filterRejectKind` via `PostTrackGnetJSON` + prebuilt `resp*` | 17/17 kinds; metrics label smoke |
| M6-07 | **JSON parse error matrix** | Truncated JSON, wrong `campaign_id` type, empty required fields; Opt ≡ standard on errors | ≥ 10 negative cases; handler 400 |
| M6-08 | **FilterEngine production order** | Stub filters with ordering assertions + deadline short-circuit mid-chain (real error types, not just `errFilter`) | Order emergency→geo→schedule→boost→Lua fixed in test |
| M6-09 | **Fraud boost concurrent reload** | `atomic` snapshot swap under parallel `Check`; campaign not in snapshot → zero boost | `-race` clean; no panic |
| M6-10 | **E2E StaticSlot alignment** | `flow_test`, `idempotency_test`: replace `JumpHashSharder(1)` with `StaticSlotSharder(1)` | Routing matches prod; e2e green |
| M6-11 | **`pkg/broker/client` tests** | Timeout expiry, reconnect, dial failure, hung server | `client_test.go`; no testcontainers |
| M6-12 | **Broker corrupt payload ingestion** | Truncated vtproto in consumer; assert no panic, offset policy documented | Test + comment in consumer |

#### Sprint 3 — P2 (infra edges + CI gates + load)

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M6-13 | **`findActualIndexSize` corrupt index** | Unit: partial/corrupt `.index` file recovery | Parity with log-tail torn write tests |
| M6-14 | **Broker throughput in nightly gate** | Add `BenchmarkBrokerThroughput` to `nightly_bench_job.sh broker` or separate job | Baseline in `.ci-baselines/broker/` |
| M6-15 | **Extend `test-alloc-gate`** | After M12-01/M5-B: `BenchmarkTrackRequest_ParseJSONOpt`, `BenchmarkHTTP1Parse`, optional broker produce/fetch alloc test | CI fails on allocs/op > 0 |
| M6-16 | **Broker in load-test** | Produce/fetch scenario under RPS in `scripts/load-test/` | Latency report + no message loss |
| M6-17 | **Fetch `maxBytes` boundary** | Server test: partial batch at byte limit across records/segments | Documented behavior |
| M6-18 | **Redis offset store direct tests** | `offset_redis.go` roundtrip without full HA chaos | Unit with miniredis or testcontainers |
| M6-19 | **E2E broker path** | `tests/e2e/broker_ingest_test.go`: tracker → broker → consumer → PG (opt-in, not `-short`) | End-to-end without shadow |
| M6-20 | **OpenRTB ingest edge cases** | Truncated OpenRTB in payload; multi-imp fixture; boost + RTB live | Coordinate with M12-02, M7 |

### Priorities (impact × bug likelihood)

| P | Tasks | Closes |
| :--- | :--- | :--- |
| **P0** | M6-01, M6-02, M6-03, M6-05 | wire regressions, silent UDP quota gap, broker cutover |
| **P1** | M6-06..M6-12 | wrong HTTP status, malformed body, e2e/prod drift, client blind spot |
| **P2** | M6-13..M6-20 | infra recovery, CI regression, load coverage |

### Definition of Done

**P0 — broker cutover safe (minimum)**

- [ ] M6-01: `parseHTTP` / `http1_fsm` table-driven unit corpus ≥ 20 cases (incomplete buffer, bad CL, duplicates, query path).
- [ ] M6-02: handler returns 429 + metric when `udpControl.TryIngress` false.
- [ ] M6-03: broker live consumer flushes `MockEventStore` and commits offset; bad vtproto policy documented.
- [ ] M6-05: keep-alive pipelining — 10 POST → 10× 202.

**P1 — filter matrix and alignment**

- [ ] M6-06: all 17 `filterRejectKind` values tested via handler (`classifyFilterErr` → prebuilt `resp*`).
- [ ] M6-07: JSON parse error matrix ≥ 10 negative cases; Opt ≡ standard on errors.
- [ ] M6-08: FilterEngine production order fixed in test (emergency → geo → schedule → boost → Lua).
- [ ] M6-09: fraud boost concurrent snapshot reload `-race` clean.
- [ ] M6-10: e2e uses `StaticSlotSharder(1)`, not `JumpHashSharder(1)`.
- [ ] M6-11: `pkg/broker/client/client_test.go` (timeout, reconnect, dial failure, hung server).
- [ ] M6-12: corrupt vtproto in consumer — no panic; offset policy documented.

**P2 — infra, CI gates, load**

- [ ] M6-13: `findActualIndexSize` corrupt index recovery test.
- [ ] M6-14: `BenchmarkBrokerThroughput` in nightly gate with baseline in `.ci-baselines/broker/`.
- [ ] M6-15: alloc-gate extended (`BenchmarkTrackRequest_ParseJSONOpt`, `BenchmarkHTTP1Parse`).
- [ ] M6-16: broker produce/fetch scenario in `scripts/load-test/`.
- [ ] M6-17: fetch `maxBytes` boundary behavior documented and tested.
- [ ] M6-18: `offset_redis.go` direct roundtrip tests.
- [ ] M6-19: `tests/e2e/broker_ingest_test.go` (opt-in, not `-short`).
- [ ] M6-20: OpenRTB ingest edge cases (coordinate M12-02, M7).

**Guide alignment**

- [ ] New fault tests use `fault_*` or `*_chaos_test.go` prefix per [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md) R2.
- [ ] `make test-alloc-gate`, perf-gate, and `make test-broker-chaos-lab` pass.

### Verification

```bash
# Hot path unit + handler
go test ./internal/ingestion/... -run 'HTTP|ParseHTTP|FilterErr|UDPIngress|Broker' -short -count=1

# E2E (testcontainers)
go test ./tests/e2e/... -count=1

# Broker server + client
go test ./pkg/broker/... -short -count=1
go test ./pkg/broker/client/... -count=1   # after M6-11

# Chaos (Docker)
bash scripts/chaos-drills/test_chaos.sh
make test-broker-chaos-lab

# Gates
make test-alloc-gate
bash scripts/perf-gate/perf_gate_run.sh
bash scripts/perf-gate/nightly_bench_job.sh broker   # after M6-14
```

---

## M7 — RTB Exchange Surface and Measurement

**Size:** L · **References:** [RTB.md](./RTB.md) §6 (R1–R31), GAP-RTB-01..08

### Already shipped (out of scope)

In-process `RunAuction` (p99 < 15 µs, 0 heap allocations), shadow and live mode on `/track`, fast OpenRTB 3.0 substring scan, PMP deals, budget authority split, shadow diff metrics, PMP CRUD, bid-floor optimizer (ClickHouse read), license limits.

**Key conclusion:** CPU is not the bottleneck. Latency is driven by the single Redis Lua script (budget p99 < 10 ms) and up to 3 preliminary Redis RTTs. Coordinate with M9 before enabling live mode.

### Sprint 1 — Measurement and PMP (P0)

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| R1 | Write `rtb_deal_outcomes` to ClickHouse | Wire `recordRtbShadowAuction` and `applyRtbAuction` to a lossy ring flush buffer. Batch flush to ClickHouse by time or buffer size. | Rows appear in ClickHouse within 5 s of auction |
| R2 | Apply PMP in `rankCandidates` | Add checks in candidate ranking: `geo_mask`, `cat_mask`, `PacingClosed`, allowed `seats`. | Passing unit and integration tests |
| R3 | Wire `ReserveMicro` in `SyncRtbCatalog` | Read `ReserveMicro` from campaign column in PostgreSQL into RTB catalog struct. | `ReserveMicro` ≠ 0 in catalog snapshot |
| R4 | Default-enable `RTB_TARGETING_INDEX` | Enable fast targeting index after load test with p99 candidate scan < 500. | CI perf-gate passes |
| R5 | Admin live-mode gate | Check `RtbShadowDiffForWindow` and `ad_rtb_budget_reconcile_high` before live flag toggle. | API blocks unsafe live switch |

### Sprint 2 — OpenRTB 2.6 Surface (P0)

**Note:** M12-08 makes OpenRTB the default `/track` ingress; R6–R8 share the same FSM core. eSPX-native ingest remains available via `ingress_schema: espx_native` in `install.yaml` (M13-05).

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| R6 | Hot parser `openrtb26_parse.go` | Zero-alloc incoming JSON parser. Direct byte reads, no intermediate strings, BCE hints. Parse `imp`, `pmp.deals`, `device`, `schain`. | Benchmark shows 0 allocs/op |
| R7 | gnet route `/openrtb/bid` | gnet handler calling `buildRtbTargeting` → `RunAuction` → minimal `BidResponse`. | Integration test for bid request |
| R8 | Bid response generation | JSON bid response from preallocated stack buffer. No `fmt.Sprintf` or `json.Marshal`. | 0 allocations on response path |
| R9 | Monotonic `tmax` deadline | Convert request `tmax` to monotonic deadline. Check timeout every N candidates in scan loop. | Timeout returns `nobid` without error |

### Sprint 3 — Yield, Trust, and Consistency (P1)

| ID | Task | Implementation details |
| :--- | :--- | :--- |
| R10–R16 | Auction optimization and signals | Multi-country fan-out, hybrid ranking weights, ML fraud boost snapshot, lightweight pre-filter, scan limit metric, clearing price in events, cold bid-shading API. |
| R17 | Pre-bid IVT gate | Fraud check before auction when `RTB_PREBID_IVT=1`. |
| R18 | `schain` validation | Supply chain check via stack-fixed `[8]node` array with allowed `asi` and `sid`. |
| R19 | ads.txt / sellers.json audit worker | Background management worker for periodic seller compliance checks. |
| R20 | Bidirectional budget sync | When `authority=rtb`, flush outbox to Redis after `CheckAndSpendAll`. |

### Backlog (P2–P3)

R21–R31: placement and domain targeting, creative-level auction, video/VAST, daypart bitmasks, pre-check frequency caps, CTV, admin auction simulation, A/B cohorts, ARTF hooks, multi-region budget, wire or remove `HybridBalancer.SelectAndShard`.

### Definition of Done

**P0 — exchange minimum (GAP-RTB-01..05, GAP-RTB-07)**

- [ ] R1: `rtb_deal_outcomes` rows in ClickHouse within 5 s of auction.
- [ ] R2: PMP enforced in `rankCandidates` (`geo_mask`, `cat_mask`, `PacingClosed`, `seats`).
- [ ] R3: `ReserveMicro` wired in `SyncRtbCatalog` (non-zero in snapshot).
- [ ] R4: `RTB_TARGETING_INDEX` default-on; p99 candidate scan < 500.
- [ ] R5: admin API blocks unsafe live switch (`RtbShadowDiffForWindow`, `ad_rtb_budget_reconcile_high`).
- [ ] R6: `openrtb26_parse.go` — 0 allocs/op; BCE hints per hot-path guide.
- [ ] R7: `/openrtb/bid` gnet handler integration test passes.
- [ ] R8: bid response — 0 allocs; no `fmt.Sprintf` / `json.Marshal`.
- [ ] R9: monotonic `tmax` deadline; timeout returns `nobid` without error.
- [ ] `RunAuction` remains 0 allocs/op; p99 < 15 µs (existing SLA).
- [ ] M9 Lua consolidation stable before production live-mode enable.
- [ ] `make test-alloc-gate` and perf-gate pass; new RTB benches in gate if added (R11.3).

**P1 — yield and trust (extended)**

- [ ] R10–R20 shipped or deferred to backlog with gap IDs in GAPS.md.

**Does not block closure:** R21–R31 backlog items.

### Verification

```bash
go test -benchmem ./internal/rtb/... -bench RunAuction
go test ./internal/ingestion/... -run Rtb -short
make test-alloc-gate
bash scripts/perf-gate/perf_gate_run.sh
```

---

## M8 — Local Budget Quanta

**Size:** XL · **References:** GAP-HOT-01, GAP-HOT-02, GAP-SHARD-06, M3 · **Depends on:** M9-02 (Lua consolidation); **blocks:** M3-08 (reconcile formula must include broker deltas)

**Context:** The bottleneck is network RTT and Redis shard blocking on `EVALSHA` per click/impression (p99 Lua < 10 ms, but dominates end-to-end latency). CPU and Go filters are not the bottleneck (see M7). The codebase already has distributed quota seeds (`QUOTA_MODE`, `budget:quota:*`, `LocalQuotaCache` with 100 ms block-after-exhaust), but the hot path still hits Redis on every event.

**Constraint (GAP-HOT-01):** Fully separating debit from Lua is allowed only with settlement deltas via broker (`pkg/broker`) + processor reconciliation. Local quanta is not "free" RAM spend without accounting.

### Risks and mitigations (chaotic / high-RPS load)

| ID | Risk | Edge case | Mitigation | Task |
| :--- | :--- | :--- | :--- | :--- |
| R8-01 | **Unaccounted RAM spend** | Tracker restart loses local ledger | Mandatory broker delta publish (M8-04); replay on startup | M8-04, M8-09 |
| R8-02 | **Per-worker double spend** | Same campaign on multiple pinned workers each hold a quantum | **One quantum pool per campaign** (shard-global), not per worker; refill serializes via `budget:refill_lock` | M8-08 |
| R8-03 | **Refill thundering herd** | 2× RPS spike → all workers request refill | Jittered backoff; existing `budget:refill_lock`; cap concurrent refills per shard | M8-07, M8-10 |
| R8-04 | **False stop** | Adaptive quantum too small vs burst → premature block | RPS EMA + minimum quantum floor; hysteresis on shrink | M8-07 |
| R8-05 | **Overshoot** | Exponential RPS growth outpaces refill | Strict mode near tail (M8-03); auto-shrink quantum only on sustained low RPS | M8-03, M8-07 |
| R8-06 | **Strict mode flip-flop** | Campaign oscillates at threshold → latency jitter | Hysteresis band (enter strict at $5, exit at $8 equivalent) | M8-03 |
| R8-07 | **Shadow/live divergence** | Local path diverges from Lua without detection | `LOCAL_QUOTA_MODE=shadow` 24 h gate before live | M8-06 |
| R8-08 | **Reconciler blind spot** | M3 snapshot misses RAM + broker pending | M8-04 broker topic is prerequisite for M3-08 | M8-04, M3-08 |
| R8-09 | **Dedup/fcap bypass** | Local debit skips Redis Lua | Dedup/fcap stay in Redis (M8-05); never local-only for idempotency | M8-05 |
| R8-10 | **Multi-region quota** | Each region holds local quantum against global PG limit | Regional quota chunks (existing `QuotaManager`); no cross-region local pool | Document in MULTI_REGION.md |

### Already shipped (out of scope)

`quota.sql` / `campaign_quotas` (PG control plane), `budget:quota:{campaign_id}` keys in unified Lua, `quotaRefillSample` (~1% Tier C), `LocalQuotaCache` (post-exhaust block 100 ms), `budget-fast.lua` for impression Tier B.

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M8-01 | **Local quanta ledger** per campaign | Background `QuotaRefillWorker` (cold goroutine, not hot path): `EVALSHA` reserves a quantum (micro-units or N events) in Redis → write to pinned worker-local slice: `atomic.AddInt64` on spend (~2 ns). SoA + cache-line pad between hot counters. 0 allocs/op on `TrySpendLocal`. | `BenchmarkLocalQuantaSpend` 0 allocs/op; spend 1M ops < 50 ms |
| M8-02 | **Refill at 80%** | When `local_remaining / chunk_size < 0.20` — async background refill; hot path not blocked. On failed refill — `LocalQuotaCache.Block` + fallback single Lua (as today). | Metric `ad_local_quota_refill_total{status}`; no starvation on burst |
| M8-03 | **Strict Mode** per campaign | When `redis_remaining < STRICT_THRESHOLD_MICRO` (default: ~$5 equivalent or `QUOTA_STRICT_THRESHOLD`), campaign enters strict: every event — single `EVALSHA` for that `campaign_id` only. Hysteresis on exit. | Chaos: overspend ≤ 1 micro-unit per campaign in strict band; `AssertBudgetInvariant` |
| M8-04 | **Broker delta path** | Each local spend publishes vtproto delta to broker topic `budget-deltas`; processor aggregates and reconciles with Redis/PG. Closes GAP-HOT-01. | Reconciliation drift < 0.01% over 5 min in load-test |
| M8-05 | **Dedup/fcap in quanta path** | In quanta mode dedup and fcap stay in Redis (or consolidated Lua M9-02). Do not promise "0 RTT" for all checks — target KPI: **≥ 90%** of high-RPS campaign events without budget Lua RTT. | perf-gate: p99 tracker handler < 500 µs for quanta-eligible impression |
| M8-06 | **Feature flag + canary** | `LOCAL_QUOTA_MODE=shadow|live`; shadow counts local spend parallel to Lua without affecting response. | Shadow diff < 0.1% over 24 h before live |
| M8-07 | **RPS-Adaptive Quanta** | EMA RPS → quantum size; floor/ceiling bounds; shrink only after sustained low RPS. | Load-test: refill latency flat under 2× RPS spike; no false stop |
| M8-08 | **Campaign-global quantum pool** | One logical pool per `campaign_id` across pinned workers; document ownership vs `PinnedWorkerPool`. | Chaos: N workers → ≤ 1× quantum overshoot |
| M8-09 | **Restart recovery** | On tracker boot: consume broker deltas since last checkpoint; rebuild local ledger before accepting quanta traffic. | SIGKILL + restart → `AssertBudgetInvariant` |
| M8-10 | **Refill herd control** | Jitter + per-shard refill concurrency cap; coordinate with `budget:refill_lock` in Lua. | Metric `ad_local_quota_refill_herd_total` = 0 in load-test |

**Expected outcome:** For quanta-mode campaigns — hot path latency drops from 5–10 ms (Redis-bound) to microseconds on the Go layer; Redis RTT amortized over hundreds/thousands of events. Strict mode protects the budget tail.

### Definition of Done

- [ ] M8-01: `TrySpendLocal` 0 allocs/op; `BenchmarkLocalQuantaSpend` — 1M ops < 50 ms; SoA layout with cache-line pad per [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §4.
- [ ] M8-02: async refill at 80% threshold; `ad_local_quota_refill_total{status}`; no starvation on burst.
- [ ] M8-03: strict mode near budget tail; overspend ≤ 1 micro-unit per campaign in strict band.
- [ ] M8-04: broker `budget-deltas` topic; processor reconciliation drift < 0.01% / 5 min (closes GAP-HOT-01).
- [ ] M8-05: dedup/fcap unchanged or documented; ≥ 90% high-RPS events without budget Lua RTT; p99 handler < 500 µs for quanta-eligible impressions.
- [ ] M8-06: `LOCAL_QUOTA_MODE=shadow` diff < 0.1% over 24 h before live.
- [ ] M8-07..M8-10: adaptive quanta, global pool, restart recovery, refill herd control — all risks R8-01..R8-10 mitigated.
- [ ] M9-02 consolidated Lua path in place before live quanta mode.
- [ ] No local spend without broker settlement path — quanta is not unaccounted RAM debit.
- [ ] `AssertBudgetInvariant` holds under quanta + strict; budget chaos emits `chaos_proof`.
- [ ] GAP-HOT-01 closed in GAPS.md.

### Verification

```bash
go test ./internal/ingestion/... -run 'Quota|LocalQuota' -short
go test ./internal/ingestion/... -bench=BenchmarkLocalQuanta -benchmem
make test-alloc-gate
bash scripts/perf-gate/perf_gate_run.sh
./scripts/chaos-drills/test_chaos.sh   # budget invariant under quanta + strict
```

---

## M9 — Edge Lua and Redis RTT Consolidation

**Size:** M · **References:** GAP-SHARD-06, GAP-HOT-01/03

### Already shipped (out of scope)

`unified-filter.lua` (9 checks), `budget-fast.lua` (5 checks), `FilterEngine` Go pre-checks, `crc32 & 1023 → slot_table` in `StaticSlot`, L7 blocklist sync, XDP LPM allow/deny lists + SYN/PPS on port `:8180`, `get_shard()` in `edge-slot-map.lua`.

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M9-01 | Enable **Tier B** for impressions | Set `LUA_FAST_PATH_ENABLED=true` for impressions after chaos stability proof. | p99 Lua latency for impression < Tier C under load |
| M9-02 | Consolidate Go checks into one Lua script | Move `EntitlementsFilter`, shard-0 fraud blocklist, and placement blocklist into one `EVALSHA`. | ≤ 1 Redis RTT on impression fast path |
| M9-03 | Move IP rate limit to edge | Use XDP PPS and nginx `limit_req`, removing IP rate-limit keys from Redis hot path. | IP rate-limit keys absent on hot path |
| M9-04 | Tier degradation inside Lua | Skip non-critical checks inside Lua near deadline instead of `lua_router.go`. Record metric `filter_tier_degraded_total`. | Degradation without extra RTT from Go |
| M9-05 | Wire `get_shard()` in Nginx | Use `edge-slot-map.lua` in nginx upstream selection matching Go `StaticSlot` logic. | Chaos test: edge shard matches Go `GetShard` |
| M9-06 | Document 40/40/20 triple mode | Document that 40/40/20 is a canary migration mode, not target state. | REDIS.md and runbooks updated |
| M9-07 | Remove JumpHash from production paths | Fully exclude `HybridBalancer.SelectAndShard` from production and add CI guard against accidental use. | Closes GAP-HOT-03 |

**Constraint:** Do not separate budget debit from Lua without a broker message path (GAP-HOT-01).

### Definition of Done

- [ ] M9-01: Tier B impressions enabled (`LUA_FAST_PATH_ENABLED=true`); impression Lua p99 < Tier C under load.
- [ ] M9-02: ≤ 1 Redis RTT on impression fast path (single `EVALSHA`).
- [ ] M9-03: IP rate limits on XDP/nginx only; no IP rate-limit keys on Redis hot path.
- [ ] M9-04: in-Lua tier degradation near deadline; `filter_tier_degraded_total` metric; no extra Go RTT.
- [ ] M9-05: nginx `get_shard()` matches Go `StaticSlot` — chaos test with `chaos_proof`.
- [ ] M9-06: 40/40/20 documented as canary-only in REDIS.md.
- [ ] M9-07: `HybridBalancer.SelectAndShard` removed from prod; CI guard prevents regression.
- [ ] Budget debit not split from Lua without broker path (GAP-HOT-01 constraint).
- [ ] Lua fast-path chaos suite passes; `make test-alloc-gate` green.
- [ ] GAP-SHARD-06 and GAP-HOT-03 closed in GAPS.md.

### Verification

```bash
go test ./internal/ingestion/... -run 'Filter|Lua|Unified' -short
go test ./internal/ingestion/... -bench=BenchmarkFilter -benchmem
make test-alloc-gate
```

---

## M10 — XDP L4 Anti-Fraud Hardening

**Size:** M · **References:** [EDGE.md](./EDGE.md) Part V, `GUIDE_COMPLIANCE.md` §1

### Already shipped (out of scope)

`deploy/edge/xdp/bpf/edge_filter.c`: LPM allow before deny, SYN limit 64/s per IP, global SYN limit 50k/s per 8 CPU, PPS token bucket 2000, per-CPU stats. Binaries `cmd/edge-xdp`, `cmd/edge-bpf-sync`, unit tests and benchmarks in `internal/edge/bpf`.

### Sprint 1 — Protocol hygiene (Tier A)

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M10-A1 | TCP anomaly filter | Drop `SYN+FIN`, `SYN+RST`, NULL, FIN-only, XMAS. Increment `XDP_STAT_DROP_ANOMALY`. | Unit test and BPF disassembler check |
| M10-A2 | SYN validity check | Drop packets with `doff < 5` and zero source/destination ports. | BPF tests |
| M10-A3 | Drop non-TCP on port `:8180` | `XDP_DROP` any non-TCP packets to port `:8180` (instead of `XDP_PASS`). | Non-TCP drop test on 8180 |
| M10-A4 | Dynamic BPF config map | Pass `syn_limit`, `pps_rate`, `global_syn_per_cpu` into BPF map. Remove hardcoded 8 CPU count. | Dynamic parameter read without 8-CPU assumption |
| M10-A5 | RST packet limit | Dedicated LRU bucket for RST rate per IP. | RST flood protection test |
| M10-A6 | Makefile build fix | Add `-I/usr/include/$(uname -m)-linux-gnu` and fix `gen.go` path to `deploy/edge/xdp/bpf/edge_filter.c`. | Successful `make` on Debian |

### Sprint 2 — SYN flood resilience (Tier B)

| ID | Task | Implementation details |
| :--- | :--- | :--- |
| M10-B1 | SYN cookies in XDP | Generate SYN cookies at XDP via `bpf_tcp_raw_gen_syncookie_ipv4` helper (Linux kernel ≥ 6.0) under `XDP_SYN_COOKIE` flag. |
| M10-B2 | Dynamic LPM spoof IP block | Hook `tcp/tcp_retransmit_synack` tracepoint to auto-add spoofed IPs to `spoof_block_v4` map. |
| M10-B3 | Autoban violators via Ringbuf | Send violation events via Ringbuf to userspace (`edge-bpf-sync`) for temporary IP block in **local BPF/Redis maps only** — no outbound traffic to visitor IPs ([GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) §2.B). |
| M10-B4 | SYN limit per /24 subnet | Add SYN rate limit at `/24` subnet level for distributed flood protection. |

### Sprint 3 — Passive IVT signals (Tier C)

| ID | Task | Implementation details |
| :--- | :--- | :--- |
| M10-C1 | TCP fingerprint at SYN | On SYN, compute `hash(window, MSS, TCP options)` and pass to ring buffer. |
| M10-C2 | IVT signal correlation | Match TCP fingerprint with User-Agent and JA3 in handler for fraud probability (`ML_GHOST_IVT`). |
| M10-C3 | No hard blocks on fingerprint | Scoring and outbox only per [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) §1.B — do not `XDP_DROP` on fingerprint alone. |
| M10-C4 | BPF stats dashboard | Dashboard anomalies, spoofing, PPS, and drops from per-CPU `stats`. |

### Out of scope (stay L7 / cold)

ASN and geo blocking, campaign-level limits, active port scanning (forbidden by security policy §2.C).

### Definition of Done

**Tier A — protocol hygiene (required for closure)**

- [ ] M10-A1: TCP anomaly filter (`SYN+FIN`, `SYN+RST`, NULL, FIN-only, XMAS); `XDP_STAT_DROP_ANOMALY` counter.
- [ ] M10-A2: drop invalid SYN (`doff < 5`, zero ports).
- [ ] M10-A3: `XDP_DROP` non-TCP on port `:8180`.
- [ ] M10-A4: dynamic BPF config map (`syn_limit`, `pps_rate`, `global_syn_per_cpu`); no 8-CPU hardcode.
- [ ] M10-A5: RST rate limit per IP (LRU bucket).
- [ ] M10-A6: Debian `make` build succeeds with correct include path.
- [ ] Kernel verifier clean; `llvm-objdump -d` review for hot path.
- [ ] `go test ./internal/edge/bpf/...` passes (CAP_BPF / memlock).
- [ ] LPM allow checked before deny per [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) §4.2.
- [ ] `scripts/ci/check_compliance.sh` passes (§6).

**Tier B — SYN flood resilience (extended)**

- [ ] M10-B1: SYN cookies via `bpf_tcp_raw_gen_syncookie_ipv4` (kernel ≥ 6.0, `XDP_SYN_COOKIE` flag).
- [ ] M10-B2: spoof LPM via `tcp/tcp_retransmit_synack` tracepoint → `spoof_block_v4`.
- [ ] M10-B3: Ringbuf autoban to `edge-bpf-sync` — local map/DB only; **no outbound** to visitor IPs (§2.B).
- [ ] M10-B4: SYN limit per /24 subnet.

**Tier C — passive IVT signals (extended)**

- [ ] M10-C1: TCP fingerprint at SYN → ringbuf.
- [ ] M10-C2: correlate fingerprint with UA/JA3 for `ML_GHOST_IVT` scoring.
- [ ] M10-C3: fingerprint used for scoring/outbox only — **no L4 hard-block** on fingerprint alone (§1.B).
- [ ] M10-C4: BPF stats dashboard (anomalies, spoofing, PPS, drops).

**Does not block Tier A closure:** M10-B and M10-C may ship in follow-up releases.

### BPF verification

- [ ] Linux kernel verifier accepts program (no unbounded loops).
- [ ] `go test ./internal/edge/bpf/...` passes.
- [ ] `scripts/ci/check_compliance.sh` passes.
- [ ] Disassembler review: `llvm-objdump -d deploy/edge/xdp/bpf/edge_filter.o`

```bash
clang -O2 -target bpf -D__TARGET_ARCH_x86 \
  -I/usr/include/$(uname -m)-linux-gnu -c deploy/edge/xdp/bpf/edge_filter.c -o deploy/edge/xdp/bpf/edge_filter.o
llvm-objdump -d deploy/edge/xdp/bpf/edge_filter.o
```

---

## M11 — Adaptive Fraud Telemetry Aggregation

**Size:** M · **References:** GAP-HOT-02 (indirect), M10-C2 · **Depends on:** broker produce path

**Context:** `FraudStreamWriter` is a lossy MPSC ring with 4096 slots; on overflow events are **dropped** (`Enqueue` → `false`). On fraud spikes ivt-detector and ML lose a coherent attack picture.

### Already shipped (out of scope)

`FraudStreamWriter` with fixed-size slots, batch XADD, false-sharing padding, drop-on-overflow metric.

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M11-01 | **80% aggregation threshold** | When `alloc-read >= 0.8 * fraudRingUsable` switch mode: instead of per-event enqueue — increment in fixed hash table `[Subnet/24 + FraudReason] → atomic.Uint64` (pre-allocated, sync.Pool for batch flush in cold goroutine only). | Hot path: 0 allocs/op in aggregate mode; `BenchmarkFraudAggregate` |
| M11-02 | **Flush worker 50–100 ms** | Cold goroutine flushes aggregates to Redis stream / broker topic as one batch event `fraud_aggregate{subnet, reason, count, window_ms}`. | ivt-detector sees spike; 0 raw drops at synthetic 50k fraud events/s |
| M11-03 | **L3/L1 priority** | Events with `l3_blocklist` or `FraudLayerL1Reject` are **not aggregated** — always full enqueue (or separate reserved ring slice 256 slots). | L3 never aggregated in chaos test |
| M11-04 | **Metrics and backpressure** | `ad_fraud_stream_mode{aggregating}`, `ad_fraud_stream_aggregated_total`, `ad_fraud_stream_dropped_total` (only on aggregate table overflow). | Alert when aggregate table > 90% |
| M11-05 | **ClickHouse sink** | Processor / ivt-detector consumer for aggregate events → `fraud_aggregate_spikes` table. | 1 h spike query returns subnet with count > 1000 |

**Expected outcome:** Attack telemetry survives spikes; hot path stays within strict RAM bounds (fixed buffers, no unbounded map).

### Definition of Done

- [ ] M11-01: aggregate mode at 80% ring fill; 0 allocs/op; fixed pre-allocated hash table (no unbounded map per [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §7).
- [ ] M11-02: flush worker 50–100 ms; `fraud_aggregate{subnet, reason, count, window_ms}` batch to stream/broker.
- [ ] M11-03: L3/L1 events never aggregated (reserved ring slice or always full enqueue).
- [ ] M11-04: `ad_fraud_stream_mode{aggregating}`, `ad_fraud_stream_aggregated_total`, `ad_fraud_stream_dropped_total`; alert when aggregate table > 90%.
- [ ] M11-05: `fraud_aggregate_spikes` in ClickHouse; 1 h query returns subnet count > 1000.
- [ ] Synthetic 50k fraud events/s — 0 raw ring drops; ivt-detector sees spike within one flush window.
- [ ] `BenchmarkFraudAggregate` 0 allocs/op; `make test-alloc-gate` pass.
- [ ] L3-never-aggregated chaos scenario emits `chaos_proof`.

### Verification

```bash
go test ./internal/ingestion/... -run 'FraudStream' -short
go test ./internal/ingestion/... -bench=BenchmarkFraudAggregate -benchmem
make test-alloc-gate
```

---

## M12 — Parsing Consolidation and OpenRTB Ingress Refactor

**Size:** M · **References:** `GUIDE_HOT_PATH_ZERO_ALLOC.md`, GAP-HOT-PARSE (new), GAP-RTB-01, M7 R6–R8, M5-B, M13-05 · **Parallel:** M5-B, M7 Sprint 2

**Context:** Part of the hot path already uses schema DFA / vtproto, but ad-hoc `bytes.Index`, unwired `ParseTrackRequestJSONOpt`, legacy processor decode, and `uuid.Parse` on warm paths remain. vtproto for protobuf **stays** for the optional eSPX-native mode; DFA for narrow text schemas.

**Ingress direction:** Refactor tracker and edge body parsing so **OpenRTB 3.0 / AdCOM** (and shared FSM with M7 OpenRTB 2.6 bid path) is the **default** wire schema. The proprietary eSPX envelope (`TrackRequest` JSON + `AdEvent` protobuf) remains supported as an **opt-in** profile via installer (`ingress_schema: espx_native`) for backward-compatible and dev deployments — not removed.

### Already shipped (out of scope)

`ParseTrackRequestJSON` (0 allocs), `ParseTrackRequestJSONOpt` (bench-only), `ParseUUID`, vtproto ingress/streams, `edge-parse-dfa.lua`, broker binary `ReadFrame`, manual 202 JSON assembly in `writeGnetTrackAccepted`, `GUIDE_HOT_PATH_ZERO_ALLOC.md`.

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M12-01 | **Wire `ParseTrackRequestJSONOpt`** | Replace calls in `handler.go` (stdlib) and `track_ingest_gnet.go` (gnet) with Opt variant; remove duplication or make Opt primary | `TestParseTrackRequestJSONOptParity` + alloc gate; bench not worse than baseline |
| M12-02 | **OpenRTB extract → FSM** | Replace `bytes.Index` in `openrtb_parse.go` (`ParseOpenRTB3Payload`, `ParseDealID`) with incremental JSON FSM; shared with M12-08 and M7 R6 | 0 allocs/op; unit tests on nested/reordered JSON |
| M12-08 | **OpenRTB ingress schema refactor** | **Default:** parse `/track` and `/openrtb/bid` bodies as OpenRTB 3.0 (AdCOM) via unified hot-path FSM (`openrtb_ingress_parse.go`); map `request.item`, `context.device`, `flr`, extensions (`deal_id`, `category_mask`) into `campaignmodel.Event` + `buildRtbTargeting`. **Optional (installer):** when `ingress_schema=espx_native`, keep current `ParseTrackRequestJSON` / `AdEvent.UnmarshalVT` path unchanged. Edge `edge-parse-dfa.lua`: OpenRTB mode extracts `item[0].id` / campaign key from spec; eSPX mode keeps `campaign_id` scan. Deprecate legacy flat `bid_micro` JSON after one release with metric `espx_ingress_legacy_json_total`. | Integration: OpenRTB body → auction; `espx_native` install → existing nginx corpus still passes; fraud scenario corpus green |
| M12-03 | **Remove legacy processor map-path** | Remove `values["campaign_id"]` + `uuid.Parse` + `time.Parse` branch in `processor.go` after all producers migrate to vtproto field `d` | Chaos + integration: vtproto stream entries only |
| M12-04 | **`ParseUUID` on warm paths** | `registry.go` pubsub, `settings.go` fraud boost scan — `ParseUUID([]byte, &id)` instead of `uuid.Parse` | No regressions in registry sync tests |
| M12-05 | **Binary campaign replica** (P2) | Replace `json.Marshal`/`Unmarshal` in `registry.saveReplica/loadReplica` with versioned binary snapshot (flat records) | Cold start registry ≤ baseline; optional JSON debug export |
| M12-06 | **GO.md ↔ code** | Sync GO.md: HTTP parse = table FSM (M5-B), link `GUIDE_HOT_PATH_ZERO_ALLOC.md`; remove misleading wording until M5-B merges | GO.md matches code or explicitly marks gap |
| M12-07 | **Alloc-gate in CI for parse package** | Extend `scripts/perf-gate` / `test-alloc-gate` with `BenchmarkTrackRequest_ParseJSONOpt`, `BenchmarkHTTP1Parse` (after M5-B) | CI fails on allocs/op > 0 on listed benches |

**vtproto vs DFA rule** (see guide §9): OpenRTB / fixed JSON subset → DFA (default ingress); eSPX protobuf → vtproto **only when** `ingress_schema=espx_native`; admin/arbitrary JSON → `encoding/json`.

### Ingress schema matrix (post M12-08)

| Mode | Installer `ingress_schema` | `/track` body | Edge DFA | Hot parser |
| :--- | :--- | :--- | :--- | :--- |
| **Default** | `openrtb_3` (default) | OpenRTB 3.0 request JSON | OpenRTB item/spec extract | `openrtb_ingress_parse` FSM |
| **Optional** | `espx_native` | `TrackRequest` JSON or `AdEvent` protobuf | `campaign_id` proto/JSON DFA (current) | `ParseTrackRequestJSON*` / `UnmarshalVT` |
| **Bid endpoint** | always OpenRTB | OpenRTB 2.6/3.0 BidRequest (M7 R6–R7) | N/A | shared FSM core with M12-08 |

### Definition of Done

- [ ] M12-01: `ParseTrackRequestJSONOpt` wired in `handler.go` and `track_ingest_gnet.go`; parity test + alloc gate pass.
- [ ] M12-02: OpenRTB `bytes.Index` replaced with incremental JSON FSM; 0 allocs/op; nested/reordered JSON tests.
- [ ] M12-08: OpenRTB 3.0 default ingress on `/track`; `espx_native` opt-in via installer (M13-05); edge DFA dual mode; legacy flat JSON deprecated with metric.
- [ ] M12-03: legacy `processor.go` map-path removed; vtproto field `d` only.
- [ ] M12-04: `ParseUUID([]byte, &id)` on warm paths (`registry.go`, `settings.go`) — no `uuid.Parse(string)` per guide §6.
- [ ] M12-06: GO.md synced with table-FSM (M5-B) or gap explicitly marked.
- [ ] M12-07: alloc-gate extended for `BenchmarkTrackRequest_ParseJSONOpt`, `BenchmarkHTTP1Parse`.
- [ ] No `encoding/json` on request path; OpenRTB / fixed JSON subset → DFA; eSPX protobuf → vtproto only in `espx_native` mode (guide §9).
- [ ] New/changed benches in `perf_gate_bench.sh` when baselines shift (R11.3).
- [ ] *Optional (M12-05):* binary campaign replica — not required for closure.

### Verification

```bash
go test ./internal/ingestion/... -run 'ParseTrack|ParseUUID|OpenRTB' -short
go test ./internal/ingestion/... -bench='BenchmarkTrackRequest_ParseJSON|BenchmarkBuildRtbTargeting' -benchmem
make test-alloc-gate
```

---

## M13 — Runtime Tuning and Installer Safety

**Size:** S · **Parallel** with M7–M11 · **Does not block** critical path

### Open tasks

| ID | Task | Implementation details | Done when |
| :--- | :--- | :--- | :--- |
| M13-01 | **GOGC / GOMEMLIMIT retune** | Hot path — 0 allocs/op; raise tracker `GOGC` from 50 to 200–500 with fixed `GOMEMLIMIT` (700 MiB compose). Keep processor `GOGC=100`. Measure STW pause (p99) and CPU under load-test. | perf-gate: CPU −3% without `go_gc_duration_seconds` p99 > 20% |
| M13-02 | **Slot index hash (benchmark-gated)** | `StaticSlotSharder` today: `crc32Castagnoli` ~5.7 ns/op, HW CRC on amd64. Evaluate xxhash3/murmur3 **only** if: (a) better shard entropy on production slot map, (b) latency not worse, (c) bump `slot_map_version` + sync `edge-slot-map.lua` (breaking change). Default: **keep CRC32** if benchmark fails. | perf-gate report; on change — chaos SO-02 + edge shard match |
| M13-03 | **Installer binary rollback** | `internal/installer`: on `apply` copy current binary to `.espx/backup/<service>-<version>`; after replace — 1 s health probe (`--health-probe`); on panic/crash loop systemd auto-rollback to backup. | Integration test: bad binary → rollback < 2 s |
| M13-04 | **compose/k8s documentation** | Update `docker-compose.yaml` and k8s deployments with recommended `GOGC` after M13-01. | PR with load-test measurements |
| M13-05 | **Installer ingress schema** | `InstallProfile.IngressSchema`: `openrtb_3` (default) \| `espx_native` (optional). Wizard prompt: *"Use legacy eSPX track schema (JSON/protobuf)? (y/N)"*. Render `TRACKER_INGRESS_SCHEMA` + edge Lua `ingress_schema` in compose/systemd templates. Default installs speak OpenRTB 3.0 on `/track`; eSPX `TrackRequest`/`AdEvent` only when explicitly opted in. | `install.yaml` round-trip test; preflight validates enum; compose dev defaults `openrtb_3`; existing e2e corpus runs with `espx_native` profile |

### Definition of Done

- [ ] M13-01: tracker `GOGC` 200–500 with fixed `GOMEMLIMIT`; processor stays `GOGC=100`; STW p99 and CPU measured under load-test.
- [ ] M13-01: perf-gate shows CPU −3% without `go_gc_duration_seconds` p99 regression > 20%.
- [ ] M13-02: CRC32 retained **or** hash change with `slot_map_version` bump, `edge-slot-map.lua` sync, chaos SO-02, perf-gate report.
- [ ] M13-03: installer rollback — bad binary → rollback < 2 s (`internal/installer`, `--health-probe`).
- [ ] M13-04: `docker-compose.yaml` and k8s deployments document recommended `GOGC`.
- [ ] M13-05: installer `ingress_schema` — default `openrtb_3`; optional `espx_native`; env rendered to tracker and edge.
- [ ] `make test-alloc-gate` still passes after GOGC retune (0 allocs/op unchanged on hot path).
- [ ] GOGC change only after confirmed 0 allocs/op per [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §1.

### Verification

```bash
go test ./internal/installer/... -short
# load-test with different GOGC — scripts/load-test/
```

---


## Cross-References

| Document | Purpose |
| :--- | :--- |
| [RTB.md](./RTB.md) | R1–R31 detail, hot path optimization |
| [GAPS.md](./GAPS.md) | Problem catalog and milestone mapping |
| [MULTI_REGION.md](./MULTI_REGION.md) | Regional cells, outbox relay, global idempotency |
| [REDIS.md](./REDIS.md) | Sharding, Lua tiers, key layout |
| [EDGE.md](./EDGE.md) | L7 ingress and XDP integration |
| [GO.md](./GO.md) | Hot path latency requirements, BPF analogs |
| `GUIDE_HOT_PATH_ZERO_ALLOC.md` | BCE, branch prediction, padding, zero-alloc, DFA vs vtproto |
| `GUIDE_STYLE_CODE.md` | Package layout, naming, and mapping rules |
| `GUIDE_CHAOS_RELIABILITY.md` | Chaos engineering requirements and scenarios |

### Optimization Proposal to Milestone Mapping

| Proposal | Milestone | Project adaptation |
| :--- | :--- | :--- |
| Multi-region dedup adapter | **M4** | D3 v2 deterministic SSID + `factor_u`/`factor_d`; claim-before-apply; risks R4-01..R4-11 |
| Budget integrity & contention | **M3** | Extends `ReconWorker`; unified invariant; outbox-only corrections; risks R3-01..R3-14 |
| Local Token Bucket / micro-quanta | **M8** | `QUOTA_MODE` + broker deltas; adaptive quanta; campaign-global pool; risks R8-01..R8-10 |
| Adaptive fraud aggregation | **M11** | Extends `FraudStreamWriter`; L3/L1 not aggregated; fixed buffers |
| Dual-write instead of fence | **M1-08/09** | Phase 2 after fence-first; fence remains fallback; `AssertBudgetInvariant` mandatory |
| XXHash3 / Murmur3 | **M13-02** | Benchmark-gated; breaking slot map change; default — keep CRC32 |
| GOGC 200–500 | **M13-01** | Only with confirmed 0 allocs/op; separate tracker vs processor |
| Installer rollback | **M13-03** | `internal/installer` + tracker `--health-probe` |
| HTTP/2–3 ingress + DFA | **M5** | Edge terminate (A); tracker H1 table-FSM (B); H2 frame FSM optional (C); H3 on gnet deferred (D) |
| Parsing consolidation / zero-alloc guide | **M12** + `GUIDE_HOT_PATH_ZERO_ALLOC.md` | Opt JSON wire; OpenRTB FSM; OpenRTB default ingress (M12-08); eSPX native opt-in (M13-05) |
| Test coverage gaps (hot path + broker) | **M6** | parseHTTP/FSM, handler 429, broker live consumer, classifyFilterErr matrix, e2e StaticSlot, CI gates |

---

## Document Maintenance

| Action | Condition |
| :--- | :--- |
| Remove task | Code shipped to production; tests and chaos checks pass. Brief summary moves to RTB.md / EDGE.md / ARCHITECTURE.md. |
| Add task | New gap identified only within current work streams. |
| Close milestone | Milestone-specific **Definition of Done** checklist fully satisfied (common §5 + per-milestone items); open-task tables empty or deferred to GAPS.md. |
