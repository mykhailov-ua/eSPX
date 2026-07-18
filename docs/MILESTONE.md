# MILESTONE - eSPX Final Roadmap

This document defines sequential milestones. Each milestone is complete only when its **§Mx.0 Standards envelope**, all Definition of Done (DoD) criteria, and applicable **§0.7 CI gates** are met. Timeline estimates are out of scope for this document.

**Execution order (after M1–M2):** M3 (commercial) **done (core)** → M4 (layout) **next** → M5 (compliance) → M6 (Day-2 ops) → M7 (multi-region). See [Execution Order](#execution-order-and-dependencies).

**Related documents:** §0 normative standards; `GUIDE_STYLE_CODE.md`, `GUIDE_CHAOS_RELIABILITY.md`, `GUIDE_IDEAS_MICROSERVICES.md`, `GUIDE_COMPLIANCE.md`, `docs/REMEDIATION.md`, `docs/DATABASE.md`, `docs/CONCEPTS.md` §10, `docs/DEVELOPMENT.md`, `docs/MULTI_REGION.md`, `docs/CRYPTO_GATEWAY.md`, `docs/MANAGEMENT.md`, `docs/LICENSING.md`, `docs/SUBSCRIPTIONS.md`, `docs/proposals/ESPX-LP-2026-V1.md`, `docs/EDGE.md`, `.cursorrules`.

---

## §0 Normative Standards (applies to all milestones)

Every milestone **MUST** satisfy the contracts below unless the milestone explicitly marks an item as optional or deferred. Per-milestone **Standards envelope** tables (§M1.0 … §M14.0) map these rules to concrete binaries, metrics, and CI gates.

### §0.1 Guide documents

| Document | Scope in milestones |
| :--- | :--- |
| `GUIDE_STYLE_CODE.md` | R1 flat packages + R1b facets (`adminapi`); R2 file naming; R3 DTO/replica types; R4 `cmd/` wiring; R8 hot/cold errors; R9 comments; R10 PR verification; R11 codegen/lint |
| `GUIDE_CHAOS_RELIABILITY.md` | R1 steady-state hypothesis; R5 real DBs + 20+ goroutines; R7 `chaos_proof` protocol; R8 observability; R9 experiment design; **R10 when chaos is required vs redundant** |
| `GUIDE_IDEAS_MICROSERVICES.md` | Step 0 workload class; criteria matrix 0–18; veto rules; **no new `cmd/` for batch/cron** unless score ≥ 11 and active clients |
| `GUIDE_COMPLIANCE.md` | §1 defensive / §2 offensive; CMP-* IDs (M5); TEL-RED (M10); PII hash (M14) |
| `.cursorrules` | Hot-path SLA and zero-alloc rules — **override** `GUIDE_STYLE_CODE.md` on conflict |

### §0.2 Universal hot-path SLA (`.cursorrules`)

| Area | Target |
| :--- | :--- |
| Ingestion (`ad_http_request_duration_seconds`) | p95 < 50 ms, p99 < 80 ms, hard ceiling 100 ms |
| Redis unified-filter Lua (per shard) | p99 < 10 ms |
| Geo filter (sampled) | p99 < 10 µs |
| RTB `RunAuction` | p99 < 15 µs; p99 candidates scanned < 500 |
| Fraud accumulator + boost snapshot | incremental < 500 µs per `FilterEngine.Check`; `BenchmarkFilterFraudBoost` ~90 ns, 0 allocs/op |
| `GetShard` (StaticSlot) | ~5.6 ns, 0 allocs/op |
| Budget invariant (Postgres) | `current_spend <= budget_limit` (micro-units); `AssertBudgetInvariant` (+/-1 micro-unit) |
| Budget reconciliation | `(budget_limit - redis_remaining) = pg_current_spend + sync_delta` within ReconWorker window |
| Durability | `XAck` only after Postgres commit or durable CH spool fsync |
| Hot-path allocations | 0 allocs/op on touched paths; `make test-alloc-gate` + perf-gate green |

Load-test abort: control-cohort p99 > 80 ms for 30 s **or** budget invariant violation stops the run.

### §0.3 Code style and layout (summary)

| Rule | Requirement |
| :--- | :--- |
| **R1** | One flat `package` per `internal/<service>/`; allowed subdirs: `db/`, `queries/`, `migrations/`, `pb/` only |
| **R1b** | `internal/adminapi/<facet>/` — domain facets, depth ≤ 1; `register.go` mounts HTTP; not Clean Arch layers |
| **R2** | `service_<domain>.go`, `handler_<area>.go`, `*_worker.go`, `*_chaos_test.go` |
| **R3** | `FooDTO` with `json` tags at I/O boundary; `domain.Foo` without tags on hot path; one-step `toFooDTO` |
| **R8 hot** | `filterRejectKind` / `NoBidReason`; no `fmt.Errorf` on reject; pre-built `filterRejectSpecs` |
| **R8 cold** | `errors.Is` / `%w`; `writeServiceError`; `pkg/cold` for pagination/JSON |
| **R9** | English, ASCII-only comments; `scripts/ci/check_comments.sh` |
| **R10** | Hot: `-benchmem` 0 allocs; cold: `go test ./... -short`; `make lint` |

**Forbidden:** nested domain packages (`internal/ingestion/filter/`), entity+DB+API triple models, `management` importing `cilium/ebpf`, hot-path `json.Marshal` per request.

### §0.4 Microservice placement (Step 0 + score)

| Workload class | Policy | Examples in roadmap |
| :--- | :--- | :--- |
| Hot-path | Never split; library in `internal/ingestion` | tracker, processor stream consumers |
| Control-plane | Standalone `cmd/` if score ≥ 11 | `payment` (16), `billing` (11), `auth` (14) |
| Batch / cron | **Library + worker** in existing binary | CH janitor → `processor`; IVT rules → `ivt-detector`; ledger batch → `SyncWorker` |
| Node utility | Standalone near data | `log-evacuator`, `edge-bpf-sync`, `installer` |

**Veto:** no gRPC from `/track`; no new `cmd/espx-*` without active callers; no split for aesthetics only.

### §0.5 Chaos engineering (R10 decision matrix)

| Change type | Required proof |
| :--- | :--- |
| New write path, outbox, stream, budget mutation | Integration + invariants + new or existing `chaos_proof` |
| New Redis Lua / shard routing | Real Redis + shard outage test |
| Payment / settlement / auth | Concurrent fault injection (existing suites) |
| Read-only admin JSON, layout refactor, comments | `go test -short` only — **no new chaos** |
| Feature flagged off | Unit tests; chaos when flag enabled |

**CI merge gate (financial / ingest paths):** `./scripts/chaos-drills/test_chaos.sh` — `CHAOS_MIN_PROOFS` default **52** (M3 licensing proofs included); each new fault logs `chaos_proof fault=<name> ...`.

**Steady-state defaults (R1):** `/track` p99 < 80 ms; error rate < 0.1% (excl. valid rejects); budget drift within ReconWorker window.

### §0.6 Compliance guardrails (all milestones touching blocks or egress)

| Check | Gate |
| :--- | :--- |
| Defensive only | `GUIDE_COMPLIANCE.md` §1 — no §2 patterns |
| Block path | `allowlist.IsProtected` before persist/BPF (M5+) |
| BPF sync | `management` → outbox → Redis → `edge-bpf-sync` — never direct map writes |
| Telemetry | `ESPX_VENDOR_TELEMETRY=0` default; license heartbeat ≠ vendor perf bundle |
| CI | `scripts/ci/check_compliance.sh` on compliance/telemetry/block changes |

### §0.7 Universal CI merge gates

```bash
go test ./... -short
make lint                                    # golangci-lint, SA9003, SA4017, errcheck
bash scripts/ci/check_comments.sh            # R9.1
./scripts/chaos-drills/test_chaos.sh         # when R10 applies to the diff
bash scripts/perf-gate/perf_gate_run.sh      # when internal/ingestion|rtb|api touched
make test-alloc-gate                         # hot-path changes
bash scripts/ci/check_compliance.sh          # M5, M10, M11 block paths, M14 PII
```

### §0.8 Milestone envelope template

Each milestone below includes **§Mx.0 Standards envelope** with: guides, binary placement (micro score), packages/patterns, SLA metrics, code zone, chaos R10 verdict, CI gates. Closure requires envelope **and** milestone-specific DoD tables.

---

## Milestone 1 - Architectural Remediation (Single-Site)

**Goal:** Close P0 risks around durability, budget correctness, hot-path isolation, and security perimeter in a single region. The system is ready for production single-site operation.

**Sources:** `docs/REMEDIATION.md`, `docs/DATABASE.md` (part III), `docs/REDIS.md`, `docs/AUTH.md`, `GUIDE_CHAOS_RELIABILITY.md`, `GUIDE_STYLE_CODE.md` (R8 hot, R10).

### 1.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R1–R5, R7–R10 (write paths); STYLE R8.3/R8.7 hot-path; MICRO Step 0 hot-path — no new `cmd/` |
| **Binaries** | `tracker`, `processor`, `management` — extend in place; no service split |
| **Packages** | `internal/ingestion` flat (R1); `processor_pg_gate.go`, `ch_spool.go`, `store_errors.go` |
| **Patterns** | Single-writer `current_spend` (H1); outbox settlement (S1); mmap CH spool before `XAck` (D2); `ProcessorPgGate` / `ProcessorChGate` (SEM-P*); idempotency Lua + PG |
| **SLA** | §0.2 universal hot-path; filter Lua p99 < 10 ms; CH batch ≥ 1000 rows or ≤ 5 s flush |
| **Metrics** | `ad_processor_stream_lag_seconds`, `ad_processor_write_acquire_wait_seconds{backend}`, `ad_processor_stream_backpressure_active`, `ad_redis_lua_noscript_total`, `ad_http_request_duration_seconds` |
| **Code** | Hot zone R8.3: `classifyFilterErr`, `filterRejectSpecs`, 0 allocs on parse/filter/respond; escape analysis on touched files |
| **Chaos R10** | **Required** — new write gates, spool, PEL retention; proofs: `clickhouse_outage`, `ch_spool_rotation`, `write_path_db_fail_pre_ack`, `processor_pg_gate_overflow`, scenarios A/C/F |
| **CI gates** | §0.7 full stack; `write_path_chaos_integration_test.go`; `tests/chaos/shard_outage_chaos_test.go`; perf-gate on `internal/ingestion` |

### 1.1 Architecture and Responsibility Boundaries

| ID | Change | DoD |
| :--- | :--- | :--- |
| D0/D1 | `XAck` for CH consumer group only after successful ClickHouse write | Unit/integration test: event remains in PEL on CH failure simulation; after CH recovery - exactly one row in CH, no duplicates (`insert_deduplicate`); `chaos_proof fault=clickhouse_outage pg_ledger_ok=true ch_catchup=true` |
| D2 | mmap WAL (spool) on processor before `XAck` during CH outage | WAL segment on disk; `fsync` before ack; **segment rotation** (`events.wal.NNNN`, lazy mmap, `CH_SPOOL_SEGMENT_MB`, `CH_SPOOL_MAX_SEGMENTS`); recovery after SIGKILL restores unacknowledged events; test T-ID-05 (partial flush) from `IDEMPOTENCY_CORE.md`; chaos proofs `ch_spool_rotation`, `ch_spool_max_segments`, `ch_spool_fd_release` |
| D4 | Connection pool limits for PG and CH batch writers | Configurable `MaxConns` per store; explicit `ProcessorPgGate` / `ProcessorChGate` (`.env`: `PROCESSOR_PG_GATE_SLOTS`, `PROCESSOR_CH_GATE_SLOTS`, `0` = auto); on saturation - backpressure via `pauseStreamReads`, stream lag bounded; metrics `ad_processor_stream_lag_seconds`, `ad_processor_write_acquire_wait_seconds`, `ad_processor_stream_backpressure_active` |
| D6 | Graceful shutdown processor/tracker | `TestE2E_GracefulShutdown_NoDataLoss`: every HTTP 202 has a row in `events`; consumer drain without loss |
| B2 | PG fallback on tracker forbidden in production | `TRACKER_PG_FALLBACK=0` (or equivalent) in prod compose; registry/cache miss -> `filterRejectCampaignNotFound`, not SQL; integration test confirms no pgx calls on `/track` on cache miss |
| H1 | Single writer for `current_spend` in Postgres | Only `SyncWorker` (processor) calls `UpdateSpend`; management does not write `campaigns.current_spend` directly; audit grep + integration test on concurrent SyncWorker/management |
| L1 | Separate budget debit and `XADD` in Lua (target model REMEDIATION 1.1) | Lua: atomic debit + dedup only; stream write - separate hop or cold-path batch; unified-filter p99 < 10 ms preserved; benchstat without filter latency regression |
| Q1 | Quota refill outside hot path | `QuotaManager` in management; hot path reads only `budget:quota:{region}:{campaign}`; no synchronous PG in filter pipeline |
| G1 | Postgres HA single-site | Sync standby in another AZ; WAL archive -> object storage (PITR); documented failover runbook; RPO ~0 for sync replica |
| S1 | Async credit path payment -> management | Payment outbox -> settlement gRPC; on management unavailability - outbox `PENDING`, no orphan credits; `chaos_proof` settlement server down scenario |

### 1.1b Write-Path Concurrency and Failure Containment (M1 extension)

Items below were not in the original M1 charter but were implemented during remediation; they are **in scope for M1 closure** and marked complete.

| ID | Change | DoD |
| :--- | :--- | :--- |
| SEM-P1 | Processor-global Postgres write gate | `ProcessorPgGate` wraps `PostgresStore.StoreBatch`; capacity from `PROCESSOR_PG_GATE_SLOTS` or `DB_PROCESSOR_MAX_CONNS - 1`; metric `ad_processor_write_acquire_wait_seconds{backend="postgres"}`; chaos `processor_pg_gate_overflow` (testcontainers) |
| SEM-P2 | Cross-shard SyncWorker shares PG gate | All `SyncWorker` instances use the same `procPgGate`; per-shard `maxConcurrency` sem disabled when gate is set; chaos `sync_worker_pg_gate_overflow` |
| SEM-P5 | ClickHouse write gate + stream backpressure | `ProcessorChGate` on `ClickHouseStore`; `StreamConsumer.pauseStreamReads` stops `XREADGROUP` while store circuit is open; metric `ad_processor_stream_backpressure_active`; spool-full treated as retriable |
| WP-PG | PG outage: PEL retention, no premature DLQ | `isRetriableStoreError` on connection refused / pool closed / deadlines; `recoverPending` and drain paths retain PEL; chaos `write_path_db_fail_pre_ack` (`pel_retained`, `dlq_avoided`, `circuit_open`) |
| WP-ENV | Processor tuning via `.env` | `PROCESSOR_PG_STREAM_MAX_WORKERS`, `PROCESSOR_CH_STREAM_MAX_WORKERS` (0 = inherit `MAX_WORKERS` / `CH_MAX_WORKERS`); `SYNC_WORKER_MAX_CONCURRENCY` for management; documented in `.env.example` |
| INST-P1 | Interactive installer (backlog) | Documented in `docs/REMEDIATION.md` §7; not implemented — `make setup` wizard deferred post-M1 |

### 1.2 Data Structures and Invariants

| Component | Requirement |
| :--- | :--- |
| `sync_idempotency` | PK on batch UUID; repeated `SyncAll` does not add rows |
| `events` | PK `(click_id, created_date)`; duplicate stream delivery -> 1 row |
| Redis `idempotency:click:{click_id}` | `SET NX`; replay -> HTTP 202 without repeat debit |
| Redis `budget:sync` / `inflight` | Lua claim -> PG `UpdateSpend` -> Lua commit; atomicity per shard |
| `balance_ledger` | Immutable rows; `amount_micro` BIGINT; single money truth |
| CH batch | >= 1000 rows or flush window <= 5 s; `Too many parts` does not occur under load |

Budget invariant after chaos recovery:
```
sum(redis_debits) + sum(sync_pending) = campaigns.current_spend (+/- 1 micro-unit)
```

### 1.3 Security and Perimeter (Auth / Edge)

| ID | DoD |
| :--- | :--- |
| AUTH-P0-IP | Management passes real client IP in gRPC metadata to auth; lockout/ratelimit by IP, not `127.0.0.1` |
| AUTH-P0-SEM | `cryptoSem` on all Argon2 entry points (login, password change, API key verify) |
| AUTH-P1-NP | NetworkPolicy isolates `cmd/auth` from public ingress |
| EDGE-P1-RL | Nginx rate limit on `/admin/login` and auth-facing paths |

### 1.4 Redis / Lua

| ID | DoD |
| :--- | :--- |
| R-LUA-migration | `budget:migration_fence` set before slot map update; chaos test slot migration partition |
| R-LUA-NOSCRIPT | `ad_redis_lua_noscript_total` spike -> baseline after recovery; scenario I backlog documented |
| Tier B/C | `budget-fast.lua` for impressions; `unified-filter.lua` for clicks; table-driven tier routing tests |

### 1.5 Tests, Benchmarks, Chaos

**Required runs (CI merge gate):**
```bash
go test ./... -short
go test -benchmem ./internal/ingestion/...   # 0 allocs/op on gated benches
bash scripts/perf-gate/perf_gate_run.sh
./scripts/chaos-drills/test_chaos.sh       # MIN_PROOFS >= 52
make lint
```

**E2E / integration (already in tree, must pass):**
- `tests/e2e/flow_test.go`, `flow_protobuf`, `idempotency`, `multishard`, `shutdown`, `rtb_live_budget`
- `tests/integration/budget_test.go`, `campaign_db_test.go`
- `tests/chaos/shard_outage_chaos_test.go` (scenario A)

**Chaos scenarios (compose manual + CI where applicable):**
- A: shard 0 outage - shards 1-3 p99 < 80 ms; outbox PENDING -> PROCESSED
- C: processor PG partition - stream grows, idempotency holds post-recovery
- F: ClickHouse outage - tracker p99 stable; PG ledger unaffected
- **Write path (testcontainers, `internal/ingestion/write_path_chaos_integration_test.go`):**
  - `processor_pg_gate_overflow` — 24 workers, peak in-flight capped at gate capacity
  - `ch_spool_rotation` — sealed segments + single active FD under CH outage
  - `ch_spool_max_segments` — fault at segment budget (`errCHSpoolMaxSegments`)
  - `write_path_db_fail_pre_ack` — PG stop: PEL retained, circuit open, no DLQ

**Perf baseline:** `docs/hot_path_baseline.md` without regression on affected benches; escape analysis without new escapes on parse/filter/respond.

Write-path cold benchmarks (informational, not merge gate): `BenchmarkCHSpoolAppendDurably` ~1 ms/op (`fsync`-bound), `BenchmarkCHSpoolMarshalPayload` ~150 ns/op, 1 alloc/op.

### 1.6 Milestone 1 - Completion Checklist

- [x] D0/D1, D2, D4, D6 implemented and covered by tests
- [x] B2, H1, G1, S1 closed
- [x] L1, Q1 - target Lua decomposition or documented exception with perf evidence (`docs/REMEDIATION.md` section 6; `QuotaManager` in management)
- [x] Auth P0, Edge P1
- [x] Budget invariant holds after all chaos scenarios A, C, F (existing suites; CH outage proof added)
- [x] `chaos_proof` count >= `CHAOS_MIN_PROOFS` (existing CI gate)
- [x] `REMEDIATION.md` items marked complete
- [x] SEM-P1, SEM-P2, SEM-P5 — processor write gates and stream backpressure (`docs/REMEDIATION.md` §7)
- [x] CH spool segment rotation, lazy FD mapping, `CH_SPOOL_*` env config
- [x] PG retriable-error PEL retention (no DLQ on transient PG partition); `write_path_db_fail_pre_ack`
- [x] Processor concurrency tunables in `.env.example` (`PROCESSOR_*_GATE_SLOTS`, `PROCESSOR_*_STREAM_MAX_WORKERS`, `SYNC_WORKER_MAX_CONCURRENCY`)
- [ ] INST-P1 interactive installer (backlog only; documented, not required for M1)

### 1.7 How the Write-Path Effect Was Reached

This section records **why** the extension work matters and **how** each mechanism contributes, for operators and future reviewers.

**Problem.** Implicit `pgxpool.MaxConns` did not cap goroutine fan-out: up to `MAX_WORKERS × shards` (64) PG stream workers plus up to `32 × shards` SyncWorker goroutines could contend for 16 pool connections. During PG or CH outage, workers kept reading Redis while stores failed, growing in-memory batches and eventually routing transient failures to DLQ via `recoverPending`.

**SEM-P1/P2 (Postgres gate).** A process-wide `chan struct{}` sized to `PROCESSOR_PG_GATE_SLOTS` (default: `DB_PROCESSOR_MAX_CONNS - 1`) serializes **logical** concurrent writers across `PostgresStore` and all `SyncWorker` shards. Effect: under a 24-worker burst, measured peak in-flight writers equals gate capacity (3 in chaos test with pool 4), not worker count. Pool wait time surfaces in `ad_processor_write_acquire_wait_seconds` instead of unbounded goroutine pile-up.

**SEM-P5 + backpressure (ClickHouse).** The same pattern on `ClickHouseStore`, plus `pauseStreamReads()` when the per-consumer store circuit is **Open**. Effect: CH long outage stops pulling new stream entries while spool or circuit blocks; metric `ad_processor_stream_backpressure_active{group}=1`. Stream lag grows in Redis (expected) rather than in process heap.

**D2 rotation.** Fixed 512 MiB single file replaced with sealed `events.wal.NNNN` segments, lazy mmap on `Scan`, one active FD. Effect: long CH outage continues durable acks until `CH_SPOOL_MAX_SEGMENTS`; chaos shows 4 segments / 3 rotated files with `open_fds=1`. Failure mode at budget exhaustion is explicit (`errCHSpoolMaxSegments`) and retriable.

**WP-PG (PEL vs DLQ).** `isRetriableStoreError` distinguishes partition/refused/timeout from poison data. `recoverPending` no longer DLQs on first PG refusal. Effect: chaos with stopped Postgres container yields `pel_retained=true`, `dlq_avoided=true`, `circuit_open=true` — messages replay when PG returns without manual DLQ recovery.

**Configuration.** All limits are `.env`-driven with **zero = inherit current production defaults**, so existing compose files behave unchanged until operators tighten gates or worker counts deliberately.

See `docs/CONCEPTS.md` §10 for the reusable principle set.

---

## Milestone 2 - Invoicing and Server-Side Admin API (cold path)

**Goal:** Complete the invoice generation -> delivery chain and prepare infrastructure for a server-side oriented admin panel: JSON HTTP endpoints, fan-out for aggregated reads, godoc contracts. HTML templates, HTMX, and OpenAPI are out of scope.

**Sources:** `docs/ADMINISTRATIVE.md`, `GUIDE_IDEAS_MICROSERVICES.md` (billing, management backlog), `GUIDE_STYLE_CODE.md` (R1b, R3 DTO, R8 cold, R9 godoc), `GUIDE_CHAOS_RELIABILITY.md` (R10).

### 2.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R1b (`adminapi` facets), R3 DTO one-step mapping, R8.2/R8.6 cold HTTP; MICRO billing **11/18** → standalone `cmd/billing`; adminapi **≤5/18** → library in `management` |
| **Binaries** | `cmd/billing` (gRPC invoices); `cmd/management` (HTTP gateway); `cmd/notifier` (delivery) — no `cmd/admin-api` |
| **Packages** | `internal/adminapi/{billing,ops,export}`; `internal/billing` flat; `internal/management` workers/outbox stay flat |
| **Patterns** | Outbox-only Redis mutations; money truth = `balance_ledger` only; `FanOutCollector` parallel reads; `PlaceholderProvider` (no Stripe in billing); `pkg/cold.PaginatedList` |
| **SLA** | `GenerateInvoice` gRPC p99 < 2 s (≤100k ledger rows); fan-out GET p99 < 1.5 s (4 shards); statement GET p99 < 800 ms |
| **Metrics** | `ad_management_outbox_oldest_pending_seconds`; fan-out partial via `FanOutResult.partial`; ledger invariant via `CheckLedgerBalanceInvariant` |
| **Code** | Cold zone: `mapServiceError` + `writeServiceError`; godoc on exported DTOs; **forbidden** `internal/ingestion` import in adminapi |
| **Chaos R10** | **Existing only** — settlement down, payment outbox race (R10 #12); **no new chaos** for read-only JSON routes (R10 #3) |
| **CI gates** | `go test ./internal/adminapi/... ./internal/billing/... -short`; `make lint`; `check_comments`; hot-path perf-gate unchanged |

### 2.1 Architecture

| Principle | DoD |
| :--- | :--- |
| Hot-path isolation | Admin API handlers do not import `internal/ingestion` filter/track code; cold-path latency does not affect tracker p99 |
| Outbox-only mutations | Campaign pause, blacklist, pacing, DLQ retry - PG transaction + `outbox_events`; no direct Redis from HTTP handler |
| Money truth | Invoice built only from `balance_ledger` aggregates; `billing` schema migrations under `internal/billing/migrations/` |
| Service boundaries | `cmd/billing` standalone (score 11/18); management calls billing gRPC + `BILLING_INTERNAL_TOKEN` |
| Server-side panel | Admin panel client is external; management serves JSON (`application/json`) and streaming export; SSR/UI not implemented |
| API contract | Exported handlers, DTOs, and service entry points documented in godoc (R9); OpenAPI / OpenRPC - out of scope until a separate decision |
| Package layout (M2.8) | Billing/ops JSON surface extracted from flat `internal/management` into `internal/adminapi/`; `cmd/management` remains HTTP gateway |

Existing `/api/v1/` prefix (`handler_api.go`, `handler_selfserve.go`) is extended; M2 routes live in `handler_admin_api.go`. **M2.8** moves billing analytics routes and ops fan-out into `internal/adminapi/` (see §2.8).

**Does a separate directory make sense?** Yes — as a **library package**, not a new binary yet.

| Approach | Verdict |
| :--- | :--- |
| New `cmd/admin-api` binary now | No — shared auth, RBAC, rate limits, and one admin port; splitting deployables adds ops cost without isolation benefit (billing gRPC already owns invoice math). |
| `internal/adminapi/` sub-tree | **Yes** — `management` has ~190 Go files mixing HTMX handlers, workers, ops, billing JSON, RTB, supply, etc. Extract cold-path JSON admin surface so `internal/management` keeps outbox workers, settlement, and HTMX; `internal/adminapi` owns `/api/v1/billing/*`, `/api/v1/ops/*`, fan-out, statement JOINs, and finance DTOs. |
| `internal/billing` scope | Unchanged — invoice generation, tax, PDF bytes, gRPC only. No HTTP, no Stripe, no admin DTOs. |

Target layout after M2.8:

```
internal/adminapi/
  billing/          # handlers, statement service, wallet DTO, preview/void
  ops/              # fan-out collector, incidents, dlq, outbox list
  export/           # async billing/audit export jobs (EXP-02)
  register.go       # RegisterRoutes(mux, deps) called from management.Handler
internal/management/  # HTMX, workers, settlement, outbox processors (unchanged role)
internal/billing/     # gRPC + InvoiceWorker (unchanged role)
cmd/management/       # single HTTP listener :8188 (unchanged)
```

Migration rule: move files; do not duplicate auth — `adminapi` receives `*management.AuthMiddleware`, `BillingClient`, `PaymentClient` via a small `Deps` struct.

### 2.2 Fan-Out Infrastructure

Aggregated reads (ops, logs, DLQ, shard health) use a shared parallel source polling layer.

| Component | Purpose | DoD |
| :--- | :--- | :--- |
| `FanOutCollector` | Parallel poll of N sources (Redis shards, processor instances, recon nodes) | `internal/management/fanout_collector.go`; context deadline on entire request; per-source timeout |
| Merge + cursor | Merge partial results with cursor pagination | Cursor encodes `(source_id, offset)`; client continues with `?cursor=` |
| Partial failure | Source unavailable on multi-source read | HTTP 200 + JSON `"partial": true` and `"errors": [{source, code}]`; or HTTP 503 if all sources failed |
| Concurrency cap | Fan-out goroutine limit | `ADMIN_FANOUT_MAX_CONCURRENCY` (default 8); does not block HTTP worker pool |
| Metrics | Fan-out observability | `ad_admin_fanout_sources_total`, `ad_admin_fanout_partial_total`, histogram latency per route |

**When fan-out is required:**
- DLQ list across processor consumer groups / shards
- Shard health (`PING`, breaker state, `XLEN` stream) - 4 Redis masters
- Audit / balance export on customer shard partition (if source > 1)
- Incident snapshot (metrics + outbox oldest + stream lag) - parallel fetch

**When fan-out is not needed:** single Postgres query (`outbox_events`, `billing.invoices`, ledger) - direct sqlc path.

### 2.3 Endpoints (invoicing + ops)

All routes: JSON request/response, RBAC (`perm`), rate limit (`limit`), godoc on handler and DTO.

| Method | Route | Function | DoD |
| :--- | :--- | :--- | :--- |
| - | `billing` gRPC | `GenerateInvoice` | Sum of line items = `SUM(balance_ledger)` for period; idempotent re-run -> same `invoice_id` |
| - | `InvoiceWorker` | Cron on 1st of month | Worker in `cmd/billing`; empty ledger -> skip without error storm |
| - | PDF renderer | Invoice bytes | PDF non-empty; `customer_id`, period, `amount_micro` |
| - | notifier | Invoice delivery | `SendNotification` with download URL after generate |
| GET | `/api/v1/billing/invoices` | List invoices | Proxy billing gRPC; pagination `limit` default 50, max 1000; `PaginatedList` JSON |
| GET | `/api/v1/billing/invoices/{id}` | Invoice detail | Line items + PDF URL or `application/pdf` sub-resource `.../pdf` |
| GET | `/api/v1/ops/incidents` | Incident snapshot | Fan-out: shard health, breaker, `stream_lag`, outbox oldest pending; single JSON object |
| GET | `/api/v1/ops/outbox` | Outbox list | Filter `status`, `event_type`; cursor pagination; PENDING visible when shard 0 down |
| GET | `/api/v1/ops/dlq` | DLQ entries | Fan-out across processor groups; cursor; read-only GET |
| POST | `/api/v1/ops/dlq/{id}/retry` | DLQ retry command | Outbox mutation; idempotency key header |
| GET | `/api/v1/ops/shards` | Per-shard health | Fan-out 4 Redis; latency per shard in response |
| GET | `/api/v1/audit/export` | Audit CSV stream | `Content-Type: text/csv`; cursor; chunk <= 10 MB; `ensureCustomerAccess` |
| GET | `/api/v1/customers/{id}/payments` | Payment history | JSON rows from `payment.*` + ledger `reference_id` link |
| - | Ledger invariant | `CheckLedgerBalanceInvariant` fail | Notifier alert; `billing_ledger_invariant_failures_total` |
| - | Low balance alert | Threshold per customer | Notifier; idempotent per day per customer |

**Out of scope for M2:** HTML pages, HTMX partials, OpenAPI spec files, admin frontend assets.

### 2.4 Data Structures

| Table / type | Fields / contract |
| :--- | :--- |
| `billing.invoices` | `id`, `customer_id`, `period_start`, `period_end`, `amount_micro`, `status`, `pdf_object_key` |
| `billing.invoice_lines` | FK invoice; aggregated category or ledger refs |
| `balance_ledger` | Unchanged; invoice read-only |
| `notifier.notifications` | `template_id=invoice_ready`; payload JSON with URL |
| `FanOutResult[T]` | `{items, partial, errors[], next_cursor}` - generic merge wrapper |
| `IncidentSnapshotDTO` | `json` tags; shards[], outbox, stream_lag, breaker_states |
| Admin idempotency | `SHA256(customer_id + canonical_json(body))` for mutating POST |
| API pagination | `pkg/cold.PaginatedList`; cursor opaque base64 |

### 2.5 SLA and Load (cold path)

| Metric | Target |
| :--- | :--- |
| `GenerateInvoice` gRPC p99 | < 2 s for customer with <= 100k ledger rows |
| Single-source GET p99 | < 500 ms |
| Fan-out GET p99 (`/ops/incidents`, `/ops/shards`) | < 1.5 s with 4 shards, all healthy |
| Fan-out partial | <= 1 source failed -> `partial: true`, HTTP 200 |
| InvoiceWorker month-end | All ACTIVE customers without manual intervention |
| Audit export stream | Sustained 10 MB/s without OOM on management process |

Hot path SLA from §0.2 — no regression after milestone 2 merge.

### 2.6 Tests

```bash
go test ./internal/billing/... ./internal/management/... -short
go test ./internal/management/... -run 'API|FanOut' -short
go test ./internal/notifier/... -run Invoice -short
```

| Test | Criterion |
| :--- | :--- |
| Invoice idempotency | Double `GenerateInvoice` for same period -> 1 invoice row |
| Ledger invariant | Drift injection -> alert; invariant restored |
| Admin RBAC | Cross-customer `GET /api/v1/...` -> 403 |
| Fan-out partial | 1 shard stopped -> `partial: true`; 3 shards data present |
| Fan-out cursor | Multi-page DLQ/export without duplicates or gaps |
| Outbox API | Stopped redis-0 -> PENDING rows in JSON response |
| godoc | `go doc` on exported DTO/handlers; `check_comments` pass |

Chaos (R10): payment/management outbox race, settlement down - existing suites pass; new chaos tests for read-only JSON API not required.

### 2.7 Milestone 2 - Completion Checklist

- [x] InvoiceWorker + PDF + notifier delivery end-to-end in compose
- [x] `FanOutCollector` + metrics; fan-out on ops/DLQ/shard routes
- [x] JSON endpoints `/api/v1/billing/*`, `/api/v1/ops/*`, audit export (P0-P2)
- [x] Low balance alert + payment history JSON (P1)
- [x] `CheckLedgerBalanceInvariant` alert wired
- [x] godoc on exported admin API types and handlers; OpenAPI not added
- [x] Hot-path perf gate green; no ingestion imports in admin handlers
- [x] `docs/ADMINISTRATIVE.md` backlog aligned with server-side API scope (ops, JOIN reports, XFM/SRT)

### 2.8 Phase 2b — Invoicing UX, Analytics, and Package Split

**Goal:** Rich finance UX for an external admin panel (statements, preview, wallet, delivery status) without hot-path coupling. Refactor bloated `internal/management` by extracting JSON admin surface to `internal/adminapi/`. **No live Stripe integration in this phase** — external payment provider is a config placeholder only.

**Sources:** `docs/ADMINISTRATIVE.md` (JOIN-06, ROL-03, FIN-07, EXP-02, TEN-07/08), §2.8 decisions below.

#### 2.8.1 External payment placeholder (no Stripe)

Billing and invoice flows must not call Stripe APIs in M2.8. Top-ups and checkout remain on `cmd/payment` (existing stub when `STRIPE_SECRET_KEY` is empty).

| Item | DoD |
| :--- | :--- |
| Config | `BILLING_PAYMENT_PROVIDER=placeholder` (default); `BILLING_PAYMENT_PROVIDER_KEY=placeholder_dev` — logged at billing startup, never sent to a network |
| Wallet / statement DTO | `payment_provider: "placeholder"`, `payment_provider_configured: false` in `GET .../wallet` |
| Invoice PDF / notifier | No checkout links; copy references “contact billing” or management top-up URL |
| Future hook | `internal/billing/payment_provider.go` interface (`Name()`, `Configured()`) with `PlaceholderProvider` only; real Stripe adapter deferred to payment milestone |

**Out of scope M2.8:** `STRIPE_SECRET_KEY` wiring in billing, hosted invoice pay links, webhook changes.

#### 2.8.2 Analytics and composite reads (panel UX)

Single round-trip read models; handlers use one sqlc query or billing gRPC + one PG query — no per-row loops (ADMINISTRATIVE §6).

| Priority | Route / worker | Function | DoD |
| :--- | :--- | :--- | :--- |
| P0 | `GET /api/v1/customers/{id}/billing/statement` | JOIN-06 billing statement | Opening/closing balance, spend by `ledger_type`, tax, invoices in period, payment summary; `from`/`to` RFC3339 or month |
| P0 | `GET /api/v1/billing/invoices/{id}/ledger-lines` | Ledger drill-down | Paginated `balance_ledger` rows for invoice `billing_month`; cursor; `ensureCustomerAccess` |
| P0 | `POST /api/v1/billing/invoices/preview` | Preview before finalize | Same math as `GenerateInvoice` without persist; returns lines, tax, `would_skip: true` when zero spend |
| P1 | `GET /api/v1/customers/{id}/wallet` | Wallet card | `balance_micro`, `currency`, `allowed_overdraft_micro`, `low_balance_threshold_micro`, burn hint, `last_invoice_at`, payment placeholder fields |
| P1 | `GET /api/v1/billing/invoices/{id}/deliveries` | Delivery status | Rows from `notifier.notifications` for `template_id=invoice_monthly` / dedup key |
| P1 | `POST /api/v1/billing/invoices/{id}/deliveries/retry` | Resend invoice | Idempotency-Key; notifier enqueue only |
| P1 | `GET /api/v1/billing/invariant` | FIN-07 | Per-customer or global scan result: `{ok, customer_id, balance_micro, ledger_sum_micro, diff_micro}` |
| P1 | `GET /api/v1/billing/summary` | Ops dashboard | Admin-only: MTD invoiced, drift failures, undelivered count, customers with preview spend > 0 |
| P2 | `GET /api/v1/customers/{id}/billing/forecast` | Burn projection | CH hourly spend + ledger run-rate → projected month-end (read-only) |
| P2 | `POST /api/v1/billing/exports` | EXP-02 async export | Job spec `{customer_id, from, to, format}`; worker writes CSV/NDJSON; `GET .../exports/{job_id}` |
| P2 | `GET /api/v1/selfserve/billing/statement` | TEN parity | Same statement DTO as admin; tenant RBAC |

**Statement DTO (sketch):** `period`, `opening_balance_micro`, `closing_balance_micro`, `lines[]` (ROL-03 grouped by `ledger_type`), `invoices[]`, `payments[]`, `tax_breakdown`, `reconciliation` (`invoice_total` vs `ledger_sum`, `delta_micro`).

#### 2.8.3 Write paths and lifecycle

| Method | Route | DoD |
| :--- | :--- | :--- |
| GET/PUT | `/api/v1/customers/{id}/tax-profile` | CRUD on `billing.customer_tax_profiles`; shown on preview/detail |
| POST | `/api/v1/billing/invoices/{id}/void` | Sets `billing.invoices.status=VOID`; audit log; no ledger mutation |
| GET | `/api/v1/billing/invoices` (admin) | Optional cross-customer list: `?month=&status=&min_total=`; admin role only |

Mutations that affect Redis/campaign state still use outbox; void is billing-schema + audit only.

#### 2.8.4 Package refactor DoD

| Task | DoD |
| :--- | :--- |
| Create `internal/adminapi` | `billing/`, `ops/`, `export/` subpackages; `RegisterRoutes(mux, Deps)` — **done** |
| Move from `management` | `fanout_collector.go`, `fanout_cursor.go`, `handler_admin_api.go` → `adminapi/` — **done**; `service_admin_ops.go`, workers stay in `management` (outbox/notify coupling) |
| Thin `management` | `handler.go` calls `adminapi.RegisterRoutes` — **done** |
| Import boundary | One-way: `management` → `adminapi`; no `adminapi` → `management` — **done** |
| Tests | `go test ./internal/adminapi/... ./internal/billing/... -short` — **done** |

#### 2.8.5 SLA (additive)

| Metric | Target |
| :--- | :--- |
| `GET .../billing/statement` p99 | < 800 ms for customer with <= 50k ledger rows in period |
| `POST .../invoices/preview` p99 | < 2 s (same as `GenerateInvoice`) |
| Wallet GET p99 | < 300 ms (single customer, no fan-out) |

#### 2.8.6 Tests

```bash
go test ./internal/adminapi/... ./internal/billing/... -short
go test ./internal/adminapi/... -run 'Statement|Preview|Wallet|FanOut' -short
```

| Test | Criterion |
| :--- | :--- |
| Statement reconciliation | `closing = opening + sum(lines)`; matches invoice total for closed month |
| Preview idempotency | Preview matches post-generate invoice lines for same month |
| Preview skip | Zero spend → `would_skip: true`, no row inserted |
| Wallet placeholder | `payment_provider=placeholder`, no outbound HTTP |
| Void | VOID status; regenerate same month blocked or returns new policy per product decision |
| Package import | `go list -deps ./internal/management/...` does not pull `adminapi` into ingestion hot paths |

#### 2.8.7 Phase 2b — Completion Checklist

- [x] `internal/adminapi/` extracted; `management` registers routes via `adminapi.RegisterRoutes`
- [x] `GET .../billing/statement` + ledger drill-down + invoice preview (P0)
- [x] Wallet + tax-profile API + FIN-07 invariant HTTP (P1)
- [x] Delivery list/retry + billing summary for ops (P1)
- [x] `PlaceholderProvider` only; `BILLING_PAYMENT_PROVIDER_KEY` placeholder — no Stripe in billing
- [x] Forecast + async export + selfserve statement (P2)
- [x] Admin cross-customer invoice list (`GET /api/v1/billing/invoices?month=&status=&min_total=`)
- [x] `internal/adminapi/export/` subpackage (EXP-02/03)
- [x] `docs/ADMINISTRATIVE.md` FIN/JOIN/EXP rows marked implemented for delivered routes
- [x] `go test ./internal/adminapi/... ./internal/billing/... -short` green

---

## Milestone 3 - Commercial Platform (Licensing, Subscriptions, Admin UX)

**Goal:** Close the commercial loop for on-prem sales: non-blocking license server, tenant entitlements (Basic / Pro / Enterprise), daily ingress quotas (RPD), admin reporting UX for buyers/finance, and optional hybrid volume / PU packaging. Hot path reads only local snapshots; subscription fee and license fee are separate from ad spend.

**Sources:** `docs/MANAGEMENT.md` (§18–21, §13 roadmap waves), `docs/LICENSING.md`, `docs/SUBSCRIPTIONS.md`, `docs/proposals/ESPX-LP-2026-V1.md`, `GUIDE_STYLE_CODE.md` (R1b, R3, R8, R9), `GUIDE_CHAOS_RELIABILITY.md`, `GUIDE_IDEAS_MICROSERVICES.md`, `GUIDE_COMPLIANCE.md` (LIC-PRIVACY).

### 3.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R1b — new routes in `adminapi/{reports,dashboards,views,licensing}`; R8 hot reject for license/RPD; MICRO `license-server` **~12/18** vendor-only; entitlements lib in `management` |
| **Binaries** | `cmd/license-server` (vendor deploy); `cmd/management` + `cmd/processor` (meters); **no** license calls on `cmd/tracker` |
| **Packages** | `internal/licensing` flat; `internal/adminapi/reports|dashboards|views`; subscription SQL in `management/queries/` |
| **Patterns** | `LicenseWatcher` async last-known-good; `UPDATE_ENTITLEMENTS` outbox; `Effective(deployment,customer)`; registry snapshot for RPS/RPD/features; separate ledger `reference_type` for license vs spend |
| **SLA** | Hot path: **0 network I/O** for entitlements; admin license status p99 < 500 ms; meter lag < 5 min; entitlement refresh < 60 s |
| **Metrics** | `ad_license_state{state}`; `filterRejectLicenseExpired` via pre-bound reject spec; `ingress:day:*` Redis keys; `usage_meters` lag gauge |
| **Code** | Hot: new `filterRejectKind` rows in `filterRejectSpecs` — bench 0 allocs; cold: `LicenseStatusDTO`, `SubscriptionDTO` with godoc |
| **Chaos R10** | **Required** — `chaos_proof fault=license_grace_expired ingest_blocked=true`; license hub down in GRACE → ingest stable; subscription outbox → Redis |
| **CI gates** | `go test ./internal/licensing/... ./internal/adminapi/... -short`; perf-gate on filter reject paths; `check_comments`; CH reports use `chquery` when M6 landed |

**Status:** **done (core)** — licensing, subscriptions, hot-path gates, chaos proofs, and `adminapi/licensing` facet shipped. Admin report waves W1–W6 remain **M4** backlog.

**Depends on:** M2 (invoice, `usage_meters`, adminapi, billing gRPC).

**Soft dependency:** M9 `espx-install license` for operator UX; M3 may ship minimal `license install|activate` CLI without full installer wizard.

### 3.1 Architecture

| Principle | DoD |
| :--- | :--- |
| Hot-path isolation | `/track` does not call license server, billing gRPC, or Postgres subscription reads |
| Unified entitlements | `internal/licensing.Entitlements`; `Effective(deployment, customer)` |
| Two commercial layers | Product license (`deployment_id`) ∩ tenant subscription (`customer_id`) |
| Three ingress axes | RPS (UDP epoch), RPD (daily Redis), events/month (billing meter) |
| Money separation | `reference_type` ∈ `spend`, `subscription`, `license`, `topup` — separate ledger lines |
| Non-blocking license | `LicenseWatcher` async; last-known-good JWT; admin reads snapshot, not network per request |
| Admin UX | JSON only; `internal/adminapi/reports`, `dashboards`, `views` per `MANAGEMENT.md` §5 |
| Reject path | `filterRejectLicenseExpired`; `daily_quota_exceeded` 429; 0 allocs on reject bodies |

### 3.2 Product licensing (`internal/licensing`, `cmd/license-server`)

| ID | Component | DoD |
| :--- | :--- | :--- |
| LIC-VERIFY | Ed25519/PASETO verify | table tests: valid, expired, tampered, wrong `kid` |
| LIC-EMBED | `//go:embed` public key | rotation via `kid`; old JWT valid until expiry |
| LIC-WATCH | `LicenseWatcher` in management | `file` + `online`; heartbeat timeout 5s; circuit breaker; disk cache |
| LIC-REDIS | `entitlement:deployment:{id}` | registry reload without tracker restart |
| LIC-PG | `license_status` | `GET /api/v1/license/status` (`LicenseStatusDTO`) |
| LIC-STATES | ACTIVE / GRACE / EXPIRED / REVOKED | `ad_license_state`; grace ingest OK; EXPIRED fail-closed on track |
| LIC-SERVER | `cmd/license-server` (vendor) | issue, renew, revoke, activate, heartbeat, CRL |
| LIC-VENDOR-DB | `licenses`, `deployments`, `revocations` | separate vendor Postgres; no customer event payloads |
| LIC-PRIVACY | heartbeat payload | unit test: no click_id, IP, ledger; `TELEMETRY=0` default |
| LIC-PU | JWT `pricing.monthly_pu`, `pu_components` | κ matrix S/M/L bands per `ESPX-LP-2026-V1` §13; invoice line `reference_type=license` |

### 3.3 Tenant subscriptions

| ID | Component | DoD |
| :--- | :--- | :--- |
| SUB-DB | `subscription_plans`, `customer_subscriptions`, `usage_meters`, `usage_daily` | seed `basic`, `pro`, `enterprise` |
| SUB-API | `GET .../subscription`, `GET .../usage`, `GET .../usage/daily` | RBAC; `effective_limits`; godoc DTO |
| SUB-ADMIN | `POST /admin/customers/{id}/subscription` | outbox `UPDATE_ENTITLEMENTS` |
| SUB-METER | processor → `usage_meters` | idempotent `(customer, meter, period)`; lag < 5 min |
| SUB-INVOICE | overage in `GenerateInvoice` | `(events - limit) * unit_price_micro`; integration test |
| SUB-ENFORCE | `RequireFeature`, `RequireUnderLimit`, `RequireLicenseFeature` | 403 `plan_feature_required`, `license_limit_exceeded` |
| SUB-HOT | registry per customer | `rtb_live`, `ml_fraud_boost`, RPS + RPD from snapshot; bench no regression |
| SUB-DAILY | Redis `ingress:day:{customer}:{date}` | 429 `daily_quota_exceeded`; `X-RateLimit-*-Day` headers |
| SUB-DAILY-FLUSH | `DailyQuotaFlushWorker` | Redis → `usage_daily` every 5 min |
| SUB-QUOTA-API | `GET .../quota-status`, `POST .../quota-bump` | operator daily bonus via `overrides_json` |

Limits: `docs/SUBSCRIPTIONS.md` §3–5. RPD limits: `docs/MANAGEMENT.md` §21.

### 3.4 Admin UX — reports and dashboards (`internal/adminapi`)

| Wave | ID | Deliverables | DoD |
| :--- | :--- | :--- | :--- |
| **W1 Buyer** | ADM-W1 | `reports/metrics`, `traffic-sources`, `unit-economics`, `dashboards/buyer`, pacing-status ADT-31 | sort+cursor; `freshness` on CH rows; RBAC; `EXPLAIN` in `scripts/sql-explain/` |
| **W2 Postbacks** | ADM-W2 | `postbacks` table, ADT-20 ingest, matcher worker, postback recon, `MarginGuard`, accountant dashboard skeleton | idempotent postback; margin pause via priority outbox |
| **W3 Finance** | ADM-W3 | JOIN-01/02, `dashboards/cfo`, `operator`, accountant close steps, buy-sell ADT-84, scheduled export | `AccountantCloseDTO.steps` green on happy path |
| **W4 Ad ops** | ADM-W4 | `dashboards/adops`, geo pivot ADT-60, heatmap ADT-63, pacing drift ADT-32, RTB win-loss ADT-51 | — |
| **W5 Views** | ADM-W5 | `saved_report_views`, period compare, `ReportJobSpec` in export, recommendations ADT-90..93 | CRUD `/api/v1/views` |
| **W6 Fraud** | ADM-W6 | fraud dashboard/reports ADT-71/73/75, `alert_rules_worker`, partner statement ADT-80 | M3 feature gates wired |

Package layout: `MANAGEMENT.md` §5. Register via `adminapi.RegisterRoutes`.

### 3.5 Optional — hybrid volume licensing (`ESPX-LP-2026-V1`)

Not required for M3 closure unless `enforcement_mode=hybrid` in contract. Track separately:

| ID | Component | DoD |
| :--- | :--- | :--- |
| LP-VOL | `billable_events` meter + `billable_weights` | hourly `VolumeMeterWorker`; CH reject class column |
| LP-SOFT | volume breach zones 80/100/115% | metrics + admin banner; ingest continues in soft zone |
| LP-HARD | hard breach degrade | disable ML IVT batch, RTB shadow-only; no tracker shutdown |
| LP-EBPF | zero-cost ebpf drops in volume accounting | edge metric `edge_xdp_drops_total` in roll-up |

Canonical calendar EXPIRED remains fail-closed per `LICENSING.md` unless JWT sets `enforcement_mode: hybrid`.

### 3.6 SLA and constraints

| Metric | Value |
| :--- | :--- |
| Hot path entitlement check | 0 network I/O |
| `LicenseWatcher` | admin p99 impact < 500 ms |
| JWT verify (cold) | < 1 ms CPU |
| Entitlement refresh after plan change | < 60 s without tracker restart |
| Subscription meter lag | < 5 min |

### 3.7 Tests

```bash
go test ./internal/licensing/... -short
go test ./internal/billing/... -run 'Subscription|Usage' -short
go test ./internal/management/... -run 'Entitlement|License|DailyQuota' -short
go test ./internal/adminapi/... -short
```

| Test | Criterion |
| :--- | :--- |
| JWT verify / grace / EXPIRED | per LIC-* |
| RPD 429 | over limit → `daily_quota_exceeded` + headers |
| Feature gate | Basic tenant RTB live → 403 |
| Subscription overage | meter + invoice line |
| Registry reload | renew JWT in grace, no tracker restart |
| Heartbeat privacy | no PII in outbound JSON |

Chaos: `chaos_proof fault=license_grace_expired ingest_blocked=true`; license hub down 48h in grace → ingest stable.

### 3.8 Milestone 3 - Completion Checklist

**Licensing and subscriptions (required)**

- [x] `internal/licensing` + embedded public key + `LicenseWatcher` + `license_status`
- [x] `cmd/license-server` vendor endpoints + vendor DB migrations
- [x] `billing` subscription tables + seed `basic` / `pro` / `enterprise`
- [x] `UPDATE_ENTITLEMENTS` outbox + Redis snapshots + registry hook
- [x] Hot path: `filterRejectLicenseExpired`, RPD gate, feature flags
- [x] `GET /api/v1/license/status`, subscription/usage/quota APIs (`adminapi/licensing`)
- [x] `usage_meters` + invoice overage (`usage_daily` flush worker deferred)
- [x] Integration + license/spool/outbox chaos tests (`scripts/chaos-drills/m3/`)
- [x] Hot-path perf gate green (0 allocs on reject spec)
- [x] `docs/LICENSING.md`, `docs/SUBSCRIPTIONS.md`, `docs/milestones/m3/README.md` match implementation

**Admin UX (minimum for M3 close: W1 + W3 finance cockpit)**

- [ ] W1 Buyer: traffic-sources, unit-economics, buyer dashboard
- [ ] W3: customer-portfolio JOIN-02, CFO + accountant dashboards, operator home
- [ ] `internal/adminapi/reports` + `dashboards` registered

**Admin UX (stretch in same milestone if capacity allows)**

- [ ] W2 Postbacks + margin guard
- [ ] W4 Ad ops geo / pacing reports
- [ ] W5 Saved views + report jobs
- [ ] W6 Fraud dashboard + alert rules

**Commercial packaging (required for sales)**

- [ ] JWT `tier_level` S/M/L + `volume_quota_monthly` + feature flags per module (OpenRTB, eBPF, ML IVT)
- [ ] PU coefficients in license JWT / quote template (`ESPX-LP-2026-V1` §13)

**Optional (LP-V1 hybrid volume)**

- [ ] `billable_events` weighted meter + volume breach alerts
- [ ] `enforcement_mode` strict vs hybrid documented per customer

**Deferred to Milestone 9 (installer)**

- [ ] Full `espx-install license install|activate|status` wizard (minimal CLI acceptable in M3)

---

## Milestone 4 - Package Layout (Facet Subpackages)

**Goal:** Единые правила раскладки cold-path кода по поддиректориям без Clean Architecture. Эталон — `internal/adminapi/`: один корневой пакет + **facets** (смысловые подпакеты) глубиной не более одного уровня. Hot path (`internal/ingestion`, `internal/rtb`) остаётся плоским.

**Sources:** `GUIDE_STYLE_CODE.md` (R1–R2, R1b), `docs/MANAGEMENT.md` §5, `internal/adminapi/register.go`, `GUIDE_CHAOS_RELIABILITY.md` (R10 #6).

**Status:** **in progress** — M4 facets `dashboards`, `reports`, `views` scaffolded + registered; R1b documented; `chquery` reserved. Remaining: migrate legacy JSON from `management/`, W1–W6 implementation (M6).

**Depends on:** M2 (`adminapi` billing/ops/export). **Blocks:** M6 CHG/reports (`adminapi/reports`, `chquery` placement).

### 4.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE **R1b** (primary) — facet exception to R1; R2 file naming inside facets; R5 import rules |
| **Binaries** | None — structural milestone only; `cmd/management` wires `adminapi.RegisterRoutes` |
| **Packages** | `internal/adminapi/{billing,ops,export,reports,dashboards,views,licensing?,errs}`; `internal/database/chquery` stub; hot path **unchanged** flat |
| **Patterns** | Facet = domain noun (not `handlers/` layer); DTO in same facet file as SQL; one `register.go`; PKG-IMP-01 no `adminapi` → `management` import |
| **SLA** | N/A runtime — review gate: new `/api/v1` **must** land in facet or PR rejected |
| **Metrics** | N/A |
| **Code** | Files < 500 lines (R2 split); godoc on `RegisterRoutes` per facet |
| **Chaos R10** | **Not required** — layout refactor preserving behavior (R10 #5–#6) |
| **CI gates** | `go test ./internal/adminapi/...`; import-cycle check; document R1b in `GUIDE_STYLE_CODE.md` (PKG-MIG-04) |

### 4.1 Принципы (не Clean Arch)

| Правило | Смысл |
| :--- | :--- |
| Facet, не слой | Имя поддиректории = **предметная область** (`billing`, `reports`, `ops`), не `handler` / `service` / `repository` / `adapter` |
| Глубина ≤ 1 | Допустимо: `internal/adminapi/billing/handlers.go`. Запрещено: `internal/adminapi/reports/buyer/handlers.go` |
| Корень бинаря плоский | `internal/management/*.go`, `internal/billing/*.go` — workers, outbox, gRPC, HTMX; без вложенных domain-пакетов |
| Hot path без facets | `internal/ingestion/`, `internal/rtb/` — только R1 mechanical: `sqlc/`, `queries/`, `migrations/`, `pb/` |
| Один `register` на фасад | `internal/adminapi/register.go` монтирует HTTP; `cmd/management` только wire + `RegisterRoutes` |
| DTO у запроса | JSON types и map PG/CH→DTO в том же facet-файле, что SQL (`reports/buyer_sources.go`), не в отдельном `dto/` |
| Общие ошибки | `adminapi/errs` или `errs/errors.go` в корне facet-дерева; без `internal/common` |

### 4.2 Как именовать поддиректории

**Формат:** `internal/<root>/<facet>/`, lowercase, одно–два слова, существительное по смыслу API или домена.

| Допустимо | Запрещено |
| :--- | :--- |
| `billing`, `ops`, `export`, `reports`, `dashboards`, `views`, `licensing` | `api`, `http`, `handlers`, `services`, `domain`, `infra`, `pkg`, `lib`, `common`, `utils` |
| `db`, `queries`, `migrations`, `pb`, `sqlc` — только generated/SQL | `v1`, `internal`, `impl` |

**Когда заводить facet вместо префикса файла (R2):** в домене ≥ 3 HTTP handler-файла **или** суммарно > ~800 строк **и** логика не worker/outbox. Иначе — `service_foo.go` / `handler_foo.go` в корне сервиса.

**Имена файлов внутри facet** — те же R2: `handlers.go`, `types.go`, `service.go`, `<report>_queries.go`; тесты рядом (`handlers_test.go`).

### 4.3 Куда класть код (по бинарю)

| Корень | Остаётся плоским | Выносится в facet (`internal/.../<facet>/`) |
| :--- | :--- | :--- |
| **adminapi** | `register.go` | `billing`, `ops`, `export` (есть); **добавить** `reports`, `dashboards`, `views`; опционально `licensing` (read-only status/usage JSON) |
| **management** | `service*.go`, `handler*.go` (HTMX), `*_worker.go`, `outbox_*.go`, `ops.go` (`/health`, `/metrics`) | Не дублировать JSON — делегировать в adminapi; `adminapi_wire.go` остаётся в management |
| **billing** | gRPC service, invoice worker, PDF | Без HTTP facets; admin HTTP только через adminapi/billing |
| **licensing** | verify, state, entitlements types (M3) | Без HTTP; management watcher + adminapi facet при необходимости |
| **ingestion / processor** | весь hot/write path | Только `sqlc/`, `queries/`; **запрет** `ingestion/filter/` |
| **database** | `clickhouse_connect.go`, shared pools | **добавить** `chquery/` — обёртка SETTINGS для admin SELECT (M6 CHG-*) |

### 4.4 Импорты и границы

| ID | Правило | DoD |
| :--- | :--- | :--- |
| PKG-IMP-01 | Facet не импортирует `management` | `go test` + `go vet` на import cycles |
| PKG-IMP-02 | Соседние facets — через узкий интерфейс в wire или общий `types` в корне adminapi, не cross-import handlers | billing не импортирует ops handlers |
| PKG-IMP-03 | `pkg/` не тянет `internal/` | без изменений R pkg |
| PKG-REG | Каждый новый facet — строка в `RouteRegistry` + `RegisterRoutes` | godoc на `Register` facet |

### 4.5 Миграция (что сделать)

| ID | Действие | DoD |
| :--- | :--- | :--- |
| PKG-MIG-01 | Создать `adminapi/reports`, `dashboards`, `views` по `MANAGEMENT.md` §5 | **done** — scaffold + `Register`; handlers return 501 until M6 |
| PKG-MIG-02 | Перенести оставшийся JSON из `management/handler_*.go` в adminapi, если дублирует `/api/v1` | **partial** — `handler_api*`, disputes, forecast, consent → facets; `selfserve` next |
| PKG-MIG-03 | `adminapi_types.go` / раздутые DTO — в facet `types.go` рядом с handlers | файлы < 500 строк где возможно |
| PKG-MIG-04 | Документ **R1b** в `GUIDE_STYLE_CODE.md`: facet exception к R1, ссылка на adminapi | **done** |
| PKG-MIG-05 | `internal/database/chquery` — пакет для M6, зарезервировать имя сейчас | **done** — godoc stub |

**Не входит:** рефакторинг `internal/ingestion` на подпакеты; переименование всех `service_*.go` в management; monorepo split.

### 4.6 SLA и ограничения

| Metric | Value |
| :--- | :--- |
| Новый PR с JSON API | facet + register, иначе reject в review |
| Глубина путей | max 2 сегмента под `internal/` (`adminapi/reports`, не глубже) |
| Циклы импортов | 0 |

### 4.8 Control plane decomposition (M4.8)

**Goal:** Shrink `internal/management` (~195 files) into **control plane core** + sibling domain packages. See [CONTROL_PLANE_SPLIT.md](./milestones/m4/CONTROL_PLANE_SPLIT.md).

| Phase | Package | Status |
| :---: | :--- | :--- |
| 1 | `licensing/` (+ watcher, client, entitlement buffer) | **done** |
| 2 | `privacy/` | backlog |
| 3 | `supplyadmin/` | backlog |
| 4–8 | `rtbadmin`, `fraudadmin`, `deliveryctrl`, `campadmin`, `billingbridge` | backlog |

- [x] Split plan documented (`docs/milestones/m4/CONTROL_PLANE_SPLIT.md`)
- [x] Phase 1: licensing adjunct code out of `management/`
- [ ] Phase 2–8 extractions
- [ ] Rename `management` → `controlplane` after phase 7

---

### 4.7 Milestone 4 - Completion Checklist

- [x] R1b в `GUIDE_STYLE_CODE.md` (facet rules, anti-patterns)
- [x] `adminapi/reports`, `dashboards`, `views` созданы и зарегистрированы
- [x] M3 новые маршруты (subscription, license status) — в `adminapi/licensing` (W1/W3 reports → M6)
- [x] Legacy `/api/v1` из `handler_api*` / disputes / forecast / consent — в facets (selfserve остаётся)
- [x] `adminapi/errs` используется для общих API ошибок
- [x] Import-cycle check в CI или `go test ./internal/adminapi/...`
- [x] `docs/MANAGEMENT.md` §5 синхронизирован с фактом
- [x] `internal/database/chquery` — зарезервирован (stub ok)

---

## Milestone 5 - Regulatory Compliance & Passive Telemetry (Art. 361 UK)

**Goal:** Закрыть юридические риски ст. 361 УК, CFAA, GDPR/ePrivacy и EU AI Act (2026): только **defensive** perimeter (§1 гайда), полный запрет **offensive** паттернов (§2), immutable allowlist, audit trail, декларативный eBPF.

**Sources:** `GUIDE_COMPLIANCE.md` §1–2, `docs/EDGE.md`, `deploy/edge/xdp/bpf/edge_filter.c`, `internal/edge/*`, `internal/ingestion/device_filter.go`, M9 installer PF-*.

**Status:** backlog — после M4 (или параллельно с хвостом M3); **до production XDP / fraud auto-block at scale**. Не блокирует M4 layout.

**Depends on:** M2 (blacklist outbox, `admin_audit_log`), M1 (edge optional). **Blocks:** широкое включение `edge_xdp` в прод и ML auto-blacklist без allowlist guard.

### 5.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | **GUIDE_COMPLIANCE.md** §1–§4 (primary); STYLE R9; CHAOS R10 #3 for read-only allowlist unit tests only |
| **Binaries** | `cmd/edge-xdp`, `cmd/edge-bpf-sync` (node caps); `cmd/management` (outbox only); **veto:** ebpf in management/tracker |
| **Packages** | `internal/edge/allowlist`, `internal/edge/blocklist`; `TLSImpersonationWorker` in `management` |
| **Patterns** | Defensive path: inbound → optional XDP → nginx → gnet; block = PG txn + `admin_audit_log` + outbox + Redis + bpf-sync; CMP-EBPF-02 gate on all auto-blocks |
| **SLA** | XDP drop: wire-rate only; optional tarpit ≤ 15 s (`EDGE_TARPIT_MAX_MS`); no hot-path latency regression |
| **Metrics** | `edge_blocklist_skip_allowlisted_total`; `edge_tarpit_delay_seconds` (optional); `espx_edge_*` drop ratios (no source IP labels) |
| **Code** | `allowlist.IsProtected` pure Go; audit in same txn as blacklist; static analysis CMP-FORB-04 |
| **Chaos R10** | **Partial** — integration tests for allowlist/BPF diff; **no** full compose chaos for compliance grep (R10 #6) |
| **CI gates** | `scripts/ci/check_compliance.sh` **mandatory**; `go test ./internal/edge/...`; CMP-FORB-01..04 patterns |

### 5.1 Gap analysis

| Area | Implemented | Gap (M5) |
| :--- | :--- | :--- |
| **§1.A XDP self-defense** | `edge_filter.c` SYN/PPS/blocklist; `edge-bpf-sync` | Immutable allowlist in Go; `edge_block_audit`; auto-block only on rate breach + operator policy |
| **§1.B passive TLS/TCP** | `DeviceFilter`, TLS hash header | JA3/JA4 doc; `TLSImpersonationWorker`; no DOM/JS (CI) |
| **§1.C tarpit** | — | Optional: nginx/gnet slow path, max delay cap, metrics |
| **§2 offensive ban** | No hack-back in repo | CI `check_compliance.sh`; lint: no outbound-to-source-IP from management; no scan deps |
| **R3 kernel load** | `edge-xdp` pins maps; management non-root | Install audit; M9 dry-run manifest |

### 5.2 Defensive catalog (`CMP-DEF-*`) — allowed

| ID | Measure | DoD |
| :--- | :--- | :--- |
| CMP-DEF-01 | Wire-rate XDP drop | Documented in `GUIDE_COMPLIANCE.md` §1.A; blocks TTL-scoped; trigger = local rate breach or operator/fraud outbox |
| CMP-DEF-02 | Passive JA3/JA4 / headers | §1.B; `DeviceFilter` signal-only; impersonation worker |
| CMP-DEF-03 | Tarpit (optional) | §1.C; edge-only; `EDGE_TARPIT_MAX_MS` ≤ 15000; metric; no billing path |
| CMP-DEF-04 | No outbound offense | Static analysis / integration test: `management` HTTP/gRPC clients never dial visitor IP from block events |

### 5.3 Offensive ban (`CMP-FORB-*`) — never ship

| ID | Forbidden | DoD |
| :--- | :--- | :--- |
| CMP-FORB-01 | DOM / Canvas / WebGL fingerprint JS | CI grep fail; no SDK packages |
| CMP-FORB-02 | Hack back (reverse DDoS, flood origin) | No code paths; review checklist §6 |
| CMP-FORB-03 | Port scan / nmap / active probe | No deps; IVT stays CH-passive only |
| CMP-FORB-04 | management → kernel direct | No `cilium/ebpf` in management/tracker (CI) |

### 5.4 Allowlist & audit (`CMP-EBPF-*`)

| ID | Requirement | DoD |
| :--- | :--- | :--- |
| CMP-EBPF-01 | `allowlist.IsProtected(ip)` | Embedded CIDRs: customer `INSTALL_LAN_CIDR`, resolvers `8.8.8.8/32`, `1.1.1.1/32`, loopback; unit tests |
| CMP-EBPF-02 | Block gate | `BlockIPWithTTL` returns error if protected; outbox never enqueued |
| CMP-EBPF-03 | Sync gate | `blocklist.Store.ApplyDiff` skips protected IPs; metric `edge_blocklist_skip_allowlisted_total` |
| CMP-EBPF-04 | Kernel order | `edge_filter.c`: allow LPM before block LPM (existing); regression test |
| CMP-EBPF-05 | `edge_block_audit` | PG table: ip, reason_id, ttl, source, created_at; written in same txn as blacklist insert |
| CMP-EBPF-06 | Fraud auto-block | `ML_BLACKLIST_ADD` handler calls CMP-EBPF-02 before `BlockIPWithTTL` |

Topology unchanged: **management never calls `ebpf.Map.Update`** — only `cmd/edge-bpf-sync` after Redis sync.

### 5.5 Declarative install (`CMP-INST-*`)

| ID | Requirement | DoD |
| :--- | :--- | :--- |
| CMP-INST-01 | Process isolation | `go list` / CI: no `cilium/ebpf` in `internal/management`, `cmd/management`, `cmd/tracker` |
| CMP-INST-02 | `edge-xdp` audit | JSON log line on attach: iface, prog_id, pinned paths |
| CMP-INST-03 | M9 installer | `espx-install apply --dry-run` lists BPF/sysctl/ethtool changes; requires `--yes` for XDP |
| CMP-INST-04 | Capability doc | `docs/EDGE.md` Part V + `GUIDE_COMPLIANCE.md` §4.3 |

### 5.6 Tests

```bash
go test ./internal/edge/allowlist/... ./internal/edge/blocklist/... -short
go test ./internal/management/... -run 'BlockIP|Allowlist' -short
bash scripts/ci/check_compliance.sh
```

| Test | Criterion |
| :--- | :--- |
| Protected IP block | `BlockIPWithTTL("8.8.8.8")` → error, no Redis key |
| BPF diff skip | ApplyDiff skips protected; map unchanged |
| Audit row | block attempt → `edge_block_audit` + `admin_audit_log` |
| Impersonation | Chrome UA + python-requests JA3 → fraud flag, no block without policy |
| No hack-back | Code search: no dialer to `blocked_ip` from management workers |

### 5.7 Milestone 5 - Completion Checklist

- [ ] `GUIDE_COMPLIANCE.md` §1–2 (defensive/offensive matrix) + README/EDGE links
- [ ] `allowlist.IsProtected` + gates in management and blocklist sync
- [ ] `edge_block_audit` migration + write path
- [ ] `TLSImpersonationWorker` or documented defer with CMP-DEF-02 only
- [ ] `scripts/ci/check_compliance.sh` in CI (§2.A–C forbidden patterns)
- [ ] CMP-FORB-04: no ebpf in management/tracker
- [ ] CMP-DEF-04: no outbound-to-source-IP from management (test or lint)
- [ ] M9 dry-run manifest includes XDP when `edge_xdp: true`
- [ ] Optional: CMP-DEF-03 tarpit behind feature flag

---

## Milestone 6 - Day-2 Operations & Analytics Pipeline

**Goal:** Production-grade operability for AdTech buyers and DevOps: zero-downtime config propagation (hardening), split liveness/readiness probes, automated ClickHouse retention, resilient log shipping observability, and resource-governed admin analytics. Hot path (`cmd/tracker`, gnet) must never restart for campaign/blacklist/budget changes.

**Sources:** `docs/MANAGEMENT.md` (§6–7, §12–14), `docs/DATABASE.md`, `docs/ARCHITECTURE.md`, `GUIDE_CHAOS_RELIABILITY.md`, `GUIDE_STYLE_CODE.md` (R1b, R8.7 `/healthz`), `GUIDE_IDEAS_MICROSERVICES.md`, M1 write-path (D0/D1/D2, SEM-P5).

**Status:** backlog — after Milestone 5; before Multi-Region (M7).

**Depends on:** M1 (write-path spool/gates), M2 (adminapi, ops DLQ), M4 (reports/chquery facets), M5 (allowlist/compliance for ops dashboards), M3 optional (RPD meters feed readiness).

### 6.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R1/R8/R9 — scenarios F + spool; STYLE R1b `adminapi/reports` + `database/chquery`; MICRO **batch worker in `processor`** (7/18) — no `cmd/ch-janitor` |
| **Binaries** | `cmd/processor` (`CHPartitionJanitor`, health); `cmd/management` (chquery consumer, ops API); `cmd/tracker` (`/healthz`/`/readyz`) |
| **Packages** | `internal/database/chquery`; `internal/ingestion/registry.go` (HR-REG); `internal/adminapi/ops` (PIPE-ADMIN) |
| **Patterns** | Single-Writer reload (HR-SWR); incremental `UpdateAndWarmCampaign` (HR-PUB); `atomic.Value` registry COW; CH spool before ack (M1 D2); readonly CH DSN for admin SELECTs |
| **SLA** | Config visible p99 < 5 s; `/healthz` 0 allocs/op; `/readyz` p99 < 10 ms (cached atomics); admin CH query killed at `max_execution_time` |
| **Metrics** | `ad_registry_warm_duration_seconds`; `ad_ch_partitions_dropped_total`, `ad_ch_janitor_last_success_timestamp`; `ad_ch_spool_segments`; stream `XLEN` gauges; `ad_processor_ch_gate_inflight` |
| **Code** | Hot: no new locks on registry read; cold: `freshness` DTO on all CH reports; `chquery.Query` injects SETTINGS |
| **Chaos R10** | **Required** — `chaos_proof fault=clickhouse_outage_10m spool_recovered=true`; `registry_incremental_reload lag_p99_lt_5s=true`; PIPE spool readiness |
| **CI gates** | §0.7; tests in §6.9; perf-gate unchanged on hot path unless HR-REG touched |

### 6.1 Gap analysis (current codebase vs target)

| Area | Already implemented | Gaps (this milestone) |
| :--- | :--- | :--- |
| **Hot reload** | `management` outbox → `publishCampaignUpdate` → Redis `campaigns:update` (shard 0); tracker `Registry.Sync` + `atomic.Value` snapshot; watchers: `SettingsWatcher` (`config:version`), `ConsentStore`, `SlotMapWatcher`, RTB `rtb:catalog:reload`, GeoIP | Pub/sub triggers **full** PG `Sync`, not `UpdateAndWarmCampaign(id)`; no unified reload channel; global blacklist push path not consolidated; **no SIGHUP** (by design — Redis is canonical) |
| **Health** | Tracker gnet `GET /health` (PG + Redis shard pings, 503 `DEGRADED`); management `GET /health` (PG + Redis); processor `GET /health`; K8s readiness on tracker/management | No `/healthz` / `/readyz` split; metrics-port `/health` on tracker is **always 200**; no liveness probe; readiness ignores CH spool depth, stream `XLEN`, circuit breakers |
| **CH retention** | CH DDL: `PARTITION BY toYYYYMM` + TTL in `deploy/clickhouse/init.sql`; PG `PartitionManager` drops `events_p*` (`LOG_RETENTION_DAYS`) in `cmd/processor` | **No Go worker** for CH `DROP PARTITION` / TTL reconcile; TTL days not env-tunable; no operator API for retention policy |
| **Log pipeline** | Lua `XADD` → Redis Streams; `cmd/processor` dual consumers (`_pg` / `_ch`); `ProcessorChGate` semaphore; `CHSpool` mmap WAL before `XAck`; Redis DLQ `{stream}:dlq` + admin `DLQ_RETRY` outbox | User TZ Postgres DLQ for CH batches — **not used** (spool is canonical); missing unified backlog metrics (`XLEN`, spool segments, gate saturation); processor readiness does not fail when spool > threshold |
| **CH analytics governance** | `ConnectClickHouse`: global `max_execution_time=60`; forecast endpoints use 1.5s ctx timeout; `CampaignStatsDTO.stale` when CH lag > 5m | No per-query `max_memory_usage`; no `readonly=1` session for admin SELECTs; no shared `CHQuery` wrapper; `freshness` object incomplete on report DTOs |

### 6.2 Topology (as deployed)

```text
[cmd/management]  Single-Writer (PG + outbox)
    |  HSET/PUBLISH on Redis shard 0 (pubsub, config, blacklist, entitlements)
    |  Admin JSON API + CH report queries (cold path, SETTINGS injection)
    v
[cmd/tracker x4]  gnet /track — zero I/O to CH/PG on accept path
    |  Registry/Settings/Consent: lock-free read (atomic.Value / atomic.Pointer)
    |  UnifiedFilter Lua: budget/fcap/dedup + XADD ad:events:{shard}
    v
[Redis x4]  Streams ad:events:*  (buffer layer, memory-bound)
    |
    v
[cmd/processor]  XREADGROUP batches (CH_BATCH_SIZE, CH_FLUSH_INTERVAL_MS)
    |  ProcessorPgGate / ProcessorChGate (max concurrent inserts)
    |  PostgresStore (settle)     ClickHouseStore (analytics)
    |       |                              |
    |       v                              +-- success → XAck
    |   PG events_p*                       +-- retriable → CHSpool mmap (no XAck until durable)
    |                                      +-- poison → Redis :dlq → management DLQ_RETRY
    v
[ClickHouse]  impressions/clicks/conversions/fraud_events + MVs
[cmd/log-compactor]  warm → audit_log_rollups (cold tier; separate retention)
```

**Invariant:** tracker never bulk-inserts ClickHouse; processor owns stream consumption and spool recovery (M1 D2). Management owns config writes and analytics query governance — not hot-path stream consumption.

### 6.3 Hot reload hardening (`HR-*`)

| ID | Component | Current | DoD |
| :--- | :--- | :--- | :--- |
| HR-SWR | Single-Writer / Multi-Reader | management outbox handlers | Documented: only management mutates campaign rows, blacklist, slot map, entitlements; tracker/processor read-only on config keys |
| HR-REG | Campaign registry COW | `internal/ingestion/registry.go` `atomic.Value` | Hot path: 0 locks on `GetCampaign`; bench unchanged; forbid new `RWMutex` on registry read path |
| HR-PUB | Incremental reload | pub/sub → full `Sync()` | On `campaigns:update` payload UUID → `UpdateAndWarmCampaign(id)` when PG reachable; full `Sync()` fallback on parse error or PG recovery |
| HR-WARM | Budget cache warm | `budget_warmer.go` | Single-campaign warm completes < 2s; metric `ad_registry_warm_duration_seconds` |
| HR-BL | Global blacklist | `redis_global.go`, edge `access_check.lua` | Blacklist change → outbox → all 4 shards + optional edge reload signal; lag p99 < 5s |
| HR-KEYS | Hash tags per campaign | Lua scripts | Keys `budget:{campaign_id}:…`, `fcap:{campaign_id}:…` share slot via `{campaign_id}` tag; integration test: no CROSSSLOT |
| HR-ENT | Entitlements (M3) | `UPDATE_ENTITLEMENTS` outbox | Registry reload for `rtb_live`, RPS/RPD caps without tracker restart; ties to M3 SUB-HOT |

**Non-goals:** SIGHUP file reload (use Redis + PG outbox). Replacing `atomic.Value` with `atomic.Pointer` on registry unless bench proves win.

### 6.4 Health checking (`HC-*`)

| ID | Endpoint | Process | DoD |
| :--- | :--- | :--- | :--- |
| HC-LIVE | `GET /healthz` | tracker, processor, management | **No I/O**; returns 200 if process alive; used as K8s `livenessProbe` |
| HC-READY | `GET /readyz` | tracker | gnet: PG + all Redis shards healthy (`StartHealthProbe` atomics); 503 removes from LB rotation |
| HC-READY-P | `GET /readyz` | processor | PG pool ping + CH ping + `CHSpool` below `CH_SPOOL_MAX_SEGMENTS` + stream lag < `PROCESSOR_READY_MAX_LAG` |
| HC-READY-M | `GET /readyz` | management | PG + Redis; optional: CH readonly ping for report plane |
| HC-MET | Tracker metrics `/health` | `cmd/tracker` METRICS_PORT | Deprecate always-200 stub; proxy readiness or remove |
| HC-OS | Host metrics | management `/metrics` | Gauges: Redis stream `XLEN` per shard, `ad_ch_spool_segments`, circuit breaker state per shard (`ad_redis_circuit_open`), optional `/proc/net/dev` drop counters |
| HC-NGINX | Edge probe doc | `deploy/nginx/` | `max_fails` + `proxy_next_upstream` on tracker `readyz`; document in `docs/EDGE.md` |

Tracker gnet `/health` remains for backward compat; alias or deprecate in favor of `/readyz` after K8s manifest update.

### 6.5 ClickHouse retention janitor (`CHJ-*`)

| ID | Component | DoD |
| :--- | :--- | :--- |
| CHJ-CFG | Retention config | Env `CH_RAW_RETENTION_DAYS`, `CH_FRAUD_RETENTION_DAYS`, `CH_ROLLUP_RETENTION_DAYS`; defaults match `deploy/clickhouse/init.sql` |
| CHJ-WORK | `CHPartitionJanitor` | Worker in `cmd/processor` (same binary as PG `PartitionManager`); daily tick |
| CHJ-DROP | Partition drop | `ALTER TABLE … DROP PARTITION` for months older than retention; idempotent; log partition id |
| CHJ-TTL | TTL reconcile | Optional `ALTER TABLE … MODIFY TTL` when env overrides DDL defaults |
| CHJ-MET | Metrics | `ad_ch_partitions_dropped_total`, `ad_ch_janitor_last_success_timestamp` |
| CHJ-API | Operator read | `GET /api/v1/ops/clickhouse/retention` — effective policy + last janitor run (management, RBAC ops) |

PG `events_p*` retention stays in `internal/database/partition_manager.go` — separate from CH janitor.

### 6.6 Log pipeline observability & DLQ (`PIPE-*`)

| ID | Component | Current | DoD |
| :--- | :--- | :--- | :--- |
| PIPE-ACK | Durable ack rule | spool → ack | M1 D2 unchanged: no `XAck` until PG row or CH spool fsync |
| PIPE-GATE | CH concurrency cap | `ProcessorChGate` | Metric `ad_processor_ch_gate_inflight`; readiness fails when at cap > 30s |
| PIPE-SPOOL | Spool recovery | `ch_spool.go` | Startup replay; chaos: CH down 10m → spool grows → processor `readyz` 503 → recovers without event loss |
| PIPE-DLQ | Failure routing | Redis `:dlq` | Poison pills only; transient CH errors **must not** DLQ (retriable per `store_errors.go`) |
| PIPE-ADMIN | DLQ ops | M2 OPS-04/05 | Extend dashboard: spool segment count, per-shard stream lag, PEL age p99 |
| PIPE-PG-DLQ | Postgres CH DLQ | — | **Optional / not default** — if required for compliance, mirror spool head to `billing.ch_write_dlq` without blocking ack path |

### 6.7 ClickHouse query governance (`CHG-*`)

| ID | Component | DoD |
| :--- | :--- | :--- |
| CHG-WRAP | `internal/database/chquery` | `Query(ctx, sql, CHQueryOpts)` injects `SETTINGS max_memory_usage, max_execution_time, readonly=1` |
| CHG-DSN | Readonly role | `CH_READONLY_DSN` for management/adminapi SELECTs; migrations stay on privileged DSN (processor only) |
| CHG-LIM | Defaults | `CH_QUERY_MAX_MEMORY_BYTES` (e.g. 10GiB), `CH_QUERY_MAX_EXECUTION_SEC` (30 admin / 60 internal) |
| CHG-FRESH | Report DTOs | All CH-backed admin routes return `freshness: {as_of, stale, consistency, ch_lag_seconds}` per `MANAGEMENT.md` §14 |
| CHG-ERR | OOM protection | Integration test: heavy `GROUP BY` returns 503 `query_resource_limit` without CH process OOM |
| CHG-MIGRATE | Call-site sweep | `service_campaign_stats`, `service_forecast`, `service_bid_floor`, `service_mab`, `adminapi/reports/*` use `chquery` |

### 6.8 SLA and constraints

| Metric | Value |
| :--- | :--- |
| Config visible on tracker after management commit | p99 < 5 s (pub/sub + warm) |
| `/healthz` handler | 0 allocs/op; no syscalls except clock |
| `/readyz` probe interval | LB 1s; handler p99 < 10 ms (cached atomics on tracker) |
| CH janitor | Runs off-peak; no impact on processor insert p99 |
| Admin CH query | Hard kill at `max_execution_time`; CH server stays up under concurrent heavy reports |

### 6.9 Tests

```bash
go test ./internal/ingestion/... -run 'Registry|Health|Spool' -short
go test ./internal/database/... -run 'CHQuery|Partition' -short
go test ./internal/management/... -run 'Retention|Ready' -short
go test ./internal/adminapi/... -run 'Freshness' -short
```

| Test | Criterion |
| :--- | :--- |
| HR-PUB incremental | publish one campaign → only that id reloaded + warmed |
| HC-READY | Redis shard down → tracker `readyz` 503; live traffic drained by LB |
| CHJ-DROP | janitor drops partition older than retention in test CH |
| PIPE chaos | CH outage → spool fills → `readyz` 503 → recovery → no duplicate rows (`insert_deduplicate`) |
| CHG-ERR | `max_memory_usage` exceeded → query error, CH alive |

Chaos: `chaos_proof fault=clickhouse_outage_10m spool_recovered=true`; `chaos_proof fault=registry_incremental_reload lag_p99_lt_5s=true`.

### 6.10 Milestone 6 - Completion Checklist

**Hot reload (required)**

- [ ] HR-PUB: `UpdateAndWarmCampaign` wired to `campaigns:update` pub/sub
- [ ] HR-BL: blacklist global replication lag metric + integration test
- [ ] HR-KEYS: hash-tag cross-slot test in CI
- [ ] Runbook: Single-Writer config flow in `docs/MANAGEMENT.md` §6

**Health (required)**

- [ ] `/healthz` + `/readyz` on tracker, processor, management
- [ ] K8s: liveness → `healthz`, readiness → `readyz` (manifests + compose healthcheck migration)
- [ ] HC-OS metrics: stream length, spool segments, circuit breaker
- [ ] Fix tracker metrics-port health stub

**ClickHouse Day-2 (required)**

- [ ] `CHPartitionJanitor` in processor + env retention knobs
- [ ] `GET /api/v1/ops/clickhouse/retention`

**Log pipeline (required)**

- [ ] Processor `readyz` includes spool + stream lag gates
- [ ] Ops dashboard: DLQ + spool + lag (extends M2 OPS routes)

**Analytics governance (required for M3 report waves)**

- [ ] `chquery` wrapper + `CH_READONLY_DSN`
- [ ] `freshness` on all new M3 CH report endpoints
- [ ] CH OOM integration test

**Optional**

- [ ] PIPE-PG-DLQ audit mirror
- [ ] eBPF `/proc/net/dev` drop counters on edge profile

---

## Milestone 7 - Multi-Region

**Goal:** A second regional cell (hot path) with a global control plane in Postgres. No cross-region I/O on `/track`.

**Sources:** `docs/MULTI_REGION.md`, `docs/IDEMPOTENCY_CORE.md` (region bits), `GUIDE_CHAOS_RELIABILITY.md` (Chaos Kong - manual only), `GUIDE_STYLE_CODE.md`, `GUIDE_IDEAS_MICROSERVICES.md`.

### 7.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R3 blast radius per region; Kong **manual game day only** (not CI); MICRO workers in `management`/`processor` — no `cmd/region-relay` |
| **Binaries** | `cmd/tracker`/`processor` per cell; `cmd/management` global control plane; second compose `compose.region-b.yaml` |
| **Packages** | `RegionOutboxRelay`, `QuotaManager` in `internal/management` flat; regional `SyncWorker` in `internal/ingestion` |
| **Patterns** | Hot path locality (no cross-region Redis/PG); `outbox_region_delivery` + `region_apply_idempotency`; `budget:quota:{region}:{campaign}`; global UUID region byte |
| **SLA** | `/track` p99 < 80 ms per region; `ad_region_relay_lag_seconds` p99 < 5 s; `sum(regional_spend) <= budget_limit` |
| **Metrics** | `ad_region_relay_lag_seconds`; per-region quota keys; cross-region budget invariant in recon |
| **Code** | Cold: region APIs with godoc; hot: embed `region_code` in idempotency token — 0 allocs |
| **Chaos R10** | **Integration required** for relay idempotency; **Chaos Kong** manual only — `chaos_proof fault=region_outage_manual ...` template |
| **CI gates** | `go test` region relay + quota integration; existing chaos suite green; perf-gate per region |

### 7.1 Topology

```
[Global Control Plane - PG primary + sync AZ + async DR]
        | outbox_region_delivery (async)
        v
[Region A cell]              [Region B cell]
  tracker x4                   tracker x4
  redis x4 (isolated)          redis x4 (isolated)
  processor                    processor
  NO cross-region Redis        NO cross-region Redis
```

| Rule | DoD |
| :--- | :--- |
| Hot path locality | `/track` in region B does not call Redis/PG in region A; traceroute / metrics confirm |
| Redis isolation | No replication between regional Redis clusters |
| GeoDNS / Anycast | Campaign traffic -> nearest cell; documented routing table |
| DR Postgres | Async replica in region B (R3); failover runbook; RPO documented |

### 7.2 Data Structures

| Component | Specification |
| :--- | :--- |
| Global UUID | Byte `[8]` = `region_code` (see `MULTI_REGION.md`); generator in tracker embeds region |
| 64-bit idempotency token | Region ID 4 bit in `IDEMPOTENCY_CORE.md`; collision test T-ID-02 across 2 regions |
| `outbox_region_delivery` | `event_id`, `target_region`, `status` (PENDING/DELIVERED/FAILED) |
| `region_apply_idempotency` | `(region, outbox_event_id)` PK; duplicate relay -> no-op |
| `campaigns.enabled_regions` | TEXT[] or junction table; campaign inactive in undelivered region |
| `PUT /api/v1/campaigns/{id}/regions` | Mutate `enabled_regions` via outbox; godoc on handler; UI out of scope |
| `budget:quota:{region}:{campaign_id}` | Local Redis key; refill via `QuotaManager` chunk reservation in global PG |
| Quota reservation PG | `quota_reservations(id, campaign_id, region, amount_micro, status)` |

### 7.3 Components

| Component | DoD |
| :--- | :--- |
| `RegionOutboxRelay` | Per-region worker; poll `outbox_region_delivery`; apply to all 4 local shards; status DELIVERED only after all shard writes ACK |
| `QuotaManager` | Reserve chunk in PG transaction; credit `budget:quota` in regional Redis; hot path debits quota only |
| `SyncWorker` | Regional spend deltas -> global PG `UpdateSpend`; idempotent via `sync_idempotency` |
| Global config relay | Blacklist, config:values, fraud boosts - same outbox path as single-site, scoped by region |
| Second compose cell | `docker compose -f compose.region-b.yaml` brings up full stack; `REGION_CODE` env |

### 7.4 SLA (per region)

| Metric | Value |
| :--- | :--- |
| `/track` p99 | < 80 ms (no cross-region RTT in filter path) |
| Region relay lag | `ad_region_relay_lag_seconds` p99 < 5 s steady state |
| Quota exhaustion | Campaign pauses in region when local quota = 0; other regions unaffected |
| Global budget | `sum(regional_spend) <= campaigns.budget_limit` across all regions |

### 7.5 Tests and Drills

**Automated:**
- Integration: `RegionOutboxRelay` duplicate delivery -> single Redis key state
- E2E: campaign with `enabled_regions=[A,B]` delivers in both cells after relay
- Idempotency: same `outbox_event_id` relayed twice -> `region_apply_idempotency` blocks double apply
- Quota: spend in region A does not debit region B quota key

**Manual game day (Chaos Kong - not CI):**
- Documented runbook: region A full outage; region B continues; global PG available; relay backlog drains on recovery
- Proof template: `chaos_proof fault=region_outage_manual region_a=down region_b=steady budget_consistent=true`

**Perf:** hot-path benches unchanged per region; `GetShard` 0 allocs/op.

### 7.6 Milestone 7 - Completion Checklist

- [ ] Global UUID + regional token deployed in all trackers
- [ ] `outbox_region_delivery` + `RegionOutboxRelay` + `region_apply_idempotency`
- [ ] `QuotaManager` + per-region quota keys
- [ ] `PUT /api/v1/campaigns/{id}/regions` + `enabled_regions` outbox delivery
- [ ] Postgres R1-R4 (sync AZ, PITR, async DR) documented and tested failover
- [ ] Second regional cell in compose; GeoDNS routing documented
- [ ] Cross-region budget invariant test passes
- [ ] `docs/MULTI_REGION.md` checklist complete
- [ ] Chaos Kong runbook executed once; results archived

---

## Milestone 8 - Crypto Gateway

**Goal:** Accept BTC / ETH / USDT via an external payment provider (variant A) with crediting to `balance_ledger` through the existing settlement pipeline. Billing and settlement gRPC contracts unchanged.

**Sources:** `docs/CRYPTO_GATEWAY.md`, `GUIDE_IDEAS_MICROSERVICES.md` (payment provider abstraction), `GUIDE_CHAOS_RELIABILITY.md` (payment chaos), `GUIDE_STYLE_CODE.md` (R8 cold, `provider_*.go`).

### 8.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | MICRO **payment 16/18** — stays standalone; CHAOS R10 #12 payment/settlement; STYLE R2 `provider_*.go`, R8.2 webhook errors |
| **Binaries** | `cmd/payment` only for crypto webhooks + provider keys; settlement unchanged in `cmd/management` |
| **Packages** | `internal/payment/provider_crypto.go`; schema `payment.crypto_*`; **veto** hot-path import |
| **Patterns** | `Provider` interface; webhook HMAC verify; outbox → `ApplyPaymentCredit`; idempotent `provider_event_id`; ledger `reference_type=crypto_intent` |
| **SLA** | Webhook handler p99 < 1 s; confirmation gates per asset (BTC 6, ETH/USDT 12 blocks) |
| **Metrics** | Existing payment outbox + settlement metrics; crypto-specific counter `payment_crypto_webhook_total{status}` |
| **Code** | Cold: no ignored `json.Unmarshal`; secrets only in payment process |
| **Chaos R10** | **Required** — `chaos_proof fault=crypto_webhook_replay idempotency_verified=true`; settlement down + crypto outbox |
| **CI gates** | `go test ./internal/payment/... -race`; existing `payment/fault_injection_test.go`; `make lint` |

### 8.1 Architecture

| Layer | Change |
| :--- | :--- |
| `cmd/payment` | New `CryptoProvider` implements `Provider`; webhook route `/webhooks/crypto/{provider}` |
| `internal/payment/schema` | `payment.crypto_transactions`, `payment.crypto_webhook_events` |
| Settlement | Unchanged: outbox -> `ApplyPaymentCredit` -> `balance_ledger` |
| `cmd/billing` | Unchanged |
| `cmd/management` | `GET /api/v1/customers/{id}/payments` - crypto intent status in JSON (payment gRPC) |

Payment remains standalone (score 16/18). Hot path does not import payment.

### 8.2 Data Structures

| Table | Fields |
| :--- | :--- |
| `payment.crypto_intents` | `id`, `customer_id`, `provider`, `idempotency_key`, `amount_micro_expected`, `currency`, `status`, `deposit_address`, `expires_at` |
| `payment.crypto_webhook_events` | `provider_event_id` UNIQUE, `payload`, `processed_at` |
| Ledger entry | `reference_type=crypto_intent`, `reference_id=intent.id`; idempotent on `reference_id` |

**Amount conversion at confirmation:**
```
amount_micro = floor(crypto_amount * fx_rate_at_confirmation * 1_000_000)
```
FX rate and crypto amount stored in metadata JSON on intent row.

### 8.3 Confirmations

| Asset | Min confirmations before credit |
| :--- | :--- |
| USDT (Ethereum) | 12 blocks |
| BTC | 6 blocks |
| ETH | 12 blocks |

Credit runs only after threshold; partial confirmations -> status `PENDING_CONFIRMATIONS`.

### 8.4 Provider Integration (variant A)

| Step | DoD |
| :--- | :--- |
| Create intent | gRPC `CreateCryptoCheckout` -> provider API -> deposit address + hosted URL |
| Webhook | HMAC/signature verify; duplicate `provider_event_id` -> 200 without second credit |
| Outbox | `PAYMENT_CREDIT` event -> settlement gRPC (same as Stripe path) |
| Recon | `ReconService` compares on-chain amount (from provider API) vs `amount_micro_expected` (+/- tolerance 1 micro-unit) |

### 8.5 SLA and Security

| Metric | Value |
| :--- | :--- |
| Webhook handler p99 | < 1 s (excludes blockchain wait) |
| Provider timeout | Circuit breaker; outbox PENDING on settlement down |
| Secrets | Provider API keys only in `cmd/payment` process |
| PCI scope | No card data; crypto keys isolated in payment container |

### 8.6 Tests

```bash
go test ./internal/payment/... -short
go test ./internal/payment/... -run Crypto -race
```

| Test | Criterion |
| :--- | :--- |
| Webhook idempotency | Duplicate webhook -> 1 ledger row |
| Confirmation gate | Credit blocked until N confirmations; then exactly 1 credit |
| FX metadata | `fx_rate` and `crypto_amount` persisted on intent |
| Settlement down | Outbox PENDING; no orphan credit; recovery -> PROCESSED |
| Recon mismatch | Deliberate amount drift -> alert; no auto-credit |

**Chaos (R10 - required):**
- `payment/fault_injection_test.go` crypto webhook replay
- `chaos_proof fault=crypto_webhook_replay idempotency_verified=true`
- Settlement server down scenario (existing) passes with crypto outbox rows

### 8.7 Milestone 8 - Completion Checklist

- [ ] `CryptoProvider` + migrations + gRPC create checkout
- [ ] Webhook route with signature verification
- [ ] Confirmation thresholds enforced per asset
- [ ] FX conversion at confirmation; metadata stored
- [ ] Settlement outbox path end-to-end in compose
- [ ] ReconService crypto reconciliation
- [ ] `GET /api/v1/customers/{id}/payments` includes crypto intent status (JSON)
- [ ] Chaos webhook replay + settlement down scenarios pass
- [ ] `docs/CRYPTO_GATEWAY.md` matrix rows marked done
- [ ] Milestones 1-7 DoD still satisfied (no hot-path regression)

---

## Milestone 9 (backlog) - CLI Installer

**Goal:** A single CLI for host validation, installing missing dependencies, and deploying eSPX for a chosen profile (single VPS, compose, optional K8s). The installer is not on the hot path and does not replace `scripts/local-dev` for development.

**Sources:** `deploy/edge/README.md`, `scripts/edge-tuning/`, `scripts/local-dev/dev_stack.sh`, `scripts/k8s/install_k3s.sh`, `scripts/ci/check_deps.sh`, `docs/CONCEPTS.md` (eBPF/XDP), `GUIDE_STYLE_CODE.md` (R4), `GUIDE_COMPLIANCE.md` (CMP-INST-03), `GUIDE_CHAOS_RELIABILITY.md` (R10 #8).

**Status:** backlog; does not block Milestones 1-8.

### 9.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R4 — `cmd/installer/main.go` wiring only; MICRO **~5/18** monolith package `internal/installer`; CHAOS **not required** (off ingest path) |
| **Binaries** | `cmd/installer` (`espx-install`); invokes existing scripts — no duplicate business logic |
| **Packages** | `internal/installer` flat: `preflight`, `provision`, `profile`, `render`; `packages.yaml` |
| **Patterns** | Idempotent `apply`; `dry-run` manifest for XDP (M5); secrets once in `/etc/espx/secrets.env`; `telemetry_enabled: false` default (M10) |
| **SLA** | `preflight` < 30 s; `provision` no `dist-upgrade`; repeat `apply` no-op |
| **Metrics** | N/A on hot path; `doctor` wraps `check_deps` + topology scripts |
| **Code** | Godoc on `PreflightCheck`, `InstallProfile`; table-driven PF-* tests |
| **Chaos R10** | **Not required** (R10 #8 helper CLI) |
| **CI gates** | `go test ./internal/installer/... -short`; golden render files; VM apply manual |

### 9.1 CLI Architecture

| Component | Location | DoD |
| :--- | :--- | :--- |
| Binary | `cmd/installer/main.go` | `go build -o espx-install ./cmd/installer`; no business logic in `main` (R4) |
| Core package | `internal/installer/` | flat package: `preflight`, `provision`, `profile`, `render` |
| Config output | `/etc/espx/install.yaml` (or `$ESPX_CONFIG_DIR`) | Idempotent re-run; merge with existing file via explicit `--force` |
| Shell escape hatch | invoke existing scripts | `edge_phase0.sh`, `install_k3s.sh`, `dev_stack.sh` - subprocess with pinned paths from `scripts/lib/paths.sh` |

**Commands (minimal surface):**

```text
espx-install preflight [--strict] [--json]
espx-install provision  [--yes]           # install missing OS packages only
espx-install configure  [--interactive]   # wizard: profile + feature flags
espx-install apply      [--dry-run]       # render units/compose/k8s manifests and apply
espx-install doctor     [--json]          # post-install health (wraps check_deps + topology)
```

Command contracts documented in godoc on exported types; OpenAPI not required.

### 9.2 Preflight - Host Checks

| Check ID | What is checked | Fail criteria | Auto-fix (`provision`) |
| :--- | :--- | :--- | :--- |
| PF-KERNEL | `uname -r` >= min (documented; e.g. 6.1+ for BTF/eBPF) | Version below min | No (report + doc link); exit code 2 |
| PF-BTF | `/sys/kernel/btf/vmlinux` or `CONFIG_DEBUG_INFO_BTF=y` | XDP profile selected, BTF missing | No |
| PF-NIC | `INGRESS_INTERFACE` exists; driver in allowlist; RX ring >= min | Interface not found or driver without XDP native/offload | Hint `ethtool -i`; NIC tune script |
| PF-XDP-ATTACH | dry-run `bpftool prog load` / cilium ebpf probe (non-destructive) | Cannot load test object | `linux-headers`, `clang`, `llvm`, `libbpf-dev` |
| PF-MEM | RAM >= profile minimum (VPS: 8 GiB; K8s control: 16 GiB) | Below threshold | No |
| PF-CPU | `nproc` >= profile minimum; NUMA hint if > 1 socket | Below threshold | No |
| PF-CGROUP | cgroup v2 unified | v1 only with K8s profile selected | No |
| PF-LIBS | docker, compose plugin, curl, jq, pg_isready, redis-cli | Binary missing | `apt`/`dnf` install by OS family |
| PF-PORTS | 5432, 6379, 8180-8188 free (configurable) | Required port occupied | Report conflicting process |
| PF-ULIMIT | `nofile` >= 1048576 for edge/tracker profile | Below | `limits.conf` snippet render |
| PF-SYSCTL | keys from `deploy/edge/99-espx-edge.conf` | Deviation with `--strict` | `edge_sysctl.sh apply` |

**Preflight output:**
- human: table `CHECK | STATUS | DETAIL | FIX`
- `--json`: array `{id, status, detail, fix_available, fix_command}`
- exit 0 = all pass; 1 = failures; 2 = hard block (kernel)

Existing `scripts/ci/check_deps.sh` is invoked from `doctor` after apply, not duplicated line-by-line.

### 9.3 Deployment Profiles (install-time configuration)

Wizard `configure --interactive` writes `install.yaml`:

```yaml
profile: single_vps          # single_vps | compose_dev | k8s_k3s
region_code: 0
features:
  edge_xdp: false            # requires PF-NIC, PF-BTF
  edge_phase0: true          # sysctl + nic-tune
  hot_path: true               # tracker x4 + redis x4
  cold_path: true              # management, processor, auth, payment, billing, notifier
  fraud_scoring: false
  log_evacuator: false
  multi_region: false          # blocked if profile=single_vps without second host
  telemetry_enabled: false     # M10: vendor perf export; default off
topology:
  redis_shards: 4
  trackers: 4
runtime:
  orchestrator: systemd        # systemd | compose | k8s
```

| Profile | Purpose | Orchestrator | K8s |
| :--- | :--- | :--- | :--- |
| `single_vps` | Single VPS, minimal ops | systemd units + host network | No |
| `compose_dev` | Local/staging parity | docker compose (`scripts/local-dev`) | No |
| `k8s_k3s` | Elastic cold path / multi-node | k3s (`scripts/k8s/install_k3s.sh`) | Optional; opt-in in wizard |

Profile validation rules:
- `edge_xdp: true` requires pass PF-KERNEL, PF-BTF, PF-NIC, PF-XDP-ATTACH
- `k8s_k3s` requires pass PF-CGROUP; hot path may remain on bare metal (split deployment)
- `multi_region: true` requires completed Milestone 7 DoD

### 9.4 Provision and Apply

| Step | DoD |
| :--- | :--- |
| OS detection | `ID` from `/etc/os-release`; Debian/Ubuntu and RHEL-family support in first phase |
| Package map | YAML `internal/installer/packages.yaml`: package name per OS family |
| `provision --yes` | Installs only missing packages; idempotent; no full system upgrade |
| Template render | systemd units, `.env`, compose override, k8s kustomize patch - from embedded templates |
| `apply --dry-run` | Prints diff; does not modify system |
| `apply` | Writes configs, `daemon-reload`, `compose up -d` or `kubectl apply`; does not overwrite secrets without `--force` |
| Secrets | Generate `INTERNAL_TOKEN`, DB passwords - once; store in `/etc/espx/secrets.env` chmod 600 |

### 9.5 Data Structures

| Artifact | Format |
| :--- | :--- |
| `install.yaml` | Install profile (see 9.3) |
| `packages.yaml` | OS family -> package list |
| `preflight_report.json` | Machine-readable check results |
| `PreflightCheck` struct | `{ID, Status, Detail, FixAvailable, FixCommand}` - godoc |
| `InstallProfile` struct | Validates feature combinations |

### 9.6 SLA and Constraints

| Metric | Value |
| :--- | :--- |
| `preflight` duration | < 30 s on typical VPS |
| `provision` | Only explicitly listed packages; no `dist-upgrade` |
| Idempotent re-run | Repeat `apply` without changing `install.yaml` - no-op (exit 0) |
| Privilege | `preflight` without root; `provision`/`apply` - root or documented sudo |

The installer does not guarantee hot-path SLA until Milestone 1 and edge tuning are complete; `doctor` reports metric status, does not achieve it itself.

### 9.7 Tests

```bash
go test ./internal/installer/... -short
go test ./internal/installer/... -run Preflight -short   # table-driven checks with fake sysfs
```

| Test | Criterion |
| :--- | :--- |
| Profile validation | `edge_xdp` without BTF -> configure error |
| Package map | Debian + RHEL mapping covered table-driven |
| Render golden | systemd/compose output matches golden files |
| Idempotent apply | Dry-run twice - identical diff |
| JSON preflight | `--json` schema stable; all check IDs documented |

CI: unit tests only; integration apply - manual VM job (does not block PR).

### 9.8 Milestone 9 - Completion Checklist

- [ ] `cmd/installer` + `internal/installer` with preflight, provision, configure, apply, doctor commands
- [ ] PF-KERNEL, PF-NIC, PF-BTF, PF-XDP-ATTACH, PF-LIBS implemented
- [ ] `provision` installs missing packages on Debian/Ubuntu
- [ ] Wizard: single_vps / compose_dev / k8s_k3s + feature flags
- [ ] Render systemd (single VPS) and integration with `dev_stack.sh` / `install_k3s.sh`
- [ ] `install.yaml` idempotent apply; secrets generated once
- [ ] godoc on exported installer API
- [ ] `docs/DEVELOPMENT.md` or `deploy/installer/README.md` - operator runbook
- [ ] `espx-install license install|activate|status` (commercial activation; depends M3 licensing)
- [ ] `install.yaml` field `telemetry_enabled: false` default (see M10)

---

## Milestone 10 (backlog) - Vendor Performance Telemetry (Opt-In)

**Goal:** Опциональный анонимный сбор **технических** perf-метрик с on-prem инсталляций для улучшения Go-core, gnet и eBPF — **выключен по умолчанию**, cold path only, без PII и коммерческих данных. Отдельно от license heartbeat (`ESPX_LICENSE_TELEMETRY`); объединяется на этапе anonymizer.

**Sources:** `GUIDE_COMPLIANCE.md` §8, `docs/LICENSING.md` §8 (heartbeat allowlist), `internal/metrics/collectors.go`, `deploy/monitoring/prometheus.yaml`, M3 `LIC-PRIVACY`.

**Status:** backlog — последний в roadmap; не блокирует M1–M9. Только после M5 (red-zone policy) и желательно после M9 (`telemetry_enabled` в `install.yaml`).

**Depends on:** M5 (forbidden-field policy), M3 optional (license `deployment_id` hash salt). **Soft:** M9 installer exposes toggle.

### 10.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | **GUIDE_COMPLIANCE.md** §8 (primary); STYLE R1 `internal/telemetry` flat; CHAOS **not required** if zero `/track` impact (R10 #9 defer until enabled) |
| **Binaries** | `cmd/management` worker only; `cmd/telemetry-ingest` vendor-side; **veto** tracker scrape on hot loop |
| **Packages** | `internal/telemetry/{collector,anonymize,denylist}.go`; worker in `management/telemetry_worker.go` |
| **Patterns** | Opt-in `ESPX_VENDOR_TELEMETRY=0`; local scrape 127.0.0.1; anonymize before POST; red-zone abort entire batch; separate from `ESPX_LICENSE_TELEMETRY` |
| **SLA** | Hot path impact **0**; scrape budget < 5 s/cycle; payload ≤ 256 KiB; air-gap drop on failure |
| **Metrics** | `telemetry_export_aborted_total{reason}`; allowlisted rollups only — no `campaign_id`/IP/money labels |
| **Code** | TEL-RED unit + fuzz tests; `check_compliance.sh` for secrets in payload |
| **Chaos R10** | **Not required** for default-off; integration test with httptest only |
| **CI gates** | `go test ./internal/telemetry/...`; `check_compliance.sh`; verify `ESPX_VENDOR_TELEMETRY=0` no egress |

### 10.1 Gap analysis

| Area | Implemented | Gap (M10) |
| :--- | :--- | :--- |
| **Opt-in** | `ESPX_LICENSE_TELEMETRY=0` default (license heartbeat only) | `telemetry_enabled: false` / `ESPX_VENDOR_TELEMETRY=0` for perf bundle; installer wizard copy |
| **Metrics source** | Prometheus `/metrics` on tracker (9090), processor, management; rich `ad_*` counters | No vendor export worker; no anonymizer |
| **Red zone** | LIC-PRIVACY unit test for heartbeat | Formal allowlist/denylist schema; fuzz test on payload |
| **Air-gap** | — | POST fails silently; no tracker impact |

### 10.2 Architecture rules

| Rule | Requirement | DoD |
| :--- | :--- | :--- |
| **TEL-A opt-in** | `telemetry_enabled: false` in `/etc/espx/install.yaml`; env `ESPX_VENDOR_TELEMETRY=0` default | No outbound vendor URL unless both license contract allows **and** flag true |
| **TEL-B cold path** | Worker in `cmd/management` only; interval 1h–24h (`TELEMETRY_INTERVAL`); never on gnet hot loop | Benchmark: worker tick 0 impact on `ad_http_request_duration_seconds` p99 |
| **TEL-C scrape local** | HTTP GET `127.0.0.1` Prometheus text from tracker/processor/management/edge metrics ports; **no** remote Prometheus in customer DC | Config: `TELEMETRY_SCRAPE_TARGETS` YAML list |
| **TEL-D anonymize** | Package `internal/telemetry/` (or `internal/management/telemetry/`): strip labels `campaign_id`, `customer_id`, `host`, `instance`; hostname → `host_{uuid}` persisted salt per install | Unit test: input series with forbidden labels → absent in output |
| **TEL-E transport** | Single async `POST` JSON to `ESPX_TELEMETRY_ENDPOINT` (vendor, e.g. `https://telemetry.espx.io/v1/ingest`); timeout 10s; 3 retries max; drop on failure | Integration test with httptest; air-gap = timeout, process healthy |
| **TEL-F separation** | Distinct from license heartbeat payload; may share `deployment_id` as **HMAC-SHA256(deployment_id, install_salt)** only | Document in `LICENSING.md` + `GUIDE_COMPLIANCE.md` |

**Sales line (docs only):** «Телеметрия поставляется полностью выключенной. Включите анонимные perf-метрики, чтобы помочь улучшить ядро — без передачи кампаний, IP и финансов».

### 10.3 Allowed payload (green zone)

Aggregates only — scrape + roll up before POST:

| Category | Prometheus / runtime sources (existing) | Export field examples |
| :--- | :--- | :--- |
| **Go runtime** | `runtime.ReadMemStats` in worker | `gc_pause_p99_ns`, `heap_alloc_bytes`, `goroutines` |
| **gnet /track** | `ad_http_request_duration_seconds`, `ad_gnet_*`, `ad_worker_pool_reject_total` | `track_latency_p50_us`, `track_latency_p99_us`, `gnet_active_conns` |
| **Redis Lua** | `ad_redis_lua_duration_seconds`, `ad_redis_lua_fast_path_total` | `lua_p99_ms`, `lua_fast_ratio` |
| **Processor / CH** | `ad_ch_spool_*`, `ad_processor_stream_backpressure_active`, `ad_db_write_duration_seconds` | `ch_batch_size_p50`, `stream_lag_p99`, `spool_segments` |
| **eBPF edge** | `espx_edge_*` / XDP stats from edge metrics | `xdp_drop_ratio`, `xdp_pass_total` (no source IPs) |
| **Build info** | binary version env / `version` metric | `espx_version`, `go_version`, `profile` (`single_vps`) |

No high-cardinality labels in export. Histograms → pre-computed quantiles in worker, not raw buckets.

### 10.4 Forbidden payload (red zone)

| ID | Forbidden | Enforcement |
| :--- | :--- | :--- |
| TEL-RED-01 | End-user IPs, `/24`, bot source IPs | Anonymizer drops; CI fixture test |
| TEL-RED-02 | `campaign_id`, `customer_id`, brand, domain, custom headers | Label denylist in `internal/telemetry/denylist.go` |
| TEL-RED-03 | Balances, spend, rates, invoice amounts (`balance_ledger`, budget metrics with money) | Exclude `ad_tracker_campaign_spend_*`, billing counters |
| TEL-RED-04 | Raw CH rows, SQL text, click_id, request paths with query strings | No CH/pg query in worker |
| TEL-RED-05 | DSN, Redis passwords, JWT, license file body | Static grep in `check_compliance.sh` |

Violation → worker aborts batch, logs local error, metric `telemetry_export_aborted_total{reason}`; **never** send partial red-zone data.

### 10.5 Components

| Component | Location | DoD |
| :--- | :--- | :--- |
| `TelemetryCollector` | `internal/telemetry/collector.go` | Scrape + MemStats; returns neutral struct |
| `Anonymizer` | `internal/telemetry/anonymize.go` | Host UUID map; label strip; HMAC deployment ref |
| `TelemetryReporterWorker` | `internal/management/telemetry_worker.go` | Ticker; calls collector → anonymizer → POST |
| `TelemetryBundle` DTO | godoc JSON schema | `schema/telemetry_bundle.json` for vendor ingest |
| Vendor ingest | `cmd/telemetry-ingest` (vendor deploy) | **Out of customer compose**; rate limit + auth |

Config env: `ESPX_VENDOR_TELEMETRY`, `ESPX_TELEMETRY_ENDPOINT`, `TELEMETRY_INTERVAL`, `TELEMETRY_INSTALL_SALT` (generated once at install).

### 10.6 SLA and constraints

| Metric | Value |
| :--- | :--- |
| Hot path impact | 0 — worker never runs on tracker/processor threads |
| Scrape budget | < 5s total per cycle |
| Payload size cap | ≤ 256 KiB JSON |
| Default | All off — zero egress |

### 10.7 Tests

```bash
go test ./internal/telemetry/... -short
go test ./internal/management/... -run Telemetry -short
bash scripts/ci/check_compliance.sh   # TEL-RED-* patterns
```

| Test | Criterion |
| :--- | :--- |
| Default off | `ESPX_VENDOR_TELEMETRY=0` → no HTTP egress |
| Anonymizer | Series with `campaign_id` label → export error or stripped |
| Air-gap | Unreachable endpoint → 3 retries, no panic, tracker healthy |
| Red zone | Inject fake IP label → batch aborted |

### 10.8 Milestone 10 - Completion Checklist

- [ ] `GUIDE_COMPLIANCE.md` §8 + `docs/LICENSING.md` cross-link
- [ ] `telemetry_enabled: false` default in `install.yaml` / `.env.example`
- [ ] `internal/telemetry` collector + anonymizer + denylist
- [ ] `TelemetryReporterWorker` in management
- [ ] Allowlist metrics documented; red-zone tests green
- [ ] `cmd/telemetry-ingest` stub or vendor runbook (outside customer bundle)
- [ ] Operator doc: enable/disable, air-gap behavior, sales privacy blurb
- [ ] CI: telemetry export tests + compliance grep

---

## Milestone 11 (backlog) - Botnet Interval Scoring (extend `ivt-detector`)

**Goal:** Выявление инфраструктурных бот-сетей **чистой статистикой временных рядов** (дисперсия интервалов между кликами σ→0), без device fingerprinting. Не новый бинарь `espx-anomaly-detector` — расширение существующего `cmd/ivt-detector` + `internal/ivtdetector`.

**Sources:** `GUIDE_COMPLIANCE.md` §1.B (passive CH stats), M5 `CMP-EBPF-*`, `docs/ARCHITECTURE.md` (cold path), `clicks` / `impressions` CH tables (не `espx_raw_log`), `GUIDE_IDEAS_MICROSERVICES.md`, `GUIDE_STYLE_CODE.md` (R2), `GUIDE_CHAOS_RELIABILITY.md` (R10).

**Status:** backlog — после M5 (allowlist) и M6 (CH governance). Feature gate: `ivt_ml_detector` / Enterprise.

**Depends on:** M5, M2 (`BlockIPWithTTL` + outbox). **Not** hot path.

### 11.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | COMPLIANCE §1.B passive CH only — **no** §2 fingerprinting; MICRO **ivt-detector 7/18** — extend, do not split; STYLE R2 `interval_variance_rule.go` |
| **Binaries** | `cmd/ivt-detector` only; block via `management` HTTP API — **no** direct BPF |
| **Packages** | `internal/ivtdetector/interval_variance_rule.go`; `chquery` when M6 done |
| **Patterns** | `Rule` interface; `SuspiciousIP` pipeline; `BlockIPWithTTL` + CMP-EBPF-02; CH tables `clicks`/`impressions` |
| **SLA** | Scan interval default 5m (`IVT_SCAN_INTERVAL_MS`); CH query bounded by `chquery` SETTINGS |
| **Metrics** | `ivt_interval_candidates_total`, `ivt_interval_blocked_total`; existing `ivt_*` ops metrics |
| **Code** | Parameterized CH SQL; no raw IP export to M10 telemetry |
| **Chaos R10** | **Not required** for read-only CH rule (R10 #3); **integration test** for block path + allowlist |
| **CI gates** | `go test ./internal/ivtdetector/...`; CH fixture test; `check_compliance.sh` (no scan deps) |

### 11.1 Gap analysis

| Proposal | eSPX today | M11 target |
| :--- | :--- | :--- |
| Отдельный `espx-anomaly-detector` | `cmd/ivt-detector` + rules (click/imp ratio, shared fingerprint cluster) | Добавить rule **IVT-INTERVAL** |
| CH scan каждые 5 мин | `IVT_SCAN_INTERVAL_MS` configurable | Default 5m; streaming CH query |
| σ(inter-arrival) ≈ 0 | Нет | `interval_variance_rule.go` |
| gRPC → management → eBPF | HTTP/gRPC `BlockIP` → outbox → Redis → `edge-bpf-sync` | Reuse; no direct BPF from ivt |

### 11.2 Detection spec (`IVT-INTERVAL`)

| ID | Logic | DoD |
| :--- | :--- | :--- |
| IVT-INT-01 | CH window 15–60m: per `ip_address`, `arraySort(groupArray(created_at))`, compute Δt between events | SQL or Go post-agg; min events ≥ N |
| IVT-INT-02 | Flag cluster if `stddevPop(Δt) < ε` AND `count() ≥ MIN_EVENTS` (timer bot) | ε, N in config |
| IVT-INT-03 | Human entropy control: exclude IPs with single event; require ≥3 intervals | FP test fixture |
| IVT-INT-04 | Score + `reason=ivt_interval_bot` → existing `SuspiciousIP` pipeline | Same as ratio rules |
| IVT-INT-05 | Auto-block only via `management` `BlockIPWithTTL` + **CMP-EBPF-02** allowlist | Integration test 8.8.8.8 never blocked |

**Compliance:** aggregate CH stats only; no Canvas/JS; bot IPs may be blocked locally (self-defense) but **не** экспортировать IP list в vendor telemetry (M10 red zone).

### 11.3 Components

| Component | Location | DoD |
| :--- | :--- | :--- |
| `intervalVarianceRule` | `internal/ivtdetector/interval_variance_rule.go` | Implements `Rule` interface |
| CH query | parameterized; `chquery` wrapper when M6 CHG done | `EXPLAIN` in `scripts/sql-explain/` |
| Config | `IVT_INTERVAL_EPSILON_SEC`, `IVT_INTERVAL_MIN_EVENTS` in `internal/config` | godoc |
| Metrics | `ivt_interval_candidates_total`, `ivt_interval_blocked_total` | Prometheus |

### 11.4 Milestone 11 - Completion Checklist

- [ ] `IVT-INTERVAL` rule registered in `ivtdetector` registry
- [ ] CH integration test with synthetic timer bot fixture
- [ ] Block path through management + allowlist gate
- [ ] `docs/ARCHITECTURE.md` cold-path diagram updated
- [ ] No new `cmd/` binary

---

## Milestone 12 (backlog) - Ledger Delta Consolidation (extend `processor`)

**Goal:** Железная ACID-точность `balance_ledger` с **батчевой консолидацией** Redis→Postgres дельт и мгновенной остановкой кампании при нулевом балансе. **Не** отдельный `espx-billing-ledger` — усиление `cmd/processor` (`SyncWorker`, `PostgresStore`, `campaign_repo`).

**Sources:** M1 H1 (single writer), `internal/ingestion/sync_worker.go`, `campaign_repo.UpdateSpend`, `balance_ledger` migrations, `docs/DATABASE.md`, `GUIDE_CHAOS_RELIABILITY.md` (R10 #10), `GUIDE_IDEAS_MICROSERVICES.md` (batch → processor), `GUIDE_STYLE_CODE.md` (R8.6).

**Status:** backlog — после M1; параллельно с M3. Не дублировать `cmd/billing` (invoices only).

**Depends on:** M1 (ledger, stream processor).

### 12.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R10 #10 **required** — budget batch + pause outbox; STYLE R8 cold in `sync_worker.go`; MICRO **monolith in processor** — veto `cmd/ledger` |
| **Binaries** | `cmd/processor` (`SyncWorker` only); **H1:** management must not `UpdateSpend` |
| **Packages** | `internal/ingestion/sync_worker.go`, `postgres_store.go`; shared `ProcessorPgGate` (SEM-P2) |
| **Patterns** | 10s in-memory rollup per campaign; single PG txn: `balance_ledger` + `current_spend` + audit; zero-balance → `PAUSE_CAMPAIGN` outbox; idempotent `sync_id` |
| **SLA** | ≤1 PG txn per campaign per 10s per shard; budget invariant ±1 micro-unit; no overspend after pause |
| **Metrics** | `ad_processor_write_acquire_wait_seconds`; recon compares Redis vs ledger; extend existing recon worker |
| **Code** | `errors.Join` on batch failures (R8.6); sorted campaign_id for deadlock avoidance |
| **Chaos R10** | **Required** — `AssertBudgetInvariant` under concurrent SyncWorker; existing `ledger_drift_chaos_test` green |
| **CI gates** | `go test ./internal/ingestion/... -run Sync -race`; `tests/integration/budget_test.go`; full chaos suite |

### 12.1 Gap analysis

| Proposal | eSPX today | M12 target |
| :--- | :--- | :--- |
| `campaign:spend_stream` per campaign | Единый `ad:events:*` + Lua budget debit + processor `PostgresStore` | Оставить M1 path; **не** второй spend stream |
| Batch 10s aggregate UPDATE | `SyncWorker` periodic flush per shard; `UpdateSpend` idempotent by `sync_id` | **LEDGER-BATCH:** in-memory rollup per campaign 10s window |
| `espx_campaign_balances` table | `campaigns.current_spend` + `balance_ledger` rows | Ledger append-only; spend via ledger lines + campaign mirror |
| Zero balance → stop Redis | Partial via budget Lua / pause outbox | **LEDGER-ZERO:** txn detects limit → outbox `PAUSE_CAMPAIGN` |

### 12.2 Spec (`LEDGER-*`)

| ID | Requirement | DoD |
| :--- | :--- | :--- |
| LEDGER-01 | `SyncWorker` aggregates Redis deltas per `(campaign_id, window)` before single `UpdateSpend` | ≤1 PG txn per campaign per 10s per shard |
| LEDGER-02 | Same txn: `INSERT balance_ledger` + update `campaigns.current_spend` + optional `financial_audit` row | Idempotent `sync_id`; ±1 micro-unit invariant tests |
| LEDGER-03 | On `current_spend >= budget_limit` or customer balance 0: enqueue `PAUSE_CAMPAIGN` / budget key delete via outbox | E2E: zero overspend after pause |
| LEDGER-04 | `ProcessorPgGate` shared with stream consumer (SEM-P2) | Chaos test unchanged green |
| LEDGER-05 | Recon worker compares Redis spend keys vs ledger sum | Existing recon extended |

**Anti-pattern (forbidden):** second microservice writing ledger; management `UpdateSpend` on hot path (H1 test).

### 12.3 Milestone 12 - Completion Checklist

- [ ] Batch aggregator in `sync_worker.go` with configurable `SYNC_BATCH_WINDOW_MS`
- [ ] Ledger + audit insert in one txn
- [ ] Zero-balance → pause outbox integration test
- [ ] `AssertBudgetInvariant` on batched path
- [ ] Document in `docs/DATABASE.md` (no `espx-billing-ledger` service)

---

## Milestone 13 (backlog) - ClickHouse Lifecycle & Compaction (extend M6)

**Goal:** Автоматический lifecycle CH: мониторинг диска, aging codec, `DROP PARTITION` — без ручного DevOps. **Не** отдельный `espx-data-purger` — расширение M6 `CHPartitionJanitor` + опционально `cmd/log-compactor` cold tier.

**Sources:** M6 `CHJ-*`, `deploy/clickhouse/init.sql` (monthly `PARTITION BY toYYYYMM`), `internal/logcompactor/`, `system.parts`, `GUIDE_IDEAS_MICROSERVICES.md`, `GUIDE_CHAOS_RELIABILITY.md` (R10 defer).

**Status:** backlog — **после M6** (базовый janitor). Параллельно M9 disk preflight.

**Depends on:** M6 `CHJ-WORK` baseline.

### 13.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | Extends M6 CHJ-*; MICRO **processor worker** — no `cmd/data-purger`; CHAOS optional disk-pressure game day (manual) |
| **Binaries** | `cmd/processor` (`CHPartitionJanitor` + advanced jobs); optional `cmd/log-compactor` for warm tier |
| **Packages** | Worker next to `partition_manager.go`; ops API in `adminapi/ops` |
| **Patterns** | Off-peak daily tick; idempotent `DROP PARTITION` / codec `ALTER`; emergency drop behind `CH_EMERGENCY_DROP` + audit |
| **SLA** | Janitor off-peak; no processor insert p99 regression; disk alert at 85%, emergency at 90% |
| **Metrics** | `ad_ch_disk_used_ratio`; `ad_ch_partitions_dropped_total`; extends M6 CHJ-MET |
| **Code** | `chquery` for `system.parts`; cold path only |
| **Chaos R10** | **Defer** unless emergency drop affects ack path (R10 #9); integration test on testcontainers CH |
| **CI gates** | `go test ./internal/database/... -run CHJ`; M6 CHJ baseline complete first |

### 13.1 Gap analysis

| Proposal | eSPX today | M13 target |
| :--- | :--- | :--- |
| Daily cron `DROP PARTITION` | M6 backlog: `CHPartitionJanitor` in processor | Implement + env retention |
| `PARTITION BY day` | **Monthly** `toYYYYMM` in `init.sql` | Document; optional migration to daily for raw tier only |
| ZSTD(1)→ZSTD(10) recompress | TTL only in DDL | **CHJ-COMPACT:** `ALTER TABLE … MODIFY COLUMN … CODEC` job |
| `system.parts` + disk free | — | **CHJ-DISK:** alert + emergency drop oldest partition |

### 13.2 Spec (`CHJ-ADV-*`)

| ID | Requirement | DoD |
| :--- | :--- | :--- |
| CHJ-ADV-01 | Worker in `cmd/processor` or `cmd/ch-lifecycle` (single binary preferred: processor) | Daily tick off-peak |
| CHJ-ADV-02 | Query `system.parts` + host disk; metric `ad_ch_disk_used_ratio` | Prometheus alert >85% |
| CHJ-ADV-03 | Partitions age > `CH_RECOMPRESS_AFTER_DAYS` → background codec upgrade | Idempotent; no ingest block |
| CHJ-ADV-04 | Partitions age > `CH_RAW_RETENTION_DAYS` → `ALTER TABLE … DROP PARTITION` | Matches contract/env |
| CHJ-ADV-05 | Emergency: if disk >90%, drop oldest month after operator audit log | Feature flag `CH_EMERGENCY_DROP` |

Tables: `impressions`, `clicks`, `conversions`, `fraud_events` — not fictional `espx_raw_log`.

### 13.3 Milestone 13 - Completion Checklist

- [ ] M6 `CHJ-*` complete or merged into M13
- [ ] Recompress + drop jobs with integration test CH
- [ ] `GET /api/v1/ops/clickhouse/retention` shows last run + disk headroom
- [ ] Runbook in `docs/DATABASE.md` Part II

---

## Milestone 14 (backlog) - PII Anonymization Pipeline (GDPR CH)

**Goal:** В CH на долгом хранении — только необратимые хэши IP/UA (rolling daily salt); сырой PII — короткий TTL в Redis для fraud hot path. **Не** отдельный `espx-privacy-anonymizer` микросервис — pipeline в `cmd/processor` (`ClickHouseStore`) + `internal/privacy/`.

**Sources:** `GUIDE_COMPLIANCE.md` §9, `management` `ErasureWorker`, `ConsentFilter`, `deploy/clickhouse/init.sql` (`ip_address String` today = gap), `GUIDE_STYLE_CODE.md` (R1), `GUIDE_CHAOS_RELIABILITY.md`.

**Status:** backlog — после M5 + M6. Требует миграции CH schema (`ip_hash`, drop raw `ip_address` after N days).

**Depends on:** M5 (policy), M12 optional (ledger unchanged), ivt rules must use hash or short window raw in CH.

### 14.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | **GUIDE_COMPLIANCE.md** §9; STYLE R1 `internal/privacy` flat; MICRO **processor batch transform** — no `cmd/privacy-anonymizer` |
| **Binaries** | `cmd/processor` (`ClickHouseStore` hash on insert); `cmd/management` (`ErasureWorker`, salt rotation) |
| **Packages** | `internal/privacy/hash.go`; CH migration `ip_hash`/`ua_hash` FixedString(64) |
| **Patterns** | Rolling daily salt in PG; hash **only** in processor batch (not gnet); IVT M11 groups by `ip_hash` post-cutover; erasure = salt bump + CH mutation |
| **SLA** | Hot path unchanged; raw PII not in CH > `CH_PII_RAW_HOURS` (default 24h) |
| **Metrics** | `privacy_salt_rotation_timestamp`; erasure job progress gauge |
| **Code** | `HashPII` deterministic unit tests; no reversible export |
| **Chaos R10** | **Required** if salt rotation concurrent with inserts — integration test idempotent batches |
| **CI gates** | `go test ./internal/privacy/...`; CH migration test; update M11 queries; `check_compliance.sh` |

### 14.1 Gap analysis

| Proposal | eSPX today | M14 target |
| :--- | :--- | :--- |
| Отдельный anonymizer service | Processor writes raw `ip_address` to CH | Transform at CH batch insert |
| Rolling salt 24h | Erasure worker deletes user keys | `PrivacySaltRotator` in processor/management |
| SHA256(IP+salt) | — | `ip_hash FixedString(64)` column |
| Fraud needs raw IP | Redis fcap/dedup keys; short-lived | Raw IP **не** в CH > `CH_PII_RAW_HOURS` (e.g. 24h) |

### 14.2 Spec (`PII-*`)

| ID | Requirement | DoD |
| :--- | :--- | :--- |
| PII-01 | `internal/privacy/hash.go`: `HashPII(value, daySalt)` SHA-256 hex | Unit tests deterministic per day |
| PII-02 | Salt rotation daily; stored in PG `privacy_salt_current` or secure env | Management rotates; processor reads snapshot |
| PII-03 | `ClickHouseStore`: write `ip_hash`, `ua_hash`; raw columns nullable TTL 24h OR omit raw entirely | Migration goose/chmigrate |
| PII-04 | IVT CH rules updated to group by `ip_hash` after cutover | IVT-INTERVAL (M11) compatible |
| PII-05 | `ErasureWorker` + `POST /privacy/erasure` invalidate hash linkage via salt bump + CH mutation job | GDPR erasure path documented |
| PII-06 | Unique user analytics on `ip_hash` in admin reports (M3) | JOIN docs in `MANAGEMENT.md` |

**Hot path:** tracker unchanged; no per-request hash on gnet (hash in processor batch only — cold path).

### 14.3 Milestone 14 - Completion Checklist

- [ ] CH migration: `ip_hash` / `ua_hash` columns
- [ ] `internal/privacy` package + processor integration
- [ ] Salt rotator worker
- [ ] IVT + fraud-scorer queries use hash columns
- [ ] `GUIDE_COMPLIANCE.md` §9 PII pipeline
- [ ] No standalone `cmd/privacy-anonymizer`

---

## Execution Order and Dependencies

Each milestone closes only when its **Standards envelope (§Mx.0)**, DoD tables, and §0.7 applicable CI gates are green.

```text
Milestone 1 (architectural remediation, single-site)
    |
    v
Milestone 2 (invoicing + server-side admin API, cold path)
    |
    v
Milestone 4 (package layout: adminapi facets)  <-- IN PROGRESS
    |
    v
Milestone 4 (package layout: adminapi facets, R1b)
    |
    v
Milestone 5 (regulatory compliance: Art. 361 UK, passive telemetry, eBPF allowlist)
    |
    v
Milestone 6 (Day-2 ops: hot reload, health, CH janitor, analytics governance)
    |
    v
Milestone 7 (multi-region)
    |
    v
Milestone 8 (crypto gateway)

Milestone 9 (backlog) - CLI installer
    parallel after M1; `multi_region` after M7; `edge_xdp` + compliance audit after M5; `license` after M3
    |
    v
Milestone 10 (backlog) - vendor perf telemetry (opt-in, anonymized)
    after M5 red-zone policy; `telemetry_enabled` in M9 install.yaml

Milestones 11-14 (backlog) - cold-path extensions (no new microservice names)
    M11 ivt interval scoring → extend cmd/ivt-detector (after M5+M6)
    M12 ledger batch sync → extend cmd/processor SyncWorker (after M1)
    M13 CH compaction → extend M6 CHJ in processor (after M6)
    M14 PII hash pipeline → extend processor ClickHouseStore (after M5, M11)
```

| Dependency | Reason | Primary guides |
| :--- | :--- | :--- |
| 2 after 1 | Invoice and ops API require stable ledger/outbox (H1, D0, G1) | CHAOS, STYLE R1b |
| **3 after 2** | **Subscription meters, invoice overage, adminapi foundation (M2)** | LICENSING, SUBSCRIPTIONS, STYLE R1b |
| **4 after 3** (parallel with M3 tail) | **Reports/dashboards M3 land in adminapi facets** | STYLE R1b |
| **5 after 4** (parallel with M3 tail) | **Immutable allowlist before prod XDP / ML auto-block** | COMPLIANCE |
| 6 after 4+5 | CHG/freshness in `adminapi/reports` + `database/chquery`; ops probes | CHAOS, STYLE |
| 7 after 6 | Enterprise `multi_region` gate; regional control plane | MULTI_REGION, CHAOS Kong manual |
| 8 after 7 | Crypto credits use global ledger and regional isolation | MICRO payment, CHAOS |
| 9 (backlog) | Does not block 1-8; `multi_region` install flag requires M7; XDP install requires M5 | STYLE R4, COMPLIANCE |
| 10 (backlog) | Vendor telemetry; M5 TEL-RED; opt-in in M9 `install.yaml` | COMPLIANCE §8 |
| **11 (backlog)** | **IVT-INTERVAL rule; auto-block via M5 allowlist** | COMPLIANCE §1.B, MICRO |
| **12 (backlog)** | **Ledger 10s batch + zero-balance pause; processor only** | CHAOS R10 #10, M1 H1 |
| **13 (backlog)** | **CH recompress + emergency drop; extends M6 CHJ** | CHAOS optional |
| **14 (backlog)** | **`ip_hash` in CH; after M11 queries migrated** | COMPLIANCE §9 |
| 9 vs K8s | `k8s_k3s` profile optional; default `single_vps` | — |
| 3 / vendor | `cmd/license-server`, `cmd/telemetry-ingest` at vendor only | MICRO |
| 3 / M7 | `multi_region` in Enterprise subscription and license JWT | SUBSCRIPTIONS |

M1–M3 — core. M4–M6 — ops/compliance/layout. M7–M8 — scale/payments. M9–M10 — install/telemetry. **M11–M14** — fraud stats, ledger, CH lifecycle, GDPR (**extend existing binaries**, veto new `espx-*` splits per `GUIDE_IDEAS_MICROSERVICES.md` Step 0).

---

## Documentation (complete before Milestone 1)

- [x] `docs/ARCHITECTURE.md` - topology and flows
- [x] `docs/REMEDIATION.md` - architectural remediation plan
- [x] `docs/DATABASE.md` - idempotency and durability
- [x] `docs/MULTI_REGION.md` - target regional topology
- [x] `docs/CRYPTO_GATEWAY.md` - crypto payment integration
- [x] `docs/MANAGEMENT.md` - admin API, billing scope, UX roadmap (canonical)
- [x] `docs/SUBSCRIPTIONS.md` - Basic / Pro / Enterprise tenant plans
- [x] `docs/LICENSING.md` - on-prem license server and JWT
- [x] `docs/proposals/ESPX-LP-2026-V1.md` - optional hybrid volume / PU pricing proposal
- [x] `GUIDE_COMPLIANCE.md` - Art. 361 UK guardrails, passive telemetry, eBPF audit (M5)
- [x] `GUIDE_STYLE_CODE.md`, `GUIDE_CHAOS_RELIABILITY.md`, `GUIDE_IDEAS_MICROSERVICES.md`
