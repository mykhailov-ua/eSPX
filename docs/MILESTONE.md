# Milestones

Roadmap M15+ derived from open gaps in [BACKLOG.md](./BACKLOG.md), residual risks in [M14_SHARD0_TECHNICAL_REPORT.md](./M14_SHARD0_TECHNICAL_REPORT.md), RTB backlog R21–R31 ([RTB.md](./RTB.md)), chaos catalog ([CHAOS.md](./CHAOS.md)), and cold-path debt ([DATA.md](./DATA.md) Part IV–V).

**Ordering:** platform milestones **hardest → easiest**; **UI/UX/frontend last** (M18 → M26-UI → M27). HTMX/Templ deprecated.

Shipped baseline: **M1–M14** — see [CAPABILITIES.md](./CAPABILITIES.md).

---

## Shipped baseline (M1–M14)

| Milestone | Theme | Status |
| :--- | :--- | :--- |
| M1 | Slot migration, `CampaignRedisKeyCatalog`, migration fence | Complete |
| M2 | Elastic triplets, `ShardOrchestrator`, TCP HMAC cutover | Complete |
| M3 | Budget reconciliation (`ReconWorker`, atomic Lua snapshot) | Complete |
| M4 | Multi-region dedup (`pkg/dedupkey`, D3 v2) | Complete |
| M5 | HTTP/1–3 ingress (edge terminate → H1.1 tracker) | Complete |
| M6 | Hot-path and broker test coverage | Complete |
| M7 | RTB exchange surface R1–R20 | Complete |
| M8 | Local budget quanta + broker `budget-deltas` | Complete |
| M9 | Lua/Redis RTT consolidation, sticky eval pins | Complete |
| M10 | XDP edge L4 tiers A–C (B2 spoof-block deferred) | Complete |
| M11 | Adaptive fraud telemetry aggregation | Complete |
| M12 | OpenRTB ingress default, parse consolidation | Complete |
| M13 | Runtime tuning (GOGC/GOMEMLIMIT), installer rollback | Complete |
| M14 | Shard-0 survival, wire hardening, quanta lifecycle | Complete |

**Closed gaps (M1–M14):** GAP-SHARD-04, GAP-SHARD-05, GAP-SHARD-06, GAP-HOT-03, GAP-WIRE-03, GAP-WIRE-04.

---

## Engineering standards

Applies to every M15+ deliverable unless a milestone explicitly marks an item out of scope. Detail: [GO.md](./GO.md), [STYLE.md](./STYLE.md), [CHAOS.md](./CHAOS.md).

### Definition of Done (DoD)

A milestone item (e.g. M16-02) is **done** only when all applicable rows below are satisfied.

#### Per deliverable

| # | Criterion | Evidence |
| :--- | :--- | :--- |
| D1 | **Gap closed** | Gap ID removed or marked closed in [BACKLOG.md](./BACKLOG.md); milestone acceptance bullets met |
| D2 | **Code merged** | PR reviewed; no known P0/P1 regressions on touched paths |
| D3 | **Tests** | New behavior covered per [Testing standards](#testing-standards) below |
| D4 | **Docs** | Env flags in `.env.example`; runbook/runbook delta in [DEVELOPMENT.md](./DEVELOPMENT.md) if ops behavior changed |
| D5 | **Metrics** | New counters use pre-bound labels on hot path; cold path uses bounded label sets |
| D6 | **Financial safety** | Budget paths: `AssertBudgetInvariant` in tests; no new TOCTOU debit outside Lua/local quanta |
| D7 | **Config mutations** | Hot-path side effects only via PG txn + `outbox_events` — never direct HTTP → Redis |

#### Per pull request (merge gate)

| # | Gate | Command / check |
| :--- | :--- | :--- |
| P1 | Unit / integration | `go test ./... -short` |
| P2 | Hot-path alloc | `make test-alloc-gate` when `internal/ingestion`, `internal/rtb`, or Lua touched |
| P3 | Compliance | `bash scripts/ci/check_compliance.sh` when edge/BPF/Lua/compliance touched |
| P4 | Chaos | `./scripts/chaos-drills/test_chaos.sh` when write path, Redis, outbox, or shard routing touched |
| P5 | Perf regression | `scripts/perf-gate/` or documented bench delta when hot path latency changed |
| P6 | BPF (if applicable) | `go test ./internal/edge/bpf/...` on privileged runner |

#### Per milestone (closure)

| # | Criterion |
| :--- | :--- |
| M1 | All deliverable IDs in the milestone table marked shipped in [CAPABILITIES.md](./CAPABILITIES.md) or this file |
| M2 | Gap → milestone map updated; no open gaps remain for that milestone theme |
| M3 | Staging sign-off recorded for user-facing milestones (M17 RTB live, M26-UI Mission Control) |
| M4 | At least one `chaos_proof fault=...` line or extended chaos test for resilience milestones (M19, M24) |

**Explicit exclusions (not required for DoD):**

- GAP-PROD-03 (OpenAPI) — deferred by design
- M25-05 (XDP spoof block) — blocked on legal review
- M19-06, M12-05 (registry binary replica) — optional; DoD only if milestone owner enables the item

---

### Testing standards

#### Test pyramid

| Layer | Scope | When required |
| :--- | :--- | :--- |
| **Unit** | Pure logic, classifiers, parsers, mappers | Every new function with branching business rules |
| **Table-driven** | Filter rejects, OpenRTB edges, API validation, `filterRejectKind` | All new reject reasons and admin validation paths |
| **Integration** | Real Postgres / Redis (`testcontainers-go`) | Outbox, Lua `EVALSHA`, sync worker, payment settlement |
| **Chaos** | Fault injection under compose or testcontainers | New write paths, shard migration, registry, budget debit |
| **E2E** | `tests/e2e/` full stack | Cross-service flows (track → processor → ledger) |
| **Benchmark** | `-benchmem`, 0 allocs/op on hot path | Any change to parse, filter, auction, Lua wire path |

#### Hot-path requirements

| Rule | Detail |
| :--- | :--- |
| **0 allocs/op** | `make test-alloc-gate` must pass for touched hot packages; register new benches in `Makefile` / `perf_gate_bench.sh` |
| **Concurrency** | Race-sensitive code: ≥ 20 goroutines in stress tests ([CHAOS.md](./CHAOS.md) R5) |
| **Real Redis** | Lua tier tests use testcontainer Redis, not mocks (`BenchmarkLuaScript_*`, `TestChaos_LUA*`) |
| **BCE / asm** | Indexed buffer access: early length abort; spot-check `objdump` on changed inner loops |
| **Idempotency** | Duplicate click/event retries must not double-debit; assert in integration + chaos |
| **Filter matrix** | New `filterRejectKind` → handler test + metric label + `filterRejectSpecs` row |

#### Cold-path requirements

| Rule | Detail |
| :--- | :--- |
| **Real databases** | sqlc queries against Postgres testcontainer or migrated test DB; no mock-only proof for financial writes |
| **Outbox** | Mutation tests assert `outbox_events` row + handler side effect; rollback leaves no Redis write |
| **HTTP handlers** | `httptest` per route: RBAC denial, 4xx validation, 5xx mapping via `mapServiceError` |
| **Workers** | Table-driven retry / DLQ / `SKIP LOCKED` contention cases |
| **CH reads** | Admin/report tests mock or testcontainer CH; queries must go through `CHQuery` when M15-05 pattern applies |

#### Chaos and steady state ([CHAOS.md](./CHAOS.md))

Before merging resilience or financial features:

1. **Hypothesis** — steady state defined (RPS, p99 < 80 ms, budget drift ±1 micro-unit).
2. **Abort criteria** — experiment stops if control cohort p99 > 80 ms for 30 s or invariant violation.
3. **Proof** — log `chaos_proof fault=<name> ...` or extend `TestChaos_*` with deterministic assertions.
4. **No mock-only resilience** — unit mocks alone do not satisfy DoD for shard/outbox/stream paths (R2).

#### Commands by change type

```bash
# Default PR
go test ./... -short

# Hot path (ingestion, rtb, Lua)
make test-alloc-gate
go test ./internal/ingestion/... -run 'Filter|Lua|Parse|Track' -short
go test ./internal/rtb/... -short

# Financial / outbox / sharding
go test ./internal/management/... -short
go test ./internal/ingestion/... -run 'TestChaos_' -timeout 15m   # Docker
./scripts/chaos-drills/test_chaos.sh

# Admin API (M18)
go test ./internal/adminapi/... -short

# E2E
go test ./tests/e2e/... -count=1

# Edge BPF
go test ./internal/edge/bpf/... -count=1   # privileged: CAP_BPF + memlock
```

#### Coverage expectations (not line-% targets)

| Area | Minimum bar |
| :--- | :--- |
| New Lua script | Happy + budget exhausted + fence + idempotency replay; bench with real Redis |
| New outbox type | Handler unit test + integration publish → Redis key assertion |
| New admin route | Auth matrix + happy path + service error mapping |
| RTB catalog change | `BenchmarkAuction` 0 allocs; chaos row in RTB matrix if ranking changes |
| Shard routing | Parity test: Go `StaticSlotSharder` == edge Lua `get_shard()` |

---

### Code quality and style

Two regimes: **hot path** optimizes latency and allocation; **cold path** follows idiomatic Go and explicit error handling. Package layout: flat `internal/<service>/`, file-name modules — [STYLE.md](./STYLE.md) R1–R2.

#### Hot path — performance (`internal/ingestion`, `internal/rtb`, edge-critical Lua)

**Packages:** `/track` ingest, `FilterEngine.Check`, `processTrack`, `RunAuction`, gnet HTTP FSM, Redis `EVALSHA` wire path.

| Category | Rule |
| :--- | :--- |
| **Allocations** | 0 heap allocs/op on parse → filter → respond; CI: `make test-alloc-gate` |
| **Forbidden** | `defer` in request loops, closures, `interface{}`/`any` boxing, `sync.Map`, string `+`, `fmt.Sprintf`, dynamic Prometheus labels, `json.Marshal` per reject |
| **Strings / keys** | Pre-sized `[]byte` append, stack `[N]byte`, `unsafe.String` only while gnet frame alive; `copy()` before async |
| **Errors** | Sentinels (`ErrBudgetExhausted`); classify at boundary via `classifyFilterErr`; RTB uses `NoBidReason`, not `error` in inner loop |
| **Responses** | `filterRejectSpecs` pre-built `[]byte` bodies; no per-request JSON encode |
| **Time** | `FilterDeadlineMono` (monotonic); propagate to Redis client timeout |
| **Concurrency** | Lock-free atomics; pad contended globals (`cpu.CacheLinePad` / `_ [56]byte`); MPSC rings not channels |
| **Data layout** | SoA for RTB candidates; flat slices on cold rebuild; bitmasks over nested `if` where possible |
| **Redis** | One `EVALSHA` per filter check; hash-tagged keys on one shard; sticky eval pins for wire path (M9) |
| **Metrics** | Pre-bound at init (`metrics_prebound.go`); sampled histograms only |
| **Lua** | Non-blocking commands only; bounded script; tier B default for impressions; `routing_epoch` in ARGV |

**PR checklist (hot path):**

1. `make test-alloc-gate`
2. Bench delta attached for perf-critical changes (`-benchmem -count=3`)
3. No new `interface{}` on `/track` path
4. BCE: `if len(buf) <= i { return ErrMalformed }` before indexed access
5. New write paths: chaos proof per [CHAOS.md](./CHAOS.md) R10

Reference: `http1_fsm.go`, `unified_filter.go`, `fraud_stream_queue.go`, `auction_rank.go`.

#### Cold path — idiomatic Go (`internal/management`, `internal/adminapi`, `payment`, `billing`, `auth`, workers)

**Packages:** Admin HTTP, gRPC, outbox, reconciliation, billing, webhooks.

| Category | Rule |
| :--- | :--- |
| **Errors** | Sentinels in `errors.go`; wrap with `fmt.Errorf("context: %w", err)`; `errors.Is` / `errors.As` — never `err.Error() ==` for new code |
| **Postgres** | `pgx.ErrNoRows` → 404 only when resource missing; other PG errors → 5xx, not masked as not-found |
| **HTTP** | Single mapping: `mapServiceError` + `writeServiceError`; validation → 400; 5xx logged once at handler |
| **JSON** | `pkg/coldpath` helpers; explicit check on `Marshal`/`Unmarshal` — no `_ = json.Marshal` |
| **Transactions** | `BeginFunc` for claim+process; `FOR UPDATE` inside same txn as side effect |
| **Outbox** | PG mutation + `outbox_events` in one txn; workers `SKIP LOCKED`; at-least-once to Redis |
| **Workers** | Return `error` for retry; permanent failure → DLQ + alert; `errors.Join` for batch failures |
| **gRPC** | Map to `codes.*` at boundary; no raw `err.Error()` to client |
| **Structs** | One role per type: `db.*` row, `*DTO` for JSON, `domain.*` for hot registry — no duplicate entity layers (STYLE R3) |
| **Mapping** | `to*DTO` at I/O boundary only; `coldpath.MapSlice` for lists |
| **Logging** | `slog` with structural attrs (`campaign_id`, `event_type`); no `log.Fatal` in handlers |
| **Concurrency** | `errgroup`, semaphores (`mgmtPgSem`), `FOR UPDATE SKIP LOCKED` — idiomatic, not lock-free tricks |
| **CH admin** | All analytical queries through `CHQuery` (timeout, row limit, audit) |

**FORBIDDEN on cold path:**

- Direct HTTP handler writes to Redis for config that affects delivery
- `panic` / `log.Fatal` in request handlers
- Reflection-based mappers (`MapStruct`)
- Nested `usecase/` / `repository/` packages per table

#### Shared / boundary rules

| Boundary | Rule |
| :--- | :--- |
| `internal/ingestion` → `internal/fraudscoring` | **Forbidden** — ML scores via Redis snapshot only |
| `pkg/*` → `internal/*` | **Forbidden** — utilities stay downstream |
| Hot ↔ cold types | `domain.Campaign` built in registry from `db` rows; no JSON round-trip on ingest |
| Lua / Go parity | Edge shard pick and ingress schema must match tracker env |
| Compliance | Defensive edge only — [COMPLIANCE.md](./COMPLIANCE.md); no outbound strike to offender IPs |

#### Lua and edge (OpenResty)

| Layer | Style |
| :--- | :--- |
| Tracker Lua | Embedded `go:embed`; versioned with tracker; non-blocking Redis ops; return codes documented in script header |
| Edge Lua | Fail-closed blacklist; DFA parse before upstream; metrics via `edge-metrics.lua`; no campaign budget logic at edge |

#### Frontend (`frontend/`, M26-UI, M27)

| Category | Rule |
| :--- | :--- |
| **Bundle** | JS+CSS Gzip ≤ 200 KB; zero CDN; fonts/icons in bundle |
| **Components** | Shadcn UI + Tremor/Recharts only — no custom primitives |
| **Data** | TanStack Query v5; DTOs match `internal/adminapi`; server-side pagination/filter |
| **Lists** | `@tanstack/react-virtual` for logs/audit; ~20 DOM rows |
| **Input** | `useDebounce(200ms)` on search/filter fields |
| **Real-time** | WS/SSE buffered in `useRef`; flush to state @ 250ms via `requestAnimationFrame` |
| **Errors** | `503 registry_stale` / `503 shard_unavailable` → banner; no uncaught runtime errors |
| **Theme** | Dark Mode Only; Tailwind v4 tokens; no light-mode CSS |
| **Forbidden** | Heavy client-side sort/filter/aggregate; `fetch` without Query; external script tags |

---

### SLA targets (production)

Baseline from [.cursorrules](../.cursorrules). Milestone-specific SLA rows override only where noted.

| Surface | Metric | Target | Hard limit / abort |
| :--- | :--- | :--- | :--- |
| **Tracker handler** | `ad_http_request_duration_seconds` p95 | < 50 ms | p99 < 80 ms; ceiling 100 ms |
| **Tracker handler** | p99 under load | < 80 ms | Load-test abort if control cohort p99 > 80 ms for 30 s |
| **Filter deadline** | `FILTER_TIMEOUT_MS` | ≤ 100 ms (prod) | 5000 ms dev default |
| **Redis unified-filter Lua** | p99 per shard | < 10 ms | Blocks shard while running |
| **Geo filter** | p99 (sampled) | < 10 µs | Fail-open on miss |
| **RTB `RunAuction`** | p99 | < 15 µs | p99 candidates scanned < 500 |
| **Fraud boost snapshot** | per `FilterEngine.Check` | < 500 µs incremental | `BenchmarkFilterFraudBoost` ~90 ns, 0 allocs/op |
| **`GetShard` (StaticSlot)** | latency | ~5.6 ns | 0 allocs/op |
| **Budget invariant** | `current_spend ≤ budget_limit` | ±1 micro-unit | `AssertBudgetInvariant` in tests |
| **Admin report API** | p95 | < 500 ms | `stale=true` when CH lag > 5 min |
| **IAM API** | p95 | < 200 ms | Cold path only |
| **Mission Control SPA** | TTI (first paint) | < 2 s LAN | Bundle Gzip ≤ 200 KB |
| **Mission Control WS** | metric flush interval | 250 ms | No main-thread jank > 16 ms/frame |

**Per-milestone SLA:** each milestone below lists applicable rows and any deltas.

---

### Per-milestone checklist legend

Every M15+ milestone ends with five blocks. Global rules: [DoD](#definition-of-done-dod) · [Testing](#testing-standards) · [Code style](#code-quality-and-style) · [SLA](#sla-targets-production).

| Block | Meaning |
| :--- | :--- |
| **DoD checklist** | Closure criteria beyond global D1–D7 |
| **Testing checklist** | Required test types and commands |
| **Code style checklist** | Hot / cold / frontend regime for this milestone |
| **Patterns** | Applicable design patterns (flat package) |
| **SLA checklist** | Metrics and abort criteria to verify before merge |

---

## Roadmap overview

M15+ split into two tracks:

1. **Platform & backend** — infrastructure, hot path, data, payments, IAM enforcement. Ordered **hardest → easiest** below.
2. **UI / UX / frontend** — separate section at the end. Starts only after platform APIs and RBAC backend are stable.

### Platform track — complexity order (hardest first)

| # | ID | Theme | Complexity | Why | Depends on |
| :---: | :--- | :--- | :--- | :--- | :--- |
| 1 | **M22** | Multi-region & Postgres DR | Very high | Cross-region budget mirror, game days, automated failover | M4, M17-05 |
| 2 | **M17** | RTB platform & ops | Very high | Multi-node budget, cohort rollout, simulate endpoint | M16 |
| 3 | **M16** | RTB inventory & pre-auction | High | Hot-path auction SoA, placement/creative indexes, 0 allocs | M7 |
| 4 | **M19** | Shard-0 control-plane | High | Distributed key fan-out, auth/consent locality, broker paths | M14, M15 |
| 5 | **M20** | PII & compliance | High | CH hash pipeline, dual-read migration, erasure audit | CH schema |
| 6 | **M24** | Chaos & reliability | Medium–high | 8 scenario families, CI proofs, orchestrator game days | M15, M9 |
| 7 | **M21** | Payments expansion | Medium | Financial idempotency, crypto webhooks, txn races | `payment` |
| 8 | **M15** | Production hardening | Medium | Broker compose, CHQuery routing, ops metrics — broad but bounded | M8, M14 |
| 9 | **M26** | RBAC & authorization (backend) | Medium | DB roles, policy engine, `/api/v1/iam/*`, route matrix | `auth` |
| 10 | **M23** | Engineering debt | Low–medium | Cold-path refactors, installer, logger/spool tuning | — |
| 11 | **M25** | XDP tier D | Low | Lab/BPF CI; optional legal-blocked B2 | M10 |

### UI / UX track — after platform APIs (easiest frontend last)

| # | ID | Theme | Layer | Depends on |
| :---: | :--- | :--- | :--- | :--- |
| 1 | **M18** | Dashboard & report APIs | Backend JSON for UI | M3, CH MVs, M26 Phase A |
| 2 | **M26-UI** | Mission Control Dashboard (React 19) | Frontend SPA + `go:embed` | M18, M26 backend |
| 3 | **M27** | Supervisor UI & agentic ops | Frontend real-time + MCP | M18, M26-UI |

**Rule:** No React screens until M26 Phase A (permission catalog + DB roles) and M18 report routes ship. HTMX/Templ admin UI is **deprecated** — replaced by M26-UI.

---

## Platform milestones (hardest → easiest)

## M22 — Multi-region and Postgres DR

**Goal:** Productize failover drills and automated DR where runbooks exist today.

**Priority:** P3 · **Depends on:** M4 (D3 dedup), `RegionOutboxRelay`

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M22-01 | Automated multi-region game day script (region loss + outbox relay) | GAP-GEO-01 |
| M22-02 | Chaos Kong scenario: documented manual drill + `chaos_proof` template | CHAOS.md |
| M22-03 | Postgres DR: automated promote script or operator CLI (`espx-install dr-promote`) | GAP-GEO-02 |
| M22-04 | Cross-region budget mirror validation (extends M17-05 / DATA.md Part VII) | GAP-RTB-12 / R30 |
| M22-05 | `outbox_region_delivery` lag alerts + DLQ replay runbook | DATA.md Part VII |

### Acceptance

- Game day script runs in CI staging (non-destructive subset) or quarterly manual sign-off
- DR runbook executable without hand-editing connection strings
- D3 dedup chaos proofs pass under region relay redelivery

### SLA checklist

- [ ] Region relay lag alert fires before `outbox_region_delivery` exceeds configured threshold
- [ ] Cross-region budget mirror drift ≤ ±1 micro-unit under game-day redelivery
- [ ] D3 dedup: no double-settlement under region failover replay
- [ ] Postgres promote CLI completes in documented RTO window

### DoD checklist

- [ ] GAP-GEO-01, GAP-GEO-02 closed in [BACKLOG.md](./BACKLOG.md)
- [ ] Game-day script + `chaos_proof` template committed under `scripts/chaos-drills/`
- [ ] `espx-install dr-promote` documented in [DEVELOPMENT.md](./DEVELOPMENT.md)
- [ ] M22-04 validates M17-05 mirror semantics in multi-region compose

### Testing checklist

- [ ] `go test ./tests/e2e/... -run Region -count=1` (or game-day subset)
- [ ] Chaos: region loss + outbox relay with `chaos_proof fault=region_relay`
- [ ] D3 dedup chaos proof under relay redelivery
- [ ] `./scripts/chaos-drills/test_chaos.sh` green after M22 proofs added

### Code style checklist

- [ ] **Cold path:** outbox relay workers use `FOR UPDATE SKIP LOCKED`; errors wrapped
- [ ] **Cold path:** DR scripts use `slog` + exit codes; no `log.Fatal` in library code
- [ ] No hot-path file moves; region mirror reads cold-reload only

### Patterns

- [ ] **Outbox relay** — at-least-once cross-region via `RegionOutboxRelay`
- [ ] **Idempotent settlement** — `pkg/dedupkey` D3 v2 on replay
- [ ] **Game-day harness** — non-destructive CI subset + quarterly manual full drill
- [ ] **Operator CLI** — `espx-install` subcommand, not ad-hoc shell scripts

---

## M17 — RTB P2: platform and ops

**Goal:** Operator tooling and gradual live rollout for programmatic lane.

**Priority:** P1 · **Depends on:** M16

| ID | Deliverable | RTB / Gap |
| :--- | :--- | :--- |
| M17-01 | CTV `gtax` / ECIDs — cold catalog tagging from OpenRTB 2.6 `content` | R26 · GAP-RTB-12 |
| M17-02 | `POST /api/v1/rtb/simulate` → `RunAuctionEval` | R27 · GAP-RTB-12 |
| M17-03 | A/B cohort rollout (`user_id` hash → shadow/live; metric `cohort`) | R28 · GAP-RTB-12 |
| M17-04 | ARTF local enrichment hooks (cold reload function pointers only) | R29 · GAP-RTB-12 |
| M17-05 | Multi-node budget consistency (region-aware mirror) | R30 · GAP-RTB-12 |
| M17-06 | Wire or remove `HybridBalancer.SelectAndShard` | R31 |

### Acceptance

- Simulate endpoint reproduces auction outcome without `/track` traffic
- Cohort metrics visible in Grafana; shadow/live diff < configured threshold before cutover
- `RTB_MODE=live` enabled on staging with M16+M17 complete
- GAP-RTB-10..12 closed in [BACKLOG.md](./BACKLOG.md)

### SLA checklist

- [ ] `RunAuction` p99 < 15 µs unchanged after M17 cold-path hooks
- [ ] Cohort shadow/live metric diff below configured threshold before live cutover
- [ ] Multi-node budget mirror drift ≤ ±1 micro-unit (M17-05)
- [ ] Tracker p99 < 80 ms with `RTB_MODE=live` on staging load test

### DoD checklist

- [ ] GAP-RTB-12 closed; R26–R31 deliverables mapped
- [ ] `POST /api/v1/rtb/simulate` returns deterministic auction result
- [ ] Cohort rollout documented in `.env.example` + [RTB.md](./RTB.md)
- [ ] Staging sign-off for `RTB_MODE=live` recorded (global DoD M3)

### Testing checklist

- [ ] `go test ./internal/rtb/... -short`; `make test-alloc-gate`
- [ ] Simulate endpoint: table-driven cases match `RunAuctionEval` output
- [ ] Cohort hash distribution test (shadow vs live partition)
- [ ] Budget mirror integration test with concurrent track + settlement
- [ ] Chaos row in RTB matrix for cohort flip

### Code style checklist

- [ ] **Hot path:** ARTF hooks = cold-reload function pointers only; no alloc in `RunAuction`
- [ ] **Cold path:** simulate endpoint uses `mapServiceError`; DTO at boundary
- [ ] **Cold path:** cohort config via PG + outbox, not direct Redis from HTTP
- [ ] `HybridBalancer.SelectAndShard` wired or removed — no dead code

### Patterns

- [ ] **CQRS-lite** — simulate = read-only `RunAuctionEval`; no budget debit
- [ ] **Feature flag** — `user_id` hash cohort; metric label `cohort=shadow|live`
- [ ] **Command + outbox** — live-mode enablement via management mutation
- [ ] **Region mirror** — extends M17-05; validated again in M22-04

---

## M16 — RTB P2: inventory and pre-auction caps

**Goal:** Unblock `RTB_MODE=live` production cutover for inventory-heavy traffic.

**Priority:** P1 · **Depends on:** M7 (R1–R20 shipped) · **Blocks:** live RTB prod

| ID | Deliverable | RTB / Gap |
| :--- | :--- | :--- |
| M16-01 | Placement / domain targeting index extension | R21 · GAP-RTB-10 |
| M16-02 | Creative-level auction (`CreativeID uint32` in SoA) | R22 · GAP-RTB-10 |
| M16-03 | Video / VAST awareness (cold catalog parse `imp.video`) | R23 · GAP-RTB-10 |
| M16-04 | Daypart bitmap pre-filter in catalog row | R24 · GAP-RTB-11 |
| M16-05 | Frequency-cap pre-check (optional Redis MGET batch before auction) | R25 · GAP-RTB-11 |

### Acceptance

- `go test ./internal/rtb/... ./internal/ingestion/... -run Rtb -short` PASS
- `BenchmarkAuction` 0 allocs/op unchanged
- Chaos: RTB matrix extended for placement/creative/daypart paths
- Admin live gate (`rtb_live_gate.go`) green on staging with M16 features enabled

### SLA checklist

- [ ] `BenchmarkAuction` 0 allocs/op; p99 < 15 µs
- [ ] p99 candidates scanned < 500 with M16 indexes enabled
- [ ] Optional Redis MGET pre-check (M16-05) p99 < 10 ms per shard batch
- [ ] Tracker handler p99 < 80 ms under RTB inventory load test

### DoD checklist

- [ ] GAP-RTB-10, GAP-RTB-11 closed
- [ ] `CreativeID uint32` in SoA; placement/daypart indexes documented
- [ ] Video/VAST cold parse does not touch hot auction path
- [ ] RTB chaos matrix rows for placement/creative/daypart

### Testing checklist

- [ ] `go test ./internal/rtb/... ./internal/ingestion/... -run Rtb -short`
- [ ] `make test-alloc-gate` — auction path unchanged alloc profile
- [ ] Table-driven: placement reject, creative mismatch, daypart bitmap, freq-cap edge
- [ ] `BenchmarkAuction` + `BenchmarkCatalog*` attached to PR if ranking changes
- [ ] Chaos: RTB matrix extended (placement/creative/daypart)

### Code style checklist

- [ ] **Hot path:** SoA layout for candidates; bitmasks for daypart pre-filter
- [ ] **Hot path:** no `interface{}`, no string `+`, no `defer` in auction loop
- [ ] **Hot path:** freq-cap MGET optional — must not block auction inner loop
- [ ] **Cold path:** VAST/catalog parse in cold reload only

### Patterns

- [ ] **SoA catalog** — `CreativeID uint32` in candidate slice; cold rebuild, hot scan
- [ ] **Bitmap pre-filter** — daypart in catalog row; reject before `RunAuction`
- [ ] **Optional Redis batch** — MGET freq-cap before auction; timeout bounded
- [ ] **Live gate** — `rtb_live_gate.go` checks M16 feature flags before prod cutover

---

## M19 — Shard-0 control-plane follow-up

**Goal:** Reduce residual SPOF after M14 mitigations; optional hardening beyond stale-serve.

**Priority:** P2 · **Depends on:** M14, M15 (broker in compose)

| ID | Deliverable | Source |
| :--- | :--- | :--- |
| M19-01 | Auth lockout keys: fan-out or local-shard read parity (M14-01 pattern) | M14 residual |
| M19-02 | Consent watch: read locality off shards 1..N | M14 residual |
| M19-03 | Brand creative notify: broker path or multi-shard publish (data already fan-out) | M14 residual |
| M19-04 | HTTP long-poll campaign updates (alternative to broker-only secondary) | M14-03 not shipped |
| M19-05 | Runbook: when to enable `ELASTIC_SHARDING_ENABLED` for shard-0-homed hot campaigns | M2 + M14-04 |
| M19-06 | Registry binary replica (`registry.saveReplica/loadReplica`) — optional fast cold start | M12-05 backlog |

### Acceptance

- Shard-0 stop < 30 s: no auth lockout false negatives on shards 1–3
- Chaos: extended `TestChaos_Shard0Outage` covers M19-01..03
- Documented triplet migration playbook for campaigns stuck on shard 0

**Note:** Non-triplet shard-0 campaigns returning `503 shard_unavailable` until Sentinel failover remains **by design**.

### SLA checklist

- [ ] Shard-0 stop < 30 s: auth lockout false-negative rate = 0 on shards 1–3
- [ ] Broker fallback path: campaign update visible < 5 s (M19-03/04)
- [ ] `503 shard_unavailable` returned within filter deadline for non-triplet shard-0 campaigns

### DoD checklist

- [ ] M19-01..03 covered in extended `TestChaos_Shard0Outage`
- [ ] Triplet migration playbook in [DEVELOPMENT.md](./DEVELOPMENT.md)
- [ ] M19-06 optional — DoD only if enabled
- [ ] `chaos_proof fault=shard0_*` lines in CI or staging log

### Testing checklist

- [ ] `go test ./tests/chaos/ -run TestChaos_Shard0Outage -timeout 15m`
- [ ] `bash scripts/chaos-drills/m14_shard0_failure.sh` with broker enabled
- [ ] Auth lockout parity test across shards 1–3 during shard-0 outage
- [ ] Consent watch locality test (M19-02)
- [ ] Brand creative notify via broker path (M19-03)

### Code style checklist

- [ ] **Hot path:** fan-out pattern matches M14-01; no new global `sync.Map`
- [ ] **Cold path:** broker/long-poll handlers use outbox or pub/sub; PG txn for config
- [ ] Registry replica (M19-06) cold-path only; no ingest dependency

### Patterns

- [ ] **Global key fan-out** — M14-01 pattern for auth lockout (M19-01)
- [ ] **Broker fallback** — campaign updates when shard-0 pub/sub degraded
- [ ] **Stale-serve** — registry `permission_epoch` / `routing_epoch` fence unchanged
- [ ] **503 contract** — `shard_unavailable` sentinel; documented triplet escape hatch

---

## M20 — Data privacy and compliance

**Goal:** PII governance in analytics; complete defensive perimeter documentation.

**Priority:** P2 · **Depends on:** CH schema stable

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M20-01 | ClickHouse `ip_address` hash pipeline + salt rotation | GAP-DATA-01 |
| M20-02 | Migration: backfill hashed IPs; dual-read window for reports | GAP-DATA-01 |
| M20-03 | Full compliance matrix: defensive vs forbidden edge behavior | GAP-CMP-01 (remainder) |
| M20-04 | Tarpit policy integration with compliance gates (`EDGE_TARPIT_*` documented per tenant tier) | GAP-CMP-01 partial |
| M20-05 | Consent erasure path audit: CH `ALTER DELETE` + tombstone TTL verification | DATA.md Part V |

### Acceptance

- No raw IP in new CH inserts after cutover date
- `scripts/ci/check_compliance.sh` covers CMP matrix rows referenced in [COMPLIANCE.md](./COMPLIANCE.md)
- IVT and fraud queries work on hashed IP with stable join keys

### SLA checklist

- [ ] Report API p95 < 500 ms with hashed-IP queries post-cutover
- [ ] Dual-read window: no report regression during backfill (M20-02)
- [ ] CH `ALTER DELETE` erasure completes within documented TTL (M20-05)

### DoD checklist

- [ ] GAP-DATA-01, GAP-CMP-01 (remainder) closed
- [ ] Salt rotation runbook in [DEVELOPMENT.md](./DEVELOPMENT.md)
- [ ] `check_compliance.sh` covers new CMP matrix rows
- [ ] Cutover date documented; no raw IP in new inserts after date

### Testing checklist

- [ ] CH migration test: hash pipeline + backfill idempotency
- [ ] Dual-read integration: reports work on old + hashed keys during window
- [ ] IVT/fraud query tests with stable hashed join keys
- [ ] `bash scripts/ci/check_compliance.sh`
- [ ] Consent erasure audit: tombstone TTL verification (M20-05)

### Code style checklist

- [ ] **Cold path:** hash salt in env/config; never log raw IP in admin paths post-cutover
- [ ] **Cold path:** CH migrations in `internal/clickhouse/migrate/` only
- [ ] Edge tarpit gates documented per tenant tier; no new hot-path PII storage

### Patterns

- [ ] **Dual-read window** — reports accept old + hashed IP during migration
- [ ] **Salt rotation** — versioned salt; re-hash job idempotent
- [ ] **Defensive edge** — [COMPLIANCE.md](./COMPLIANCE.md) matrix; tarpit ∩ compliance gates
- [ ] **Erasure path** — CH `ALTER DELETE` + PG tombstone; audit log row

---

## M24 — Chaos and reliability expansion

**Goal:** Close chaos backlog scenarios not yet in CI.

**Priority:** P2 · **Depends on:** M15 (broker), M9 (Lua tiers)

| ID | Deliverable | Source |
| :--- | :--- | :--- |
| M24-01 | Scenario I: `SCRIPT FLUSH` under traffic → NOSCRIPT recovery | CHAOS.md §Scenario I |
| M24-02 | Scenario J: clock step backward + daypart stability | CHAOS.md §Scenario J |
| M24-03 | UDP control plane matrix UDP-01..26 (subset in CI) | CHAOS.md M2 matrix |
| M24-04 | Redis LUA-01..11 chaos proofs (tier degrade, routing_epoch fence) | CHAOS.md |
| M24-05 | Shard Orchestrator SO-01..08 false-migrate guards | CHAOS.md |
| M24-06 | Outbox storm under slow pub/sub (Scenario G) — CI or staging proof | CHAOS.md |
| M24-07 | Edge blacklist staleness (Scenario H) — automated TTL proof | CHAOS.md |
| M24-08 | Elastic shard orchestrator + slot migration dual-write game day | M1-08 + M2 |

### Acceptance

- `CHAOS_MIN_PROOFS` incremented when proofs land
- `./scripts/chaos-drills/test_chaos.sh` includes M24-01, M24-02 at minimum
- Each new proof logs `chaos_proof fault=...` line per [CHAOS.md](./CHAOS.md) R7

### SLA checklist

- [ ] Steady state during chaos: control cohort p99 < 80 ms (abort if exceeded 30 s)
- [ ] Budget invariant ±1 micro-unit holds across all M24 scenarios
- [ ] NOSCRIPT recovery (M24-01) < 1 s to re-`SCRIPT LOAD` under traffic

### DoD checklist

- [ ] `CHAOS_MIN_PROOFS` incremented in CI config
- [ ] M24-01, M24-02 minimum in `test_chaos.sh`
- [ ] Each deliverable logs `chaos_proof fault=<name>`
- [ ] Hypothesis + abort criteria documented per [CHAOS.md](./CHAOS.md) R1–R2

### Testing checklist

- [ ] `./scripts/chaos-drills/test_chaos.sh` — M24-01, M24-02 required
- [ ] Scenario I: `SCRIPT FLUSH` → NOSCRIPT recovery under load
- [ ] Scenario J: clock step backward + daypart stability
- [ ] LUA-01..11 subset; SO-01..08 subset; UDP matrix subset in CI
- [ ] `go test ./internal/ingestion/... -run 'TestChaos_' -timeout 15m` for new proofs

### Code style checklist

- [ ] Chaos tests use real Redis/compose — no mock-only resilience proof
- [ ] **Hot path:** NOSCRIPT recovery reuses sticky eval pins (M9)
- [ ] Proof logging: `chaos_proof fault=...` structured line per scenario

### Patterns

- [ ] **Hypothesis / abort** — steady state defined before each experiment
- [ ] **Sticky eval pins** — NOSCRIPT recovery without per-request `SCRIPT LOAD`
- [ ] **routing_epoch fence** — Lua ARGV epoch mismatch → safe reject
- [ ] **Game day** — M24-08 dual-write orchestrator drill

---

## M21 — Payments expansion

**Goal:** Non-Stripe payment rail for self-serve top-up.

**Priority:** P2 · **Depends on:** `payment` service, `balance_ledger`

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M21-01 | `CryptoProvider` interface + webhook handler | GAP-PAY-01 |
| M21-02 | Settlement outbox integration (same claim→apply pattern as Stripe) | GAP-PAY-01 |
| M21-03 | Fix `payment/crypto_hold_worker.go` `FOR UPDATE` outside txn race | DATA.md Part V |
| M21-04 | `pg_advisory_xact_lock` inside txn for `CreatePaymentIntent` | DATA.md Part V |

### Acceptance

- Crypto webhook idempotent via `dedup_claim_confirm` or payment intent PK
- `AssertBudgetInvariant` holds under concurrent crypto + track settlement
- Stripe path regression tests unchanged

### SLA checklist

- [ ] Webhook handler p95 < 500 ms (cold path)
- [ ] `AssertBudgetInvariant` ±1 micro-unit under concurrent crypto + track
- [ ] No double-credit on webhook replay (idempotency PK)

### DoD checklist

- [ ] GAP-PAY-01 closed
- [ ] M21-03 race fixed: `FOR UPDATE` inside same txn as hold apply
- [ ] M21-04: `pg_advisory_xact_lock` inside `CreatePaymentIntent` txn
- [ ] Stripe regression suite unchanged (CI green)

### Testing checklist

- [ ] `go test ./internal/payment/... -short` — crypto + Stripe
- [ ] Webhook idempotency: duplicate POST → single ledger entry
- [ ] Concurrent crypto settlement + track: `AssertBudgetInvariant`
- [ ] `FOR UPDATE` race test (M21-03); advisory lock contention (M21-04)
- [ ] Outbox handler test: crypto settlement → balance credit

### Code style checklist

- [ ] **Cold path:** `BeginFunc` for payment txn; `FOR UPDATE` same txn as side effect
- [ ] **Cold path:** webhook uses `dedup_claim_confirm` or intent PK idempotency
- [ ] **Cold path:** `mapServiceError` at HTTP boundary; no raw PG errors to client
- [ ] Settlement via outbox — same claim→apply pattern as Stripe

### Patterns

- [ ] **Outbox settlement** — PG txn + `outbox_events` → worker applies credit
- [ ] **Idempotent webhook** — dedup key on provider event ID
- [ ] **Advisory lock** — `pg_advisory_xact_lock` inside intent creation txn
- [ ] **Provider interface** — `CryptoProvider` parallel to Stripe adapter

---

## M15 — Production hardening

**Goal:** Close operational gaps between shipped code and production-ready default stack.

**Priority:** P0 · **Depends on:** M8, M14

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M15-01 | Add `cmd/broker` to `docker-compose.yaml` + `.env.example` | GAP-ENG-02 |
| M15-02 | Document and enable `CAMPAIGN_UPDATE_BROKER_FALLBACK` in prod profile | M14 residual (broker opt-in) |
| M15-03 | `LOCAL_QUOTA_MODE` shadow → live runbook; gate on `ad_local_quota_shadow_diff_total` | M8 enablement |
| M15-04 | Unified ops dashboard: outbox DLQ + processor PEL + spool depth (extend M14-11 Grafana) | GAP-OPS-04 (remainder) |
| M15-05 | Route `service_mab.go`, `ivtdetector/analyzer.go`, `marginguard/worker.go` through `CHQuery` | GAP-OPS-03 |
| M15-06 | `mgmtPgSem` on management background workers | DATA.md SEM-P3 |

### Acceptance

- `bash scripts/chaos-drills/m14_shard0_failure.sh` passes with broker fallback enabled
- No admin CH query bypasses `CHQuery` wrapper
- Grafana single pane: outbox oldest pending, processor DLQ, fraud ring fill (existing), spool WAL depth
- `make dev-up` starts broker; M8 recon includes broker deltas in default compose

### SLA checklist

- [ ] Broker fallback: campaign update visible < 5 s after shard-0 degradation
- [ ] `CHQuery` timeout enforced on all admin CH paths (M15-05)
- [ ] Local quota shadow→live: `ad_local_quota_shadow_diff_total` within gate before live enable
- [ ] Tracker p99 < 80 ms unchanged after M15 ops changes

### DoD checklist

- [ ] GAP-ENG-02, GAP-OPS-03, GAP-OPS-04 (remainder) closed
- [ ] `cmd/broker` in default `docker-compose.yaml` + `.env.example`
- [ ] `LOCAL_QUOTA_MODE` runbook in [DEVELOPMENT.md](./DEVELOPMENT.md)
- [ ] Grafana dashboard extends M14-11 (outbox, PEL, spool, fraud ring)

### Testing checklist

- [ ] `make dev-up` — broker starts; M8 recon includes broker deltas
- [ ] `bash scripts/chaos-drills/m14_shard0_failure.sh` with broker fallback
- [ ] CHQuery wrapper test: no raw CH client in `service_mab`, ivtdetector, marginguard
- [ ] `mgmtPgSem` contention test on background workers
- [ ] `go test ./... -short`

### Code style checklist

- [ ] **Cold path:** all new CH reads through `CHQuery` (timeout, row limit, audit)
- [ ] **Cold path:** `mgmtPgSem` on workers; no unbounded goroutine fan-out
- [ ] Compose/env changes only — no hot-path edits in M15

### Patterns

- [ ] **CHQuery facade** — single entry for admin analytical queries
- [ ] **Broker opt-in** — `CAMPAIGN_UPDATE_BROKER_FALLBACK` prod profile
- [ ] **Shadow→live gate** — metric-driven quota mode promotion (M8)
- [ ] **Ops dashboard** — Grafana single pane for DLQ/PEL/spool/fraud ring

---

## M26 — RBAC & authorization (backend only)

**Goal:** Database-backed **role templates** + **custom roles**; `/api/v1/iam/*` JSON API; `Authorize()` policy engine; delivery API unification under scoped permissions — flat `management` / `adminapi`, no CA/DDD. **No frontend** — UI in [M26-UI](#m26-ui--mission-control-dashboard-react-19).

**Priority:** P1 · **Depends on:** `auth` users table · **Blocks:** M18, M26-UI

### Current state (eSPX)

#### Roles (`internal/management/rbac.go`)

| Code | Name | Permissions |
| :--- | :--- | :--- |
| `A` | Admin | All 16 permissions incl. `settings:*`, `blacklist:*`, `shards:*`, `users:write` |
| `M` | Manager | Customers + campaigns + brands + `audit:read` — **no** settings/blacklist/shards |
| `U` | User (tenant) | `campaigns:*`, `brands:*`, `customers:read` — scoped to `customer_id` in session |

**Problems:**

1. Roles are **hardcoded** in Go (`rolePermissions` map).
2. No **permission catalog** for matrix editor (consumed by M26-UI).
3. Functional gaps from AdTech/arbitrage personas (finance vs fraud vs supply) — see table below.
4. Dual HTTP surface (`/admin` legacy vs `/api/v1`); many report `501` stubs.

### AdTech / arbitrage personas → **role templates** (seed data)

Templates are **read-only presets** shipped with the product. Admin clones a template into a **custom role** and may trim permissions. Templates are re-seeded on upgrade (merge new permissions only; never shrink custom roles).

| Template code | Display name | Typical permissions |
| :--- | :--- | :--- |
| `TPL_PLATFORM_ADMIN` | Platform administrator | `*` (all assignable permissions incl. `iam:write`) |
| `TPL_OPS_ENGINEER` | Ops / SRE | `ops:*`, `shards:*`, `audit:read`, `campaigns:read`, `iam:read` |
| `TPL_HEAD_BUYER` | Head of media buying | `campaigns:*`, `brands:*`, `analytics:*`, `cost:read`, `postbacks:*`, `customers:read`, `teams:read` |
| `TPL_MEDIA_BUYER` | Media buyer / trafficker | `campaigns:*`, `brands:*`, `analytics:read`, `postbacks:read`, `customers:read` |
| `TPL_AD_OPS` | Ad operations | `campaigns:read`, `campaigns:write` (delivery subset), `postbacks:*`, `analytics:read` |
| `TPL_ANALYST` | ROI / arbitrage analyst | `analytics:*`, `cost:read`, `campaigns:read`, `customers:read` |
| `TPL_FINANCE` | Finance / billing | `finance:*`, `customers:read`, `audit:read` |
| `TPL_FRAUD_ANALYST` | Fraud / IVT | `fraud:*`, `blacklist:*`, `analytics:read`, `campaigns:read` |
| `TPL_SUPPLY_MANAGER` | Supply / programmatic | `supply:*`, `analytics:read`, `campaigns:read` |
| `TPL_ADVERTISER` | Advertiser (tenant) | Self-serve bundle; tenant-scoped only |
| `TPL_IAM_VIEWER` | IAM read-only | `iam:read`, `audit:read` |

Legacy JWT aliases preserved: `A` → `TPL_PLATFORM_ADMIN`, `M` → `TPL_HEAD_BUYER`, `U` → `TPL_ADVERTISER`.

---

### Target model: templates + custom roles + permission catalog

#### Permission catalog (static in code, metadata in DB)

**Assignable permissions** live in `internal/management/permission_catalog.go` (single source for enforcement + UI labels). DB table `permissions` mirrors catalog on migrate for SQL joins and i18n overrides.

```text
# Domain groups (UI matrix columns)
campaigns:read|write     brands:read|write       customers:read|write
finance:read|write       fraud:read|write        supply:read|write
ops:read|write           analytics:read|write    postbacks:read|write
cost:read|write          audit:read              shards:read|write
settings:read|write      iam:read|write          teams:read|write
blacklist:read|write     users:read|write        api_keys:read|write
```

- `iam:read` — list roles, users, teams, permission catalog; view assignments.
- `iam:write` — create/edit/delete **custom** roles, assign roles to users, manage teams.
- `users:write` — deprecated path for register; folds into `iam:write` for staff user create.
- **Wildcard:** template `TPL_PLATFORM_ADMIN` stores sentinel `*`; runtime expands to all assignable keys.
- **License gate:** `effective = catalog.Has(perm) && license.Feature[domain]` ([MANAGEMENT.md](./MANAGEMENT.md) entitlements).

`settings:*` narrowed to system key-value only; RTB/fraud/breaker move to domain permissions.

#### Database schema

```sql
-- Catalog (synced from code on startup/migrate)
permissions (
  key           TEXT PRIMARY KEY,          -- e.g. campaigns:write
  domain        TEXT NOT NULL,             -- campaigns | finance | iam | ...
  label         TEXT NOT NULL,             -- UI label
  description   TEXT,
  is_assignable BOOLEAN NOT NULL DEFAULT true
)

-- Templates (product seed; is_template=true, customer_id NULL)
roles (
  id            UUID PRIMARY KEY,
  customer_id   UUID NULL,                 -- NULL = global staff role; set = tenant custom role
  code          TEXT UNIQUE,                 -- TPL_MEDIA_BUYER or custom slug
  name          TEXT NOT NULL,
  description   TEXT,
  is_template   BOOLEAN NOT NULL DEFAULT false,
  is_system     BOOLEAN NOT NULL DEFAULT false,  -- undeletable: TPL_PLATFORM_ADMIN, TPL_ADVERTISER
  cloned_from_id UUID NULL REFERENCES roles(id),
  permission_version INT NOT NULL DEFAULT 1,   -- bump on edit → force session refresh
  created_at    TIMESTAMPTZ,
  updated_at    TIMESTAMPTZ,
  created_by    UUID NULL
)

role_permissions (
  role_id       UUID REFERENCES roles(id) ON DELETE CASCADE,
  permission_key TEXT REFERENCES permissions(key),
  PRIMARY KEY (role_id, permission_key)
)

-- users: add role_id FK (auth DB or management mirror)
-- users.role TEXT kept for migration; backfill role_id from template code

teams (id, customer_id, name, created_at)
team_members (team_id, user_id, role_id)   -- optional desk-level role override
team_campaigns (team_id, campaign_id)

role_audit_log (
  id, actor_user_id, action, role_id, target_user_id,
  before_json, after_json, created_at
)
```

**Runtime:** on login, resolve `user.role_id` → join `role_permissions` → cache `[]string` in JWT claims or server-side session with `permission_epoch`. Middleware: `HasPermission(cached, required)` then `Authorize(scope)`.

Invalidate sessions when `roles.permission_version` increments (same pattern as registry stale-serve).

#### Scope (ABAC-lite)

`AuthenticatedUser` extended:

```text
RoleID, CustomerID, TeamIDs[], PermissionEpoch
```

`Authorize(ctx, perm, Resource{CustomerID, CampaignID})` — staff respects `team_campaigns`; advertiser respects `customer_id` only.

---

### IAM JSON API — `/api/v1/iam/*`

Editable **only** by users with `iam:write`. `iam:read` — view-only. Consumed by M26-UI React screens.

| Method | Route | Permission |
| :--- | :--- | :--- |
| `GET` | `/api/v1/iam/permissions` | `iam:read` |
| `GET` | `/api/v1/iam/roles` | `iam:read` |
| `POST` | `/api/v1/iam/roles` | `iam:write` |
| `GET` | `/api/v1/iam/roles/{id}` | `iam:read` |
| `PUT` | `/api/v1/iam/roles/{id}` | `iam:write` |
| `DELETE` | `/api/v1/iam/roles/{id}` | `iam:write` |
| `POST` | `/api/v1/iam/roles/{id}/clone` | `iam:write` |
| `GET` | `/api/v1/iam/role-templates` | `iam:read` |
| `GET` | `/api/v1/iam/users` | `iam:read` |
| `POST` | `/api/v1/iam/users` | `iam:write` |
| `GET` | `/api/v1/iam/users/{id}` | `iam:read` |
| `PUT` | `/api/v1/iam/users/{id}` | `iam:write` |
| `PUT` | `/api/v1/iam/users/{id}/role` | `iam:write` |
| `POST` | `/api/v1/iam/users/{id}/block` | `iam:write` |
| `GET/POST/PUT` | `/api/v1/iam/teams` | `teams:read` / `teams:write` |
| `GET` | `/api/v1/iam/audit` | `audit:read` |
| `GET/DELETE` | `/api/v1/iam/api-keys` | `api_keys:read` / `api_keys:write` |

`GET /api/v1/auth/me` returns: `role_id`, `role_name`, `permissions[]`, `permission_epoch`, `teams[]`.

**Permission matrix rules** (enforced server-side; UI in M26-UI):

- Read implied when write checked.
- Tenant custom roles cannot grant `iam:write` or `shards:*`.
- Dangerous permissions require `iam:write` to assign.

---

### Delivery / analytics API (post-IAM)

Unify on `/api/v1`; deprecate `/admin/*` HTML routes. Authorization uses **resolved permissions from role**, not hardcoded `A`/`M`/`U`.

| Area | Key routes | Permission |
| :--- | :--- | :--- |
| Campaigns | `GET/POST/PATCH /api/v1/campaigns`, `.../clone`, `bulk/pause`, pause/resume/schedule/pacing | `campaigns:*` |
| Budget | `GET .../budget/status`, `POST .../budget/adjust`, `.../approve` | `finance:*` / approval |
| Creatives | `/api/v1/brands/{id}/creatives`, submit-review, approve | `brands:*` |
| Analytics | `/api/v1/reports/*`, `/dashboards/*` | `analytics:read` |
| Arbitrage | `/api/v1/offers`, `campaigns/{id}/offers`, `.../sources` | `cost:*`, `analytics:read` |
| Postbacks | existing `/api/v1/postbacks/*` | `postbacks:*` |
| Fraud / supply / ops | `/api/v1/fraud/*`, `/supply/*`, `/ops/*` | domain perms |

Full route table: see M26-20..28 in deliverables below.

---

### Design patterns (flat package)

| Pattern | Use |
| :--- | :--- |
| **Permission catalog** | `permission_catalog.go` — compile-time list; migrate syncs to `permissions` table |
| **Template seed** | `role_template_seed.go` + migration `INSERT` on deploy; idempotent upsert |
| **Policy functions** | `Authorize(ctx, perm, Resource)` — no interface registry |
| **Route registry** | `handler_iam_registry.go`, `handler_registry.go` — `{method, path, perm, handler}` |
| **Command + audit** | Role/user mutations in PG txn + `role_audit_log` row |
| **Session invalidation** | `permission_version` on role; middleware rejects stale epoch |
| **DTO at boundary** | `RoleDTO`, `PermissionMatrixDTO`, `UserAdminDTO` in `handler_iam.go` |
| **CQRS-lite** | IAM reads from PG; no Redis on IAM path |
| **Approval queue** | `approval_requests` for budget/creative (separate from IAM) |

**Forbidden:** CA/DDD layers, `entity/`, `usecase/`, per-table repository interfaces.

---

### M26 deliverables

#### Phase A — Permission catalog and DB roles

| ID | Deliverable |
| :--- | :--- |
| M26-01 | `permission_catalog.go` + migration `permissions`, extended domain perms (`finance`, `fraud`, `supply`, `ops`, `analytics`, `postbacks`, `cost`, `iam`, `teams`, `api_keys`) |
| M26-02 | `roles`, `role_permissions`, `role_audit_log` migrations + sqlc |
| M26-03 | Seed **role templates** (`role_template_seed.go`) — 11 templates from persona table |
| M26-04 | Backfill `users.role_id` from legacy `users.role` (`A`/`M`/`U`); keep alias in JWT during transition |
| M26-05 | Runtime: load permissions from DB on login; `permission_epoch` in session/JWT; middleware uses DB-backed set |
| M26-06 | Deprecate hardcoded `rolePermissions` map; `GetPermissionsForRole(roleID)` from DB |

#### Phase B — IAM JSON API (no UI)

| ID | Deliverable |
| :--- | :--- |
| M26-10 | `handler_iam.go` — `/api/v1/iam/*` CRUD (roles, users, teams, permissions, audit) |
| M26-11 | `GET /api/v1/auth/me` extended with `permissions[]`, `role_id`, `permission_epoch` |
| M26-12 | Staff API keys admin (`/api/v1/iam/api-keys`) |
| M26-13 | All IAM writes append `role_audit_log` |

#### Phase C — Authorization wiring

| ID | Deliverable |
| :--- | :--- |
| M26-15 | `rbac_policy.go` — `Authorize(ctx, perm, Resource)` + team/customer scope |
| M26-16 | Route registry; migrate handlers off `PermSettingsWrite` monolith to domain perms |
| M26-17 | `TestRBAC_RouteMatrix` — every route × template × negative case |
| M26-18 | Tenant guard: custom tenant roles cannot assign `iam:write`, `shards:*`, `ops:write` |
| M26-19 | Chaos: extend `api_chaos_test.go` for IAM escalation attempts |

#### Phase D — Delivery APIs (backend for M26-UI)

| ID | Deliverable |
| :--- | :--- |
| M26-20 | `/api/v1` campaign mirror (pause, resume, schedule, pacing, templates) |
| M26-21 | `PATCH /api/v1/campaigns/{id}`, `POST .../clone`, `bulk/pause` |
| M26-22 | `GET .../budget/status`, `POST .../budget/adjust` + `approval_requests` |
| M26-23 | `/api/v1/brands/{id}/creatives` mirror + submit-review / approve |
| M26-24 | `POST /api/v1/placements/{id}/pause` |
| M26-25 | Offer catalog + `campaigns/{id}/offers` (arbitrage) |
| M26-26 | Split `/api/v1/fraud`, `/supply`, `/ops` route groups with domain perms |
| M26-27 | M18 dashboard/report routes use `analytics:read` from user role (not hardcoded) |

### Acceptance (backend)

- Admin with `iam:write` can create custom role from template and assign to user via API; without deploy
- `TPL_PLATFORM_ADMIN` / `TPL_ADVERTISER` cannot be deleted; template defaults resettable on clone only
- User with custom “Buyer limited” role loses access within 1 request after `permission_version` bump
- `iam:read` user gets 403 on `PUT /api/v1/iam/roles/{id}`
- Tenant custom role cannot grant `iam:write` (server rejects)
- `TestRBAC_RouteMatrix` + IAM escalation chaos tests pass
- Legacy `A`/`M`/`U` tokens work until `role_id` migration complete

### SLA checklist

- [ ] IAM API p95 < 200 ms; `/api/v1/auth/me` p95 < 100 ms
- [ ] `permission_version` bump → session invalidation within 1 request
- [ ] Delivery API mutations via outbox — no direct HTTP→Redis
- [ ] Report routes (M26-27) p95 < 500 ms with `analytics:read` gate

### DoD checklist

- [ ] GAP-RBAC-01, GAP-RBAC-02 closed (backend portion)
- [ ] Phases A–D deliverables complete; M26-28 menu visibility deferred to M26-UI
- [ ] `TestRBAC_RouteMatrix` — every route × template × negative case
- [ ] IAM escalation chaos tests pass
- [ ] Legacy `A`/`M`/`U` JWT aliases work during migration

### Testing checklist

- [ ] `go test ./internal/management/... -run 'IAM|RBAC|RoleTemplate|PermissionMatrix' -short`
- [ ] `go test ./internal/adminapi/... -short`
- [ ] `TestRBAC_RouteMatrix` — route × role × deny cases
- [ ] IAM escalation in `api_chaos_test.go`
- [ ] Outbox test: role mutation → `role_audit_log` row
- [ ] `permission_epoch` bump → 403 on stale session

### Code style checklist

- [ ] **Cold path:** `Authorize(ctx, perm, Resource)` — policy functions, no interface registry
- [ ] **Cold path:** `RoleDTO` / `PermissionMatrixDTO` at handler boundary only
- [ ] **Cold path:** PG txn + `role_audit_log` on every IAM write
- [ ] **Forbidden:** CA/DDD layers, `entity/`, `usecase/`, per-table repository interfaces
- [ ] No Redis on IAM read path

### Patterns

- [ ] **Permission catalog** — `permission_catalog.go` + DB mirror on migrate
- [ ] **Template seed** — idempotent upsert; 11 AdTech presets
- [ ] **Session invalidation** — `permission_version` on role edit
- [ ] **Route registry** — `{method, path, perm, handler}` slice
- [ ] **Approval queue** — `approval_requests` for budget/creative (separate from IAM)
- [ ] **CQRS-lite** — IAM reads PG; delivery writes PG+outbox

### Migration notes

1. Ship Phase A + Phase B API before rewiring handlers (Phase C).
2. `users.role` TEXT column deprecated after backfill; remove after M26-UI ships.
3. `POST /api/v1/auth/register` requires `iam:write`; document in [MANAGEMENT.md](./MANAGEMENT.md).

---

## M23 — Engineering and platform debt

**Goal:** Reduce operability friction and cold-path bottlenecks without hot-path risk.

**Priority:** P3–P4 · **Background**

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M23-01 | Management package split plan (filename tags → bounded subpackages) | GAP-ENG-01 |
| M23-02 | Vendor telemetry opt-in bundle (default off) | GAP-ENG-03 |
| M23-03 | Interactive installer wizard (`espx-install` first-run `.env`) | DATA.md INST-P1 |
| M23-04 | Logger group-commit fsync (`pkg/logger/flush_persist.go`) | GAP-DB-01 |
| M23-05 | CH spool group-commit (if PEL retains unacked under load) | GAP-DB-02 |
| M23-06 | Metrics-driven `AcquireWeighted` processor gates | GAP-DB-03 |
| M23-07 | Outbox poll backoff / `LISTEN/NOTIFY` (reduce 20 ms idle WAL pressure) | DATA.md Part V |
| M23-08 | `admin_audit_log` sampling or CH offload on ledger flush | DATA.md Part V |

### Acceptance

- No hot-path file moves in M23-01 (plan + first cold-path extraction only)
- Vendor telemetry build tag; zero outbound calls when disabled
- Processor PG gate metrics stable under M23-06 tuning

### SLA checklist

- [ ] Processor PG gate: no PEL backlog growth under tuned `AcquireWeighted` (M23-06)
- [ ] Outbox poll: WAL pressure reduced vs 20 ms idle baseline (M23-07)
- [ ] Logger/spool fsync: no p99 regression on processor hot path

### DoD checklist

- [ ] GAP-ENG-01, GAP-ENG-03, GAP-DB-01..03 addressed per deliverable
- [ ] M23-01: plan only + first cold-path extraction — no hot-path moves
- [ ] Vendor telemetry build tag; default off; zero outbound when disabled
- [ ] Installer wizard (M23-03) documented in [DEVELOPMENT.md](./DEVELOPMENT.md)

### Testing checklist

- [ ] `go test ./... -short` after each M23 item
- [ ] Processor gate metrics benchmark before/after M23-06
- [ ] Outbox poll integration test with `LISTEN/NOTIFY` (M23-07)
- [ ] Vendor telemetry: build with tag off → no network calls in test

### Code style checklist

- [ ] **Cold path only** — no changes to `internal/ingestion` hot loops
- [ ] **Cold path:** `mgmtPgSem`, `errgroup`, `FOR UPDATE SKIP LOCKED` patterns preserved
- [ ] Package split (M23-01): filename tags → bounded subpackages; no `usecase/`

### Patterns

- [ ] **Filename modules** — flat package; split by concern not layer
- [ ] **Build tag** — vendor telemetry opt-in (`//go:build telemetry`)
- [ ] **Group-commit** — logger/spool batch fsync where measured benefit
- [ ] **LISTEN/NOTIFY** — outbox wake instead of tight poll loop

---

## M25 — XDP tier D and deferred B2

**Goal:** Portability, perf gate, optional research items for edge L4.

**Priority:** P4 · **Depends on:** M10 tiers A–C

| ID | Deliverable | Source |
| :--- | :--- | :--- |
| M25-01 | CO-RE / BTF libbpf skeleton (M10-D1) | EBPF.md §7 |
| M25-02 | Native XDP perf gate in `scripts/perf-gate/` (M10-D4) | EBPF.md §10 CI gaps |
| M25-03 | Privileged BPF CI job or self-hosted runner (memlock) | EBPF.md §10 |
| M25-04 | XDP chaos injector for lab/staging only (M10-D3) | EBPF.md §7 |
| M25-05 | Spoof block map — **deferred pending legal review** (M10-B2) | EBPF.md §5 |
| M25-06 | HW offload documentation + SmartNIC lab proof (M10-D2) | EBPF.md §7 |

### Acceptance

- M25-02: tracker p99 ±2 ms with XDP attached under load test
- M25-05: no merge until legal sign-off; compliance checklist updated
- `go test ./internal/edge/bpf/...` green on privileged runner

### SLA checklist

- [ ] M25-02: tracker p99 delta ≤ ±2 ms with XDP attached under load test
- [ ] XDP perf gate in `scripts/perf-gate/` documented threshold
- [ ] M25-05 (spoof block): **no merge** until legal sign-off

### DoD checklist

- [ ] M25-01..04, M25-06 per deliverable; M25-05 explicitly excluded until legal
- [ ] Privileged BPF CI job or self-hosted runner documented
- [ ] EBPF CI gaps closed in [EBPF.md](./EBPF.md) §10
- [ ] `bash scripts/ci/check_compliance.sh` updated if edge behavior changes

### Testing checklist

- [ ] `go test ./internal/edge/bpf/... -count=1` on privileged runner (CAP_BPF + memlock)
- [ ] `scripts/perf-gate/` XDP bench attached to PR (M25-02)
- [ ] XDP chaos injector lab-only; not in default CI
- [ ] `bash scripts/ci/check_compliance.sh`

### Code style checklist

- [ ] **Edge/BPF:** CO-RE/BTF skeleton; no hot-path Go changes for XDP attach
- [ ] Lab/staging only injectors — not enabled in prod compose default
- [ ] Compliance: defensive edge only; M25-05 blocked on legal

### Patterns

- [ ] **libbpf CO-RE** — BTF skeleton for portability (M25-01)
- [ ] **Perf gate** — native XDP in `scripts/perf-gate/` (M25-02)
- [ ] **Lab injector** — chaos XDP staging-only (M25-04)
- [ ] **Legal gate** — M25-05 spoof block deferred; checklist before enable

---

## UI, UX & frontend (last)

Platform milestones above must ship first. UI track consumes `/api/v1/*` only — no HTMX/Templ.

---

## M18 — Dashboard & report APIs (backend for UI)

**Goal:** Implement scaffold admin API routes; operators and tenants get real reporting data for Mission Control screens.

**Priority:** P1 · **Depends on:** M3 (recon), CH MVs, M26 Phase A

| ID | Deliverable | Closes |
| :--- | :--- | :--- |
| M18-01 | `GET /api/v1/dashboards/buyer` — spend, pacing, budget burn | GAP-PROD-01 |
| M18-02 | `GET /api/v1/dashboards/cfo`, `/accountant` — ledger, forecast, close metrics | GAP-PROD-01 |
| M18-03 | `GET /api/v1/dashboards/adops`, `/fraud` — delivery health, IVT overview | GAP-PROD-01 |
| M18-04 | Reports: `pacing-drift`, `spend-velocity`, `campaign-overview`, `geo-roi`, `ivt-by-source` | GAP-PROD-01 |
| M18-05 | `POST /api/v1/reports/jobs` — async export jobs | GAP-PROD-01 |
| M18-06 | Remaining report stubs (`source-margin`, `daypart-heatmap`, `discrepancy-buy-sell`, …) | GAP-PROD-01 |

**Out of scope:** GAP-PROD-03 (OpenAPI) — deferred by design; godoc remains contract. No React in this milestone.

### Acceptance

- Zero `501 NOT_IMPLEMENTED` on P1 dashboard and report routes
- `CompositeReadService` used for PG+CH statements; `stale=true` when CH lag > 5 min
- RBAC enforced via M26 role permissions (`analytics:read` on report routes)

### SLA checklist

- [ ] Dashboard/report API p95 < 500 ms
- [ ] `stale=true` in response when CH lag > 5 min
- [ ] Async export jobs (M18-05) complete within documented SLA per row count
- [ ] Pagination enforced — no unbounded list responses

### DoD checklist

- [ ] GAP-PROD-01 closed (API portion; screens in M26-UI)
- [ ] Zero `501 NOT_IMPLEMENTED` on P1 dashboard/report routes
- [ ] `CompositeReadService` for all PG+CH statements
- [ ] RBAC: `analytics:read` on every report route

### Testing checklist

- [ ] `go test ./internal/adminapi/... -run 'Dashboard|Report' -short`
- [ ] Per route: RBAC deny + happy path + `mapServiceError` mapping
- [ ] `stale=true` when CH lag simulated > 5 min
- [ ] Async job: submit → poll → download integration test
- [ ] Pagination: limit/offset/cursor tests on large datasets

### Code style checklist

- [ ] **Cold path:** all CH queries through `CHQuery`
- [ ] **Cold path:** `to*DTO` at handler boundary; `coldpath.MapSlice` for lists
- [ ] **Cold path:** `mapServiceError` + `writeServiceError`; validation → 400
- [ ] No React code in M18 — JSON API only

### Patterns

- [ ] **CompositeReadService** — PG + CH in one read facade
- [ ] **Stale flag** — CH lag exposed to UI for graceful degradation
- [ ] **Async export** — job queue + poll endpoint (M18-05)
- [ ] **RBAC gate** — `analytics:read` from M26 resolved permissions

---

## M26-UI — Mission Control Dashboard (React 19)

**Goal:** On-Premise SPA baked into management binary via `//go:embed`; 4 fixed screens; Dark Mode Only; zero CDN; bundle < 200 KB Gzip.

**Priority:** P1 · **Depends on:** M18, M26 backend · **Effort:** ~2 weeks (1 dev + Cursor)

### Tech stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Framework** | React 19 + TypeScript | Strict DTO typing from `internal/adminapi` |
| **Build** | Vite → `frontend/dist` | `npm run build`; embedded by Go |
| **CSS** | Tailwind CSS v4 | Dark Mode Only; no custom `.css` |
| **Components** | Shadcn UI + Radix UI | Cards, Tables, Dialogs, Tabs — no custom primitives |
| **Charts** | Tremor + Recharts | Latency AreaCharts, budget bars, KPI cards |
| **State** | TanStack Query v5 | Cache, polling, 503 graceful degradation |
| **Tables** | TanStack Table v8 + `@tanstack/react-virtual` | Campaigns, audit logs (~20 DOM rows) |
| **Real-time** | WebSocket / SSE | Metrics stream (M27 extends) |
| **Go delivery** | `ui.go` + `//go:embed` | SPA fallback: non-API routes → `index.html` |

### Envelope requirements

| Rule | Detail |
| :--- | :--- |
| **Zero external runtime** | No Node.js on server; single Go binary |
| **Offline-first** | No CDN fonts/scripts; lucide-react + fonts in bundle |
| **Bundle limit** | JS+CSS Gzip ≤ ~200 KB |
| **Main thread** | No heavy filter/sort in JS; pagination/search via `/api/v1/*` |
| **Throttling** | `useDebounce(200ms)` on inputs; WS metrics flush @ 250ms via `requestAnimationFrame` |
| **Degradation** | `503 registry_stale` / `503 shard_unavailable` → warning banner, no crash |

### Screens (strictly 4)

| # | Screen | Components | API |
| :--- | :--- | :--- | :--- |
| 1 | **Mission Control** | p95/p99 AreaCharts, 4× Redis shard status, MPSC ring gauge | M18 dashboards, ops metrics WS |
| 2 | **Campaigns Studio** | TanStack Table, Play/Pause, budget_limit + M8 quanta sliders | `/api/v1/campaigns/*` |
| 3 | **Security Shield** | IVT signals, XDP/eBPF drop chart, IP blacklist table | `/api/v1/fraud/*`, `/api/v1/blacklist/*` |
| 4 | **System & IAM** | JWT license form, role matrix editor, users/teams, Outbox/DLQ | `/api/v1/iam/*`, `/api/v1/ops/*` |

### Deliverables

| ID | Deliverable |
| :--- | :--- |
| M26UI-01 | Vite + React 19 scaffold; Tailwind 4 + Shadcn base; Dark theme tokens |
| M26UI-02 | `internal/management/ui.go` — `//go:embed frontend/dist` + SPA fallback router |
| M26UI-03 | Screen 1: Mission Control (Tremor charts + ops WS hook) |
| M26UI-04 | Screen 2: Campaigns Studio (TanStack Table + M8 budget controls) |
| M26UI-05 | Screen 3: Security Shield (fraud/XDP visualization) |
| M26UI-06 | Screen 4: System & IAM (role matrix, user/team admin, license form) |
| M26UI-07 | Sidebar nav filtered by `permissions[]` from `/auth/me` |
| M26UI-08 | DTO types generated/synced from Go structs; 503 degradation handler |
| M26UI-09 | `make frontend-build` target; CI bundle size gate (200 KB Gzip) |
| M26UI-10 | Remove HTMX/Templ admin routes; redirect `/admin/*` → SPA |

### Acceptance

- Single binary serves API + UI; no Node on server
- Bundle < 200 KB Gzip; zero external network calls in browser
- All 4 screens functional with live API data
- Permission matrix editor creates/edits roles without deploy
- Sidebar hides routes user cannot access

### SLA checklist

- [ ] Bundle JS+CSS Gzip ≤ 200 KB (`make frontend-build` gate)
- [ ] TTI first paint < 2 s on LAN
- [ ] WS metrics flush @ 250 ms; no frame drops > 16 ms during stream
- [ ] `503 registry_stale` / `503 shard_unavailable` → banner only; no crash
- [ ] Search/filter debounce 200 ms; server-side pagination only

### DoD checklist

- [ ] M26UI-01..10 complete; HTMX/Templ routes removed (M26UI-10)
- [ ] Staging sign-off: Mission Control 4 screens with live API (global DoD M3)
- [ ] GAP-PROD-01 closed (UI portion); GAP-RBAC-01/02 UI portion closed
- [ ] `make frontend-build` in CI; bundle size gate enforced
- [ ] Zero external CDN requests (offline audit in browser devtools)

### Testing checklist

- [ ] `cd frontend && npm run build && npm run test`
- [ ] Bundle gate: `gzip -c frontend/dist/assets/*.js | wc -c` ≤ 204800
- [ ] E2E smoke: login → each of 4 screens loads data
- [ ] RBAC: sidebar hides forbidden routes per `permissions[]`
- [ ] 503 degradation: mock `registry_stale` → warning banner, app stable
- [ ] Role matrix: create role from template → assign user → 403 on stale epoch

### Code style checklist

- [ ] **Frontend:** Shadcn + Tremor only; Dark Mode Only; Tailwind v4
- [ ] **Frontend:** TanStack Query for all API calls; no raw `fetch` loops
- [ ] **Frontend:** `@tanstack/react-virtual` on audit/log tables
- [ ] **Frontend:** `useDebounce(200ms)` on inputs; WS buffer @ 250ms rAF
- [ ] **Go:** `ui.go` SPA fallback; `//go:embed frontend/dist`
- [ ] **Forbidden:** client-side sort/filter of large arrays; custom modals/charts

### Patterns

- [ ] **SPA fallback** — non-API routes → `index.html` 200
- [ ] **DTO sync** — TS types from `internal/adminapi` structs
- [ ] **Permission-driven nav** — sidebar from `/auth/me` `permissions[]`
- [ ] **Graceful degradation** — 503 codes → banner component
- [ ] **Server-side pagination** — TanStack Table + API `limit`/`offset`

---

## M27 — Supervisor UI and Agentic Ops

**Goal:** Extend Mission Control with real-time stream observability, collaborative annotations, and agentic automation — React components only, flat `management` package.

**Priority:** P2 · **Depends on:** M18, M26-UI

| ID | Deliverable | Focus |
| :--- | :--- | :--- |
| M27-01 | **Live Traffic Stream (gnet-ws)** — sampled `/track` events via WebSocket to React UI | Real-time |
| M27-02 | **Event Annotations** — notes on campaigns with Recharts timeline markers | Collaboration |
| M27-03 | **Optimization Rules Engine** — visual if-then builder (JSON DSL) → outbox actions | Automation |
| M27-04 | **MCP Server for eSPX** — LLMs query ClickHouse/Postgres via natural language | AI / Agentic |
| M27-05 | **Forecast UI Extension** — budget depletion runway + P&L simulations (Tremor) | Finance |
| M27-06 | **Placement Health Matrix** — 2D drill-down (CTR vs IVT) with bulk actions | Arbitrage |
| M27-07 | **Notification Center** — toast/history for outbox failures and rule triggers | Ops |

### Design patterns (Agentic / Supervisor)

| Pattern | Implementation |
| :--- | :--- |
| **WS Sampling** | `gnet` → MPSC ring → WebSocket worker → React `useRef` buffer @ 250ms |
| **Command DSL** | JSON AST in `margin_guard_policies`; edited via Shadcn form builder |
| **Audit Context** | Every action carries `actor_id` (user or `bot_id`) + `reason` |
| **Live Blocks** | TanStack Query polling or SSE `EventSource` for CH hot tables |

### Acceptance

- Sampled Live Stream visible in React with ≤ 100ms latency
- Rule created in UI triggers `PAUSE_PLACEMENT` outbox event
- External AI agent queries P&L via MCP
- Charts show annotation markers from media buyers

### SLA checklist

- [ ] Live Stream WS latency ≤ 100 ms (sampled events)
- [ ] WS metrics flush @ 250 ms; UI frame budget ≤ 16 ms
- [ ] Rule engine action → outbox event < 2 s (cold path)
- [ ] MCP read queries p95 < 5 s (analytical; not hot path)

### DoD checklist

- [ ] M27-01..07 deliverables complete
- [ ] Every agentic/manual action carries `actor_id` + `reason` in audit
- [ ] MCP server documented; auth gate for external agents
- [ ] Rule DSL stored in `margin_guard_policies`; no arbitrary code exec

### Testing checklist

- [ ] WS integration: sampled event appears in React ≤ 100 ms
- [ ] Rule engine: UI rule → `PAUSE_PLACEMENT` outbox row assertion
- [ ] Annotation CRUD + chart marker render test
- [ ] MCP smoke: natural language query returns bounded row set
- [ ] Notification center: outbox failure → toast + history entry

### Code style checklist

- [ ] **Frontend:** WS buffer in `useRef`; rAF flush @ 250ms
- [ ] **Frontend:** Rule builder = Shadcn forms → JSON AST only
- [ ] **Cold path:** MCP server cold-path only; CH via `CHQuery`
- [ ] **Cold path:** outbox for all rule-triggered mutations
- [ ] **Forbidden:** LLM direct Redis/ingestion access; HTMX/SSE legacy paths

### Patterns

- [ ] **WS Sampling** — gnet → MPSC ring → WebSocket worker → React buffer
- [ ] **Command DSL** — JSON AST in `margin_guard_policies`
- [ ] **Audit Context** — `actor_id` + `reason` on every action
- [ ] **Live Blocks** — TanStack Query poll or SSE `EventSource`
- [ ] **MCP read facade** — bounded CH/PG queries for external agents

---

## Gap → milestone map

| Gap ID | Milestone | Status |
| :--- | :--- | :--- |
| GAP-RTB-10 | M16 | Open |
| GAP-RTB-11 | M16 | Open |
| GAP-RTB-12 | M17 | Open |
| GAP-PROD-01 | M18 (API) + M26-UI (screens) | Open |
| GAP-RBAC-01 | M26 backend + M26-UI matrix editor | Open |
| GAP-RBAC-02 | M26 backend + M26-UI sidebar scope | Open |
| GAP-PROD-03 | — | Deferred by design |
| GAP-OPS-03 | M15 | Open |
| GAP-OPS-04 | M15 (partial → M15-04) | Partial (M14-11 fraud slice done) |
| GAP-ENG-02 | M15 | Open |
| GAP-ENG-01 | M23 | Open |
| GAP-ENG-03 | M23 | Open |
| GAP-DATA-01 | M20 | Open |
| GAP-CMP-01 | M20 (partial → M20-03/04) | Partial (M14-08 tarpit shipped) |
| GAP-PAY-01 | M21 | Open |
| GAP-GEO-01 | M22 | Open |
| GAP-GEO-02 | M22 | Open |
| GAP-DB-01..03 | M23 | Open |
| GAP-SHARD-04 | M14 | **Closed** |
| GAP-WIRE-03/04 | M14 | **Closed** |
| M14 residuals (auth, consent, creative notify) | M19 | Open |
| M12-05 registry replica | M19-06 | Optional |
| M10-B2 spoof block | M25-05 | Deferred (legal) |
| CHAOS Scenarios I, J, G, H, M2 matrix | M24 | Open |
| DATA.md SEM-P3 | M15 | Open |
| DATA.md SEM-P4 | M23 | Conditional |
| DATA.md INST-P1 | M23 | Open |
| EBPF CI gaps | M25 | Open |

---

## Suggested sequencing

```text
# Platform (hardest → easiest) — ship first
M15 (P0 prod hardening)
  ├─→ M16 → M17 (RTB live prod)
  ├─→ M19 (shard-0 follow-up)
  ├─→ M20 (PII/compliance)
  ├─→ M24 (chaos)           [parallel after M15]
  ├─→ M21 (crypto payments)
  ├─→ M26 backend (RBAC API)
  ├─→ M23 (eng debt)        [background]
  ├─→ M25 (XDP lab)         [background]
  └─→ M22 (multi-region DR) [after M17-05]

# UI / UX — ship last (after APIs stable)
M26 Phase A (permission catalog)
  └─→ M18 (dashboard/report APIs)
        └─→ M26-UI (React Mission Control, ~2 weeks)
              └─→ M27 (supervisor + agentic)
```

| Phase | Milestones | Outcome |
| :--- | :--- | :--- |
| **Q1** | M15, M16, M24 (I/J), M26 Phase A–C | Prod-ready stack; RBAC backend |
| **Q2** | M17, M19, M20, M21 | RTB live; shard-0 hardening; PII; crypto |
| **Q3** | M18, M26 Phase D, M26-UI | Report APIs + Mission Control SPA |
| **Q4** | M27, M22, M23, M25 | Agentic UI; multi-region; platform debt; XDP |

---

## Verification commands (per milestone)

```bash
# M15
make dev-up && bash scripts/chaos-drills/m14_shard0_failure.sh

# M16–M17
go test ./internal/rtb/... ./internal/ingestion/... -run Rtb -short
make test-alloc-gate

# M26 (RBAC backend)
go test ./internal/management/... -run 'IAM|RBAC|RoleTemplate|PermissionMatrix' -short
go test ./internal/adminapi/... -short

# M18 (report APIs)
go test ./internal/adminapi/... -run 'Dashboard|Report' -short

# M26-UI (frontend)
cd frontend && npm run build && npm run test
# bundle gate: gzip -c frontend/dist/assets/*.js | wc -c  # ≤ 204800

# M19
go test ./tests/chaos/ -run TestChaos_Shard0Outage -timeout 15m

# M24
./scripts/chaos-drills/test_chaos.sh

# M25
go test ./internal/edge/bpf/... -count=1   # privileged
bash scripts/ci/check_compliance.sh
```

---

## Document index

| Document | Role |
| :--- | :--- |
| [CAPABILITIES.md](./CAPABILITIES.md) | Shipped M1–M14 detail |
| [BACKLOG.md](./BACKLOG.md) | Gap IDs and priority themes |
| [RTB.md](./RTB.md) | R1–R31 RTB roadmap |
| [CHAOS.md](./CHAOS.md) | Fault injection catalog |
| [DATA.md](./DATA.md) | Store semantics, DR, bottlenecks |
| [M14_SHARD0_TECHNICAL_REPORT.md](./M14_SHARD0_TECHNICAL_REPORT.md) | Shard-0 residual risks |
| [GO.md](./GO.md) | Hot-path performance rules |
| [STYLE.md](./STYLE.md) | Layout, errors, cold-path idioms |

*Last updated: 2026-07-24 (per-milestone DoD/Testing/Code/Patterns/SLA checklists).*
