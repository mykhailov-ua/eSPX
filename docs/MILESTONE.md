# eSPX — Engineering Milestones & Delivery Roadmap

Unified reference for engineering milestones, delivery criteria, implementation patterns, and chaos matrices. Milestones are **numbered by domain** (M1–M18); **execution order** follows complexity grading — easy, buyer-visible work first; distributed-systems work last.

**Shipped summary:** [SHIPPED.md](./SHIPPED.md). **Open gaps:** [GAPS.md](./GAPS.md). **Hot-path rules:** [GO.md](./GO.md), `.cursorrules`. **Style:** [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md). **Chaos:** [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md).

---

## §0 Normative Standards (all milestones)

Each milestone closes only when its **§Mx.0 Standards envelope**, functional DoD tables, testing section, and applicable **§0.7 CI gates** are green.

### §0.1 Guide documents

| Document | Scope in milestones |
| :--- | :--- |
| `GUIDE_STYLE_CODE.md` | R1 flat packages; R1b `adminapi`; R2 file naming; R3 DTO; R8 hot/cold errors; R9 comments; R10 PR verification |
| `GUIDE_CHAOS_RELIABILITY.md` | R1 steady-state; R7 `chaos_proof`; R8 observability; **R10 when chaos required vs redundant** |
| `GUIDE_IDEAS_MICROSERVICES.md` | Step 0 workload class; score ≥ 11 for new `cmd/`; veto rules |
| `GUIDE_COMPLIANCE.md` | CMP-* (M5); TEL-RED (M10); PII (M14) |
| `GO.md` / `.cursorrules` | Hot-path SLA, zero-alloc, gnet, BCE, atomics — **override STYLE on conflict** |
| `CONCEPTS.md` | Cache-line padding, syscall batching, MPSC rings, DOD layout |

### §0.2 Universal hot-path SLA (`.cursorrules` + `GO.md`)

| Area | Target |
| :--- | :--- |
| Ingestion (`ad_http_request_duration_seconds`) | p95 < 50 ms, p99 < 80 ms, hard ceiling 100 ms |
| Redis unified-filter Lua (per shard) | p99 < 10 ms |
| Geo filter (sampled) | p99 < 10 µs |
| RTB `RunAuction` (M18) | p99 < 15 µs; p99 candidates scanned < 500 |
| Fraud accumulator + boost snapshot | < 500 µs per `FilterEngine.Check`; `BenchmarkFilterFraudBoost` ~90 ns, 0 allocs/op |
| `GetShard` (StaticSlot) | ~5.6 ns, 0 allocs/op |
| Budget invariant | `current_spend ≤ budget_limit` (±1 micro-unit); `AssertBudgetInvariant` |
| Hot-path allocations | **0 allocs/op** on touched paths; `make test-alloc-gate` green |
| Monotonic deadlines | `FilterDeadlineMono`; no wall-clock in filter loops |
| Forbidden on hot path | `defer` in loops, closures in request loops, `interface{}` boxing, `sync.Map`, `fmt.Sprintf`/`+` on strings, dynamic Prometheus labels |

Load-test abort: control-cohort p99 > 80 ms for 30 s **or** budget invariant violation.

### §0.3 Code zones

| Zone | Packages | Error model | Alloc policy |
| :--- | :--- | :--- | :--- |
| **Hot** | `internal/ingestion`, `internal/rtb` | `filterRejectKind` / `NoBidReason`; `filterRejectSpecs` pre-built responses | 0 allocs/op; `unsafe.String` with BCE + `runtime.KeepAlive` |
| **Cold** | `management`, `adminapi`, `payment`, workers | `errors.Is` / `%w`; `mapServiceError` + `writeServiceError` | Idiomatic Go; `pkg/cold` for pagination/JSON |
| **Edge/BPF** | `internal/edge`, `cmd/edge-*` | Verifier-safe C; explicit `data_end` bounds | Kernel maps; per-CPU scratchpad arrays |

### §0.4 Microservice placement (Step 0 + score)

| Workload | Policy | Examples |
| :--- | :--- | :--- |
| Hot-path | Never split | tracker, processor stream consumers |
| Control-plane | `cmd/` if score ≥ 11 | `payment` (16), `postback-sender` (13), `cost-sync` (11) |
| Batch/cron | Library + worker in existing binary | CH janitor → processor; IVT → `ivt-detector` |
| Node utility | Standalone near data | `log-evacuator`, `edge-bpf-sync`, `installer` |

**Veto:** no gRPC from `/track`; no new `cmd/` without active callers; no split for aesthetics.

### §0.5 Chaos engineering (R10 matrix)

| Change type | Required proof |
| :--- | :--- |
| New write path, outbox, stream, budget mutation | Integration + invariants + `chaos_proof` |
| New Redis Lua / shard routing | Real Redis + shard outage test |
| Payment / settlement / auth | Concurrent fault injection |
| Read-only admin JSON, installer CLI | `go test -short` only — **no new chaos** |
| Feature flagged off | Unit tests; chaos when flag enabled |

**Steady-state (R1):** `/track` p99 < 80 ms; error rate < 0.1% (excl. valid rejects); budget drift within ReconWorker window.

### §0.6 CI merge gates

```bash
go test ./... -short
make lint
bash scripts/ci/check_comments.sh
./scripts/chaos-drills/test_chaos.sh         # when R10 applies; CHAOS_MIN_PROOFS >= 52
bash scripts/perf-gate/perf_gate_run.sh      # internal/ingestion|rtb touched
make test-alloc-gate                         # hot-path changes
bash scripts/ci/check_compliance.sh          # M5, M10, M11 block, M14 PII
```

### §0.7 Milestone envelope template

Every milestone **§Mx.0** table MUST cover:

| Dimension | Content |
| :--- | :--- |
| **Guides** | Which guide sections apply |
| **Binaries** | `cmd/*` created or extended |
| **Packages** | `internal/*` layout (R1 flat) |
| **Patterns** | Outbox, idempotency, snapshots, gates |
| **SLA** | Hot and/or cold latency targets |
| **Metrics** | Prometheus names |
| **Code zone** | Hot / cold / edge; R8 subsection |
| **Perf hacks** | GO.md / CONCEPTS techniques if hot-path touched |
| **Chaos R10** | Required / partial / not required + proof lines |
| **CI gates** | Subset of §0.6 |

---

## 1. Complexity Grading

| Tier | Label | Typical effort | Risk profile |
| :---: | :--- | :--- | :--- |
| **S** | Small | days–2 weeks | Cold path only; no hot-path touch |
| **M** | Medium | 2–6 weeks | External APIs; new workers; CH queries |
| **L** | Large | 1–2 months | Cross-component consistency |
| **XL** | Expert | 2+ months | Distributed state; chaos matrix |

**Grading principle:** buyer-visible integrations (**S–M**) precede infrastructure scale (**XL**).

---

## 2. Roadmap Overview — Execution Order

| Exec # | ID | Tier | Title | Status |
| :---: | :---: | :---: | :--- | :--- |
| — | M1–M3, M5 | — | Core platform | **Shipped** |
| 1 | M9 | S | CLI Installer | **Next** |
| 2 | M6-W | S | Buyer Reports | **Next** |
| 3 | M15 | M | S2S Postback | **Shipped** |
| 4–5 | M16–M17 | M | Cost sync + Margin guard | Planned |
| 6 | M6 | M | Day-2 Operations | Planned |
| 7–16 | M3-T … M14 | M–XL | Platform depth → scale → compliance | Backlog |

Full table: [§6 Milestone ID Quick Index](#6-milestone-id-quick-index). Dependencies: [§5](#5-execution-order--dependencies).

---

## 3. Shipped Milestones

---

### M1 — Core Ingestion & Ledger `Shipped`

**Goal:** Production single-site: durability, budget correctness, hot-path isolation.

#### M1.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R1–R10 (write paths); STYLE R8.3 hot; MICRO Step 0 — no new `cmd/` |
| **Binaries** | `tracker`, `processor`, `management` — extend in place |
| **Packages** | `internal/ingestion` flat; `processor_pg_gate.go`, `ch_spool.go` |
| **Patterns** | H1 single-writer spend; D2 mmap CH spool before `XAck`; SEM-P* gates; Lua idempotency |
| **SLA** | §0.2 universal; CH batch ≥ 1000 rows or ≤ 5 s |
| **Perf hacks** | gnet epoll + DFA parse; `PinnedWorkerPool`; vtproto pools; monotonic filter deadline; BCE on buffer indexes |
| **Chaos R10** | **Required** — `clickhouse_outage`, `ch_spool_rotation`, `write_path_db_fail_pre_ack`, `processor_pg_gate_overflow` |
| **CI gates** | §0.6 full stack; perf-gate on `internal/ingestion` |

#### M1.1 Implementation (hot path)

| Component | How | Perf / design |
| :--- | :--- | :--- |
| HTTP ingest | `gnet` event loop; DFA scanner on ring buffer | No per-conn goroutine; `connContext` per connection |
| Parse | `requests_parse.go` byte-slice parse → `domain.Event` pool | `unsafe.String` views; lifetime ≤ gnet read frame; `copy()` for async |
| Filter pipeline | `FilterEngine.Check`: breaker → geo → schedule → fraud boost → Lua | `GetFraudScoreBoosts()` snapshot 0 allocs; MaxMind fail-open |
| Sharding | `StaticSlotSharder`: `[1024]uint8` slot table; atomic load | ~5.6 ns `GetShard`; no `sync.Map` |
| Redis Lua | `unified-filter.lua`: debit + fcap + dedup + XADD one RTT | p99 < 10 ms; `migration_fence` on slot move |
| Stream | `XADD` → processor `XREADGROUP` | `ProcessorPgGate` / `ProcessorChGate` backpressure |
| CH durability | mmap WAL `events.wal.NNNN`; `fsync` before `XAck` | Waterline batching per CONCEPTS §2.1 |
| Budget | Only `SyncWorker` writes `current_spend` | H1 mutex; `balance_ledger` immutable |

#### M1.2 Code style

- **Hot:** `filterRejectSpecs` table; `classifyFilterErr`; no `fmt.Errorf` on reject path (R8.3).
- **Cold:** outbox handlers `json.Marshal` small structs; `%w` error chains (R8.2).
- **Files:** `track_core.go`, `filters.go`, `sharding.go`, `*_chaos_test.go` (R2).

#### M1.3 Testing

```bash
go test ./... -short
go test -benchmem ./internal/ingestion/...    # 0 allocs/op on gated benches
bash scripts/perf-gate/perf_gate_run.sh
make test-alloc-gate
./scripts/chaos-drills/test_chaos.sh
```

| Suite | Criterion |
| :--- | :--- |
| `write_path_chaos_integration_test.go` | PG gate overflow; spool rotation; PEL retained on PG outage |
| `tests/chaos/shard_outage_chaos_test.go` | Shard 0 outage; budget ±1μ |
| `tests/e2e/shutdown` | Every HTTP 202 → row in `events` |
| Budget invariant | `AssertBudgetInvariant` after chaos A/C/F |
| Escape analysis | `-gcflags="-m"` on touched hot files — no new heap escapes |

Detail: [SHIPPED.md](./SHIPPED.md) §M1, [REMEDIATION.md](./REMEDIATION.md).

---

### M2 — Admin API & Invoicing `Shipped`

**Goal:** Invoice chain + JSON `/api/v1` cold path for external admin panel.

#### M2.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R1b `adminapi`; R3 DTO; R8.2/R8.6 cold; MICRO billing 11/18 |
| **Binaries** | `cmd/billing`, `cmd/management`, `cmd/notifier` |
| **Packages** | `internal/adminapi` flat; `billing_handlers.go`, `ops_recon.go` tags |
| **Patterns** | Outbox-only Redis mutations; `FanOutCollector`; `pkg/cold.PaginatedList` |
| **SLA** | `GenerateInvoice` gRPC p99 < 2 s; fan-out GET p99 < 1.5 s |
| **Code zone** | **Cold only** — **FORBIDDEN** `internal/ingestion` import in `adminapi` |
| **Chaos R10** | Settlement/payment existing suites only; **no chaos** for read-only JSON (R10 #3) |

#### M2.1 Implementation

| Area | Pattern |
| :--- | :--- |
| Money truth | Invoice from `balance_ledger` aggregates only; `billing` schema isolated |
| Mutations | HTTP → PG txn + `outbox_events`; workers push Redis |
| Fan-out | Parallel shard poll; `partial: true` on partial failure |
| DTO boundary | `db.Row` → `toFooDTO()` one step; `json` tags only on DTOs (R3) |
| Idempotency | `SHA256(customer_id + canonical_json(body))` on mutating POST |

#### M2.2 Testing

```bash
go test ./internal/adminapi/... ./internal/billing/... -short
make lint
```

| Test | Criterion |
| :--- | :--- |
| Invoice idempotency | Re-run `GenerateInvoice` → same `invoice_id` |
| Ledger invariant | `CheckLedgerBalanceInvariant` → notifier on fail |
| Fan-out partial | One shard down → 200 + `partial: true` |
| No hot import | `go list -deps ./internal/adminapi` excludes `ingestion` |

Detail: [ADMINISTRATIVE.md](./ADMINISTRATIVE.md).

---

### M3 — Commercial Platform `Shipped (core)`

**Goal:** Product license JWT, tenant subscriptions, entitlements on hot path.

#### M3.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | `cmd/license-server` (vendor); `internal/licensing` library |
| **Hot path** | `filterRejectLicenseExpired` — local `atomic.Value` JWT snapshot; **zero network** on `/track` |
| **Cold path** | `LicenseWatcher`; `VolumeMeterWorker`; `UPDATE_ENTITLEMENTS` outbox |
| **Patterns** | `min(license.limits, subscription.limits)`; RPD `ingress:day:{customer}:{date}` |
| **Chaos R10** | `scripts/chaos-drills/m3/` — grace, expired JWT, spool |

#### M3.1 Implementation

| Component | How |
| :--- | :--- |
| License verify | Ed25519 JWT; `LicenseWatcher` refreshes cold; tracker reads snapshot |
| Entitlements | Outbox → Redis shard 0; UDP `max_rps` from plan |
| Usage | `usage_meters` hourly; overage on invoice line |
| Self-serve | `/api/v1/selfserve/*`; API key via auth gRPC |

**Open tail:** M3-T (PU packaging), M6-W (reports). Detail: [LICENSING.md](./LICENSING.md), [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md).

---

### M5 — Edge Compliance & eBPF `Shipped`

**Goal:** Defensive perimeter only; allowlist before block; audit trail.

#### M5.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | `GUIDE_COMPLIANCE.md` §1–§2 primary |
| **Binaries** | `cmd/edge-xdp`, `cmd/edge-bpf-sync`; **veto** ebpf in tracker/management |
| **Patterns** | management → outbox → Redis → `edge-bpf-sync` → pinned maps |
| **Edge C** | Explicit `data + sizeof(hdr) <= data_end`; `#pragma unroll`; per-CPU maps for 512-byte stack limit |
| **Chaos R10** | Allowlist integration tests; **no** full compose chaos for compliance grep |
| **CI** | `scripts/ci/check_compliance.sh` mandatory |

#### M5.1 Implementation

| ID | Requirement |
| :--- | :--- |
| CMP-EBPF-01 | `allowlist.IsProtected(ip)` before any block |
| CMP-EBPF-05 | `edge_block_audit` same PG txn as blacklist |
| CMP-FORB-04 | CI grep: no `cilium/ebpf` in management/tracker |
| XDP | LPM allow before LPM block; `XDP_DROP` at wire rate |

Detail: [SHIPPED.md](./SHIPPED.md) §M5, [EDGE.md](./EDGE.md) Part V.

---

## 4. Upcoming Milestones (execution order)

---

### M9 — CLI Installer & Preflight `Tier S` `Exec #1`

**Goal:** `espx-install` — preflight, provision, configure, apply, doctor.

#### M9.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R4 (`main` wiring only); MICRO ~5/18 |
| **Binaries** | `cmd/installer` — **no business logic in `main`** |
| **Packages** | `internal/installer` flat: `preflight.go`, `provision.go`, `profile.go`, `render.go` |
| **Patterns** | Idempotent `apply`; subprocess to existing scripts (`paths.sh`); secrets once |
| **SLA** | `preflight` < 30 s; repeat `apply` no-op |
| **Code zone** | Cold CLI; table-driven PF-* checks |
| **Chaos R10** | **Not required** (R10 #8 helper CLI) |
| **CI gates** | `go test ./internal/installer/... -short`; golden render files |

#### M9.1 Functional specification

| Command | Behavior |
| :--- | :--- |
| `preflight [--strict] [--json]` | PF-KERNEL ≥ 6.1, PF-BTF, PF-NIC, PF-LIBS, PF-PORTS, PF-ULIMIT, PF-SYSCTL |
| `provision [--yes]` | OS package install from `packages.yaml`; no `dist-upgrade` |
| `configure [--interactive]` | Wizard → `install.yaml` (profile + feature flags) |
| `apply [--dry-run]` | Render systemd / compose / k8s; write `/etc/espx/secrets.env` chmod 600 |
| `doctor [--json]` | Wrap `check_deps.sh` + topology probes |
| `license install\|activate\|status` | M3 licensing integration |

**Profiles:** `single_vps` | `compose_dev` | `k8s_k3s`. Flags: `edge_xdp` (needs M5 + PF-BTF), `multi_region` (M7 Enterprise), `telemetry_enabled: false`.

#### M9.2 Implementation patterns

- **R4:** `main.go` only binds config + `installer.NewCLI().Run()`.
- **R2:** `preflight_linux.go`, `render_systemd.go`; tests `preflight_test.go` with fake sysfs.
- **No duplicate logic:** invoke `scripts/local-dev/dev_stack.sh`, `scripts/k8s/install_k3s.sh` via pinned paths.
- **Idempotency:** `apply` compares checksum of rendered templates; skip if unchanged.
- **Validation:** `InstallProfile.Validate()` — `edge_xdp` without BTF → error at configure time.

#### M9.3 Testing

| Test | Criterion |
| :--- | :--- |
| Table-driven PF-* | Mock `/sys/kernel/btf/vmlinux`, fake `ethtool` output |
| Profile validation | `k8s_k3s` without cgroup v2 → configure error |
| Golden render | systemd unit output matches `testdata/golden/` |
| JSON schema | `--json` preflight stable field names |
| Idempotent apply | Dry-run twice → identical diff |

```bash
go test ./internal/installer/... -short
go test ./internal/installer/... -run Preflight -short
```

Integration apply: manual VM job (does not block PR).

#### M9.4 DoD checklist

- [x] `cmd/installer` + `internal/installer` (preflight, provision, profile, render)
- [x] PF-KERNEL, PF-NIC, PF-BTF, PF-LIBS on Debian/Ubuntu
- [x] Wizard + idempotent `apply`; secrets in `/etc/espx/secrets.env`
- [x] `espx-install license install|activate|status`
- [x] Operator runbook in `deploy/installer/README.md`
- [x] Godoc on `PreflightCheck`, `InstallProfile` (R9)

---

### M6-W — Buyer Reports & Dashboards `Tier S` `Exec #2`

**Goal:** Demoable placement ROI / subid analytics for Pro tier.

#### M6-W.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R1b — flat `adminapi`; R3 DTO; R8.2 cold HTTP |
| **Binaries** | None — extend `internal/adminapi` only |
| **Files** | `reports_handlers.go`, `reports_types.go`, `reports_metrics.go`, `dashboards_*.go`, `views_*.go` |
| **Patterns** | `register.go` mount; tier gate from entitlements; `chquery` when M6 CHG ready |
| **SLA** | Report GET p99 < 2 s (CH governed); cursor pagination |
| **Chaos R10** | **Not required** (read-only JSON) |

#### M6-W.1 Functional specification

| Wave | Routes | Response |
| :--- | :--- | :--- |
| W1 | `GET /api/v1/reports/placements` | ROI by subid/zone; `freshness` object |
| W1 | `GET /api/v1/reports/keywords` | Keyword revenue drilldown (RSOC prep) |
| W2 | `GET /api/v1/dashboards/campaign/{id}` | Overview tiles |
| W3 | `GET/POST /api/v1/views` | Saved filter views (Enterprise) |
| — | `GET /api/v1/selfserve/usage` | Pro+ usage summary |

**DTOs:** `PlacementReportRowDTO`, `KeywordReportRowDTO` with `json` tags; CH rows → one-step `toPlacementReportRowDTO` (R3).

#### M6-W.2 Implementation

- **No `management` import** in `adminapi` (R1b #4).
- **Tier gate:** `RequireEntitlement("margin_guard")` or plan check per [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md).
- **CH queries:** use `database/chquery.Query` with `max_execution_time=10`, `readonly=1` when M6 CHG ships; until then stub `stale: true`.
- **Pagination:** `pkg/cold.PaginatedList`; opaque cursor base64.
- **Partial data:** return 200 with `freshness.stale=true` when CH lag > 5 min.

#### M6-W.3 Testing

```bash
go test ./internal/adminapi/... -run 'Reports|Dashboards|Views' -short
```

| Test | Criterion |
| :--- | :--- |
| Route registration | All handlers reachable via `register.go` integration test |
| Tier gate | Basic plan → 403 on Pro-only routes |
| DTO mapping | Table-driven `to*DTO` from fake CH rows |
| Freshness | `ch_lag_seconds` populated when CH mock lag injected |

#### M6-W.4 DoD checklist

- [x] `reports_*`, `dashboards_*`, `views_*` in `register.go`
- [x] Pro/Enterprise gates enforced
- [x] `freshness` on every CH-backed response
- [x] Godoc on exported DTOs (R9)

---

### M15 — S2S Postback Dispatcher `Tier M` `Exec #3` `Shipped`

**Goal:** Egress conversions to FB CAPI, Google, TikTok, custom S2S. Pro tier.

**Spec:** [IDEAS_MICROSERVICES_EXPANSION.md](./IDEAS_MICROSERVICES_EXPANSION.md) §3.1.

#### M15.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | STYLE R1 `internal/postback` flat; R8.2 workers; MICRO **13/18** → `cmd/postback-sender` |
| **Binaries** | `cmd/postback-sender`; gRPC optional later; HTTP admin via `adminapi` proxy |
| **Patterns** | Transactional outbox; idempotency hash; token-bucket per destination; DLQ |
| **SLA** | Ingest p99 unchanged; dispatch p99 < 5 s per event (cold); 5 retries + jitter |
| **Code zone** | **Cold only** — **veto** any postback HTTP from tracker |
| **Chaos R10** | **Required** — external timeout, 429, DNS fail; proof ingest unaffected |

#### M15.1 Functional specification

| Component | Implementation |
| :--- | :--- |
| **Outbox consumer** | Poll `outbox_events` `type=SEND_POSTBACK`; `SELECT FOR UPDATE SKIP LOCKED` |
| **Adapters** | `provider_facebook.go`, `provider_google.go`, `provider_tiktok.go`, `provider_webhook.go` |
| **Macro engine** | Pre-parse template once; per-event replace into `[]byte` buffer (no `fmt.Sprintf` in hot dispatch loop) |
| **Secrets** | OAuth tokens in PG encrypted column; refresh in worker |
| **Idempotency** | `SHA256(customer_id|click_id|event_type)` → `postback_dispatches` unique |
| **DLQ** | After 5 failures → `postback_dlq`; admin retry API |
| **RSOC events** | `impression`, `search`, `click` types; `param10` keyword passthrough |

**Admin API:**

| Method | Route |
| :--- | :--- |
| GET | `/api/v1/postbacks/config` |
| PUT | `/api/v1/postbacks/config/{campaign_id}` |
| GET | `/api/v1/postbacks/dlq` |
| POST | `/api/v1/postbacks/dlq/{id}/retry` |

#### M15.2 Implementation patterns

- **Outbox:** processor/management writes `SEND_POSTBACK` in same PG txn as conversion row.
- **HTTP client:** dedicated `http.Transport` per provider with max idle conns; rate limit via token bucket (`golang.org/x/time/rate`).
- **No reflection:** adapter registry as `map[string]PostbackAdapter` of concrete types.
- **PII egress:** hash email/phone SHA-256 before FB CAPI (document in handler).
- **Files:** `postback_sender_worker.go`, `provider_*.go`, `macro_engine.go`, `outbox_postback.go`.

#### M15.3 Testing

```bash
go test ./internal/postback/... -short
go test ./internal/postback/... -run Macro -short
```

| Test | Criterion |
| :--- | :--- |
| Macro substitution | Table-driven `{click_id}`, `{payout}` edge cases |
| Idempotency | Duplicate outbox event → single HTTP egress (httptest recorder) |
| Adapter contract | Mock FB/Google payloads match API schema |
| Chaos | `chaos_proof fault=postback_external_timeout ingest_p99_ok=true` |
| Chaos | `chaos_proof fault=postback_rate_limit_429 retried=true` |
| Load | 1000 concurrent dispatches — no tracker metric regression |

#### M15.4 DoD checklist

- [x] `cmd/postback-sender` + `internal/postback` flat package
- [x] FB CAPI, Google offline, TikTok, custom webhook adapters
- [x] DLQ + admin API
- [x] RSOC multi-event types
- [x] SUBSCRIPTIONS Pro gate
- [x] Migrations: `postback_dispatches`, `postback_dlq`
- [x] Chaos proofs; `go test -short` green

---

### M16 — Cost Sync & RSOC Revenue `Tier M` `Exec #4`

**Goal:** Buy-side spend + sell-side RSOC revenue into CH/PG for ROI.

**Spec:** IDEAS §3.2.

#### M16.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | MICRO **11/18** → `cmd/cost-sync`; STYLE cold workers |
| **Binaries** | `cmd/cost-sync` |
| **Patterns** | Cron + manual trigger; OAuth refresh; idempotent ingest keys |
| **SLA** | Hourly sync; manual run p99 < 120 s per network |
| **Chaos R10** | Duplicate report → no double ledger rows |

#### M16.1 Functional specification

| Connector | API | Granularity |
| :--- | :--- | :--- |
| Facebook Ads | Marketing API OAuth | campaign/adset/ad/subid |
| Taboola / Outbrain | Partner API | campaign/placement |
| Google Ads | OAuth offline | campaign/ad group |
| Tonic RSOC | token + secret | EPC daily + stats_by_country blend |
| System1 RSOC | auth key | hourly + 10-day final reconciliation |

| Component | Behavior |
| :--- | :--- |
| `CostSyncWorker` | Hourly cron; `pg_try_advisory_lock` single leader |
| `campaign_costs` table | `(customer_id, campaign_id, date, network, placement_id)` unique |
| Currency | ECB daily rates → `amount_micro BIGINT` |
| Reconciliation | `tracker_estimated_spend` vs API → balancing `balance_ledger` entry |
| CH rollup | Insert `cost_snapshots` / update MV source for M17 |

**Admin API:** `POST /api/v1/cost-sync/run`, credentials CRUD, sync history log.

#### M16.2 Implementation patterns

- **OAuth:** store refresh token in `payment` schema pattern; rotate in worker; never log tokens.
- **Batch PG:** `pgx.Batch` for line items; one txn per network per day.
- **RSOC blend:** Tonic pattern — intraday from `epc/daily`, adjustments from `rsoc/stats_by_country` after 10 days.
- **Files:** `cost_sync_worker.go`, `provider_facebook.go`, `provider_taboola.go`, `provider_tonic_rsoc.go`, `provider_system1_rsoc.go`.

#### M16.3 Testing

| Test | Criterion |
| :--- | :--- |
| Idempotency | Re-import same day → 0 new rows |
| OAuth refresh | Expired token → refresh → successful fetch (httptest) |
| Currency | EUR → USD micro-units correct rounding |
| RSOC fixture | Golden JSON from Tonic/System1 samples → expected CH rows |
| Chaos | `chaos_proof fault=cost_sync_duplicate_report ledger_balanced=true` |

```bash
go test ./internal/costsync/... -short
```

#### M16.4 DoD checklist

- [x] `cmd/cost-sync` + providers for FB, Taboola, Outbrain, Google
- [x] Tonic + System1 RSOC revenue sync
- [x] Admin credentials + manual trigger API
- [x] Unique constraints on cost line items
- [x] CH materialized view feed for placement stats

---

### M17 — Margin Guard & Placement Auto-Pauser `Tier M` `Exec #5`

**Goal:** Auto-pause losing subids/zones. Pro tier.

**Spec:** IDEAS §3.3.

#### M17.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | MICRO **12/18** → `cmd/margin-guard`; CHAOS partial |
| **Binaries** | `cmd/margin-guard` |
| **Patterns** | CH governed query → outbox `PAUSE_PLACEMENT` → Redis blacklist |
| **SLA** | Policy evaluation every 60 s; hot-path absorb < 5 s |
| **Hot path** | Tracker reads `blacklist:placement:{id}` — **no new code on gnet loop** if key already supported |
| **Chaos R10** | CH stale → no pause; CH down → worker idle |

#### M17.1 Functional specification

| Rule | Default | Configurable |
| :--- | :--- | :--- |
| Min sample | ≥ 50 clicks | per campaign |
| ROI floor | < −30% | per policy |
| Zero-conv streak | 100 clicks, 0 conv | per policy |

| Component | Flow |
| :--- | :--- |
| `MarginGuardWorker` | Every 60 s: `chquery` on `mv_placement_stats_hourly` |
| Evaluation | `profit = revenue - spend`; `roi = profit/spend * 100` |
| Action | `PAUSE_PLACEMENT` outbox → `HSET blacklist:placement:{id}` all shards |
| Allowlist | VIP placements excluded |
| Alerts | `notifier.SendNotification` with metrics snapshot |

**Admin API:** `/api/v1/margin-guard/policies`, `/activity`, `/overrides`.

#### M17.2 Implementation patterns

- **Stale guard:** if `freshness.ch_lag_seconds > 300` → skip evaluation cycle; metric `margin_guard_stale_skips_total`.
- **Sample gate:** do not pause until `clicks >= min_clicks` (prevent premature pause).
- **Outbox:** same pattern as campaign pause — management outbox worker fans to Redis.
- **No hot-path import:** margin-guard never imports `internal/ingestion`.

#### M17.3 Testing

| Test | Criterion |
| :--- | :--- |
| Rule engine | Table-driven ROI/trigger cases |
| Stale CH | Lag > 5 min → 0 pauses emitted |
| Outbox integration | Pause event → Redis key within 5 s (integration) |
| Allowlist | VIP subid never paused |
| Chaos | `chaos_proof fault=margin_guard_ch_stale no_false_pause=true` |

```bash
go test ./internal/marginguard/... -short
```

#### M17.4 DoD checklist

- [x] `cmd/margin-guard` worker
- [x] Policy CRUD + activity log
- [x] Notifier integration
- [x] Pro tier gate
- [x] Stale-data guard

---

### M6 — Day-2 Operations & Analytics Pipeline `Tier M` `Exec #6`

**Goal:** Production operability without tracker restart for config changes.

#### M6.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS R1/R8; STYLE R1b; MICRO batch in processor (7/18) |
| **Binaries** | Extend `processor`, `management`, `tracker` — no new `cmd/ch-janitor` |
| **Patterns** | `atomic.Value` registry COW; incremental warm; `chquery` governance |
| **SLA** | Config visible p99 < 5 s; `/healthz` 0 allocs/op; `/readyz` p99 < 10 ms |
| **Perf hacks (tracker)** | `UpdateAndWarmCampaign(id)` — no full `Sync()` on pub/sub; registry read 0 locks |
| **Chaos R10** | **Required** — spool recovery, incremental reload |

#### M6.1 Functional specification — Hot reload (`HR-*`)

| ID | Implementation | Perf |
| :--- | :--- | :--- |
| HR-PUB | `campaigns:update` payload UUID → `UpdateAndWarmCampaign(id)` | Avoid full PG catalog scan |
| HR-REG | `registry.go` `atomic.Value` snapshot | 0 locks on `GetCampaign`; no new `RWMutex` on read |
| HR-BL | Blacklist outbox → all 4 shards + edge signal | Lag p99 < 5 s |
| HR-KEYS | Lua hash tags `{campaign_id}` | CI cross-slot test |
| HR-WARM | `budget_warmer.go` single campaign | < 2 s; metric `ad_registry_warm_duration_seconds` |

#### M6.2 Functional specification — Health (`HC-*`)

| Endpoint | Process | Rule |
| :--- | :--- | :--- |
| `GET /healthz` | all | **No I/O**; liveness only |
| `GET /readyz` | tracker | PG + Redis shards via cached atomics |
| `GET /readyz` | processor | PG + CH + spool segments + stream lag |
| `GET /readyz` | management | PG + Redis |

**Tracker `/healthz`:** increment atomic counter only — 0 allocs/op (GO.md §2).

#### M6.3 Functional specification — CH & pipeline

| ID | Component |
| :--- | :--- |
| CHJ-* | `CHPartitionJanitor` in processor; `CH_RAW_RETENTION_DAYS` env |
| CHG-* | `internal/database/chquery`; `CH_READONLY_DSN`; `SETTINGS max_memory_usage, max_execution_time, readonly=1` |
| PIPE-* | `readyz` fails when spool > `CH_SPOOL_MAX_SEGMENTS`; unified `XLEN`/PEL metrics |

#### M6.4 Code style

- **Hot (HR-REG):** forbid `json.Unmarshal` on registry path; build `domain.Campaign` from `db` rows directly.
- **Cold (CHG):** all `adminapi/reports_*` and `service_campaign_stats` use `chquery.Query`.
- **Files:** `registry.go`, `budget_warmer.go`, `chquery.go`, `ch_partition_janitor.go`, `health.go`.

#### M6.5 Testing

```bash
go test ./internal/ingestion/... -run 'Registry|Health|Spool' -short
go test ./internal/database/... -run 'CHQuery|Partition' -short
go test ./internal/adminapi/... -run 'Freshness' -short
```

| Test | Criterion |
| :--- | :--- |
| HR-PUB | Single campaign pub/sub → only that id reloaded |
| HC-READY | Redis down → tracker `readyz` 503 |
| CHJ-DROP | Partition older than retention dropped in test CH |
| CHG-ERR | Heavy `GROUP BY` killed without CH OOM |
| Bench | `/healthz` handler 0 allocs/op |
| Chaos | `chaos_proof fault=clickhouse_outage_10m spool_recovered=true` |
| Chaos | `chaos_proof fault=registry_incremental_reload lag_p99_lt_5s=true` |

#### M6.6 DoD checklist

- [x] HR-PUB, HR-BL, HR-KEYS
- [x] `/healthz` + `/readyz` on tracker, processor, management
- [x] `CHPartitionJanitor` + retention API
- [x] `chquery` + `freshness` on all CH reports
- [x] Processor `readyz` spool/lag gates
- [x] K8s manifest migration to split probes

---

### M3-T — Commercial PU Packaging `Tier M` `Exec #7`

**Goal:** Hybrid volume licensing per [PROPOSALS.md](./PROPOSALS.md) ESPX-LP-2026-V1.

#### M3-T.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | Extend `management` workers + `internal/licensing` |
| **Hot path** | JWT claims → local `atomic.Value`; filter rejects on EXPIRED only |
| **Patterns** | Weighted billable events; grace period; file-based license mode |
| **Chaos R10** | License server unreachable → operate on last-known-good JWT |

#### M3-T.1 Functional specification

| Component | Behavior |
| :--- | :--- |
| JWT bands | S/M/L volume + flags: `openrtb_engine`, `ivt_ml_detector`, `ebpf_xdp_edge`, `ml_fraud_boost` |
| `VolumeMeterWorker` | Hourly CH rollup; weights: accepted=1.0, dedup_reject=0.1, ebpf_drop=0.0 |
| `usage_meters` | Append-only; invoice overage line |
| `usage_daily` | Optional flush worker |
| Ingress gates | RPS/RPD from JWT → UDP quota + Redis `ingress:day:*` |

#### M3-T.2 Testing

| Test | Criterion |
| :--- | :--- |
| Weighted rollup | Golden CH fixture → expected PU count |
| Grace | Expired JWT + grace → ingest continues |
| Hot path | License check 0 allocs/op (`BenchmarkFilterLicense`) |
| Chaos | `scripts/chaos-drills/m3/` proofs |

#### M3-T.3 DoD checklist

- [x] JWT tier S/M/L in `internal/licensing`
- [x] `VolumeMeterWorker` + weighted meters
- [x] Module flags enforced on feature workers
- [x] Documented in `LICENSING.md` + `PROPOSALS.md`

---

### M11 — Botnet Interval Scoring `Tier M` `Exec #8`

**Goal:** Timer-bot detection via inter-click variance.

#### M11.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | Extend `cmd/ivt-detector` only (7/18) |
| **Patterns** | Passive CH aggregation; outbox block via M5 allowlist |
| **Compliance** | `allowlist.IsProtected` before `BlockIPWithTTL` |
| **Chaos R10** | **Required** when auto-block enabled |

#### M11.1 Functional specification

| ID | Spec |
| :--- | :--- |
| IVT-INTERVAL | `stddevPop(Δt)` per /24; flag when $\sigma^2 < 0.005$ s², $N \ge 30$ |
| Query | Governed `chquery`; readonly CH user |
| Action | `SuspiciousFinder` → management HTTP `BlockIPWithTTL` (or gRPC when wired) |
| Metrics | `ivt_candidates`, `ivt_enqueued`, `ivt_backpressure` |

#### M11.2 Implementation

- **No active probe** — CH passive only (CMP-FORB-03).
- **Plugin:** implement `SuspiciousFinder` interface; register in `ivt_rule_registry.go`.
- **Post-M14:** group by `ip_hash` not raw `ip_address`.

#### M11.3 Testing

```bash
go test ./internal/ivtdetector/... -short
bash scripts/ci/check_compliance.sh
```

| Test | Criterion |
| :--- | :--- |
| Variance math | Synthetic Δt series → flag/no-flag |
| Allowlist | Protected IP never enqueued |
| Chaos | `chaos_proof fault=ivt_interval_autoblock allowlist_respected=true` |

---

### M12 — Ledger Delta Consolidation `Tier L` `Exec #9`

**Goal:** ≤1 PG txn per campaign per 10 s.

#### M12.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | Extend `processor` SyncWorker |
| **Patterns** | In-memory rollup map; H1 single writer preserved |
| **Chaos R10** | **Required** — concurrent sync + chaos |

#### M12.1 Implementation

| Component | How |
| :--- | :--- |
| Rollup map | `map[campaignID]int64` flushed every 10 s; pre-sized buckets |
| Txn | `balance_ledger` + `UpdateSpend` + audit in one `pgx.Tx` |
| Zero-balance | Pause campaign on partial failure; metric `ledger_batch_pause_total` |
| **Hot path** | Unchanged — rollup is cold SyncWorker only |

#### M12.2 Testing

| Test | Criterion |
| :--- | :--- |
| Batch correctness | 100 deltas → 1 ledger row |
| Invariant | `AssertBudgetInvariant` after flush |
| Chaos | `chaos_proof fault=ledger_batch_pg_outage rollup_retained=true` |

---

### M13 — ClickHouse Lifecycle Advanced `Tier L` `Exec #10`

**Goal:** ZSTD recompress, emergency drop beyond M6 CHJ.

#### M13.1 Implementation

- Extends M6 `CHPartitionJanitor` — same binary.
- Off-peak `ALTER TABLE ... MATERIALIZE`; monitor `system.parts`.
- Emergency: `CH_EMERGENCY_DROP_PERCENT` policy with ops alert.

#### M13.2 Testing

```bash
go test ./internal/database/... -run 'Partition|OffPeak|Emergency|CHPartitionJanitor' -short
go test ./internal/database/... -run 'CHPartitionJanitor' -timeout 10m  # integration CH
go test ./internal/management/... -run 'CHEmergency' -short
```

| Test | Criterion |
| :--- | :--- |
| Recompress | Integration on test CH |
| Emergency | Disk threshold triggers drop + notifier |

#### M13.3 DoD checklist

- [x] Off-peak `OPTIMIZE FINAL` on fragmented partitions (`system.parts`)
- [x] ZSTD codec migration on raw `payload` columns
- [x] `CH_EMERGENCY_DROP_PERCENT` policy + `OpsAlerter` broadcast
- [x] `ad_ch_disk_used_percent` + janitor counters
- [x] Processor wiring + graceful shutdown `Wait()`

---

### M8 — Crypto Gateway `Tier L` `Exec #11`

**Goal:** USDT top-ups via [CRYPTO_GATEWAY.md](./CRYPTO_GATEWAY.md).

#### M8.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | Extend `cmd/payment` |
| **Patterns** | Same outbox → settlement gRPC as Stripe |
| **Secrets** | Webhook HMAC; isolated payment schema |
| **Chaos R10** | Duplicate webhook → idempotent credit |

#### M8.1 Implementation

| Component | Behavior |
| :--- | :--- |
| `CryptoProvider` | TRC20/ERC20; confirmation depth config |
| Webhook | Verify signature; `webhook_events` idempotency |
| Settlement | `ApplyPaymentCredit` → `balance_ledger` |
| Hold | 14-day hold + fraud gate before release |

#### M8.2 Testing

| Test | Criterion |
| :--- | :--- |
| Webhook replay | Same tx_hash → one credit |
| Underpay | Reject below minimum |
| Chaos | `chaos_proof fault=crypto_webhook_storm idempotent=true` |

---

### M4 — Shard Orchestrator & Elastic Triplets `Tier XL` `Exec #12`

**Goal:** Dynamic Redis scale beyond N=4 StaticSlot.

#### M4.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Guides** | CHAOS full M4 matrix (90+ scenarios); CONCEPTS false-sharing |
| **Binaries** | `management` orchestrator worker; tracker UDP control |
| **Hot path** | `atomic.Value` routing snapshot; `routing_epoch` fence in Lua |
| **Perf hacks** | Flat `[1024]uint8` slot table updates via atomic store; pad EWMA counters `_ [56]byte` |
| **Chaos R10** | **Required** — full M4 catalog |

#### M4.1 Topology

- **SO:** single writer `campaign_shard_assignment` PG; EWMA `H_ema`, `C_ema`.
- **Triplet:** Primary A/B 40/40 + Reserve R 20% canary.
- **Migration:** Fence → micro-batch COPY → epoch bump.

#### M4.2 Hot-path implementation

| Rule | Implementation |
| :--- | :--- |
| Linearizable debit | One home master per campaign per ms |
| Fencing | Lua: `if redis_epoch != routing_epoch then return fenced end` |
| Idempotency L1–L4 | click dedup → batch → migrate → orchestrator epoch |
| UDP control | TCP snapshot + HMAC + ACK (replace UDP-only cutover) |
| **Forbidden** | JumpHash / HybridBalancer on hot path (GAP-HOT-05) |

#### M4.3 Capacity scoring

$$C_{\text{ema}} \leftarrow \text{EWMA}(C_{\text{raw}}, 60\text{s})$$

Scale-out when $C_{\text{ema}} \ge 0.85$ for 300 s + quorum gate + cooldown 3600 s.

#### M4.4 Chaos matrix (minimum proofs)

| ID | Proof line |
| :--- | :--- |
| UDP-01 | `udp_loss_reorder` |
| UDP-11 | `udp_stale_fail_closed` |
| LUA-10 | `slot_migration_fence` |
| REDIS-10 | `sentinel_active_failover` |
| SO-01 | `orchestrator_no_false_migrate` |
| SO-02 | `campaign_routing_migration` |

Full catalog: [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) M4 extended.

#### M4.5 Testing

```bash
go test ./internal/ingestion/... -run 'Shard|Routing|Epoch' -short
go test ./tests/chaos/... -run Shard
./scripts/chaos-drills/test_chaos.sh
make test-alloc-gate
```

| Test | Criterion |
| :--- | :--- |
| Migration fence | Debit during COPY → fenced reject |
| Budget | ±1μ during SO-02 |
| Bench | `GetShard` still 0 allocs/op after routing table change |
| False sharing | Orchestrator EWMA fields padded |

---

### M7 — Multi-Region `Tier XL` `Exec #13`

**Goal:** Enterprise cells per [MULTI_REGION.md](./MULTI_REGION.md).

#### M7.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Patterns** | `RegionOutboxRelay`; cell-isolated Redis; global PG read models |
| **Hot path** | Per-region quota; no cross-region Redis Lua |
| **Chaos R10** | Chaos Kong game day (manual) |

#### M7.1 Implementation

| Component | Behavior |
| :--- | :--- |
| `RegionOutboxRelay` | Forward global events cell-to-cell |
| Quota | Per-region RPD in Redis + UDP |
| License | JWT `multi_region` + Enterprise subscription |
| Installer | M9 `multi_region: true` unblocked |

#### M7.2 Testing

| Test | Criterion |
| :--- | :--- |
| Cell isolation | Redis key in cell A invisible in cell B |
| Relay | Outbox event reaches remote cell |
| Game day | Documented runbook; manual chaos Kong |

---

### M18 — OpenRTB & Smart Pacing `Tier XL` `Exec #14`

**Goal:** Live RTB + conversion-weighted pacing for network operators.

#### M18.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Hot path** | `internal/rtb` — **full GO.md / .cursorrules** |
| **SLA** | `RunAuction` p99 < 15 µs; candidates < 500 |
| **License** | `openrtb_engine` JWT flag |
| **Chaos R10** | Auction + budget under Redis failover |

#### M18.1 Hot-path implementation

| Component | Perf requirement |
| :--- | :--- |
| `RunAuction` | SoA candidate layout; sorted array scan; `NoBidReason` not `error` |
| Candidate pool | Pre-allocated slice; no `append` without cap in loop |
| Catalog | `atomic.Value` RTB catalog snapshot |
| Pacing | `smart-pacer` cold cron → Redis multiplier keys |
| Thompson Sampling | Cold `lander-optimizer` pattern in management worker |

**Forbidden:** `interface{}` on bid loop; reflection; `defer` in auction scan.

#### M18.2 Testing

```bash
go test -benchmem ./internal/rtb/... -bench RunAuction
make test-alloc-gate
bash scripts/perf-gate/perf_gate_run.sh
```

| Test | Criterion |
| :--- | :--- |
| Bench | `RunAuction` p99 < 15 µs; 0 allocs/op |
| License gate | Flag off → 403 on RTB routes |
| Win/loss | Notification → CH row |
| Chaos | `chaos_proof fault=rtb_redis_failover nobid_graceful=true` |

---

### M10 — Vendor Telemetry `Tier S` `Exec #15`

#### M10.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Default** | `ESPX_VENDOR_TELEMETRY=0` |
| **Compliance** | TEL-RED — strip campaign_id, customer_id, IP, money labels |
| **Chaos R10** | Not required |

#### M10.1 Implementation

- Scrape `127.0.0.1` Prometheus on cold path only.
- Payload: Go runtime, gnet stats, eBPF drop ratios — **green zone only** per `GUIDE_COMPLIANCE.md` §8.
- `install.yaml` `telemetry_enabled: false` default.

#### M10.2 Testing

| Test | Criterion |
| :--- | :--- |
| Red team | Payload builder rejects forbidden labels |
| Default off | No egress when env unset |

---

### M14 — PII Anonymization `Tier L` `Exec #16`

#### M14.0 Standards envelope

| Dimension | Contract |
| :--- | :--- |
| **Binaries** | `processor` batch + `internal/privacy` — **no** `cmd/privacy-anonymizer` |
| **Hot path** | **Unchanged** — hash in processor CH batch only, not gnet |
| **Compliance** | `GUIDE_COMPLIANCE.md` §9 |

#### M14.1 Implementation

| ID | Spec |
| :--- | :--- |
| PII-01 | `HashPII(value, daySalt)` SHA-256 hex |
| PII-02 | Daily salt rotation in PG; processor reads snapshot |
| PII-03 | CH columns `ip_hash`, `ua_hash` FixedString(64) |
| PII-05 | `ErasureWorker` salt bump + CH mutation |

#### M14.2 Testing

```bash
go test ./internal/privacy/... -short
bash scripts/ci/check_compliance.sh
```

| Test | Criterion |
| :--- | :--- |
| Deterministic hash | Same input+day → same hash |
| Salt rotation | Concurrent insert + rotate → no panic |
| Hot path | Tracker bench unchanged (0 allocs) |

---

## 5. Execution Order & Dependencies

```text
SHIPPED: M1 → M2 → M3 (core) → M5 → M15

NEXT: M9 ∥ M6-W → M16 → M17 → M6

MID: M3-T → M11 → M12 → M13 → M8

SCALE: M4 → M7 → M18

TAIL: M10 → M14
```

| Dependency | Reason |
| :--- | :--- |
| M6-W before M17 | Placement UI before auto-pause |
| M15 before M16 | RSOC postback templates |
| M16 before M17 | Cost + revenue in CH |
| M6 CHG before heavy CH consumers | OOM protection |
| M4 after Arbitrage Ops | Scale does not block first sale |
| M14 after M11 | IVT queries use `ip_hash` |

---

## 6. Milestone ID Quick Index

| ID | Title | Tier | Status |
| :---: | :--- | :---: | :--- |
| M1 | Core Ingestion & Ledger | — | Shipped |
| M2 | Admin API & Invoicing | — | Shipped |
| M3 | Licensing & Subscriptions | — | Shipped (core) |
| M4 | Shard Orchestrator | XL | Backlog |
| M5 | Edge Compliance & eBPF | — | Shipped |
| M6 | Day-2 Operations | M | Planned |
| M6-W | Buyer Reports | S | **Next** |
| M7 | Multi-Region | XL | Shipped |
| M8 | Crypto Gateway | L | Backlog |
| M9 | CLI Installer | S | **Next** |
| M10 | Vendor Telemetry | S | Backlog |
| M11 | Botnet Interval | M | Backlog |
| M12 | Ledger Consolidation | L | Backlog |
| M13 | CH Lifecycle Advanced | L | Shipped |
| M14 | PII Anonymization | L | Backlog |
| M15 | S2S Postback | M | Shipped |
| M16 | Cost Sync & RSOC | M | Planned |
| M17 | Margin Guard | M | Planned |
| M18 | OpenRTB & Smart Pacing | XL | Backlog |
| M3-T | Commercial PU Packaging | M | Shipped |

---

## Appendix — Documentation Cross-References

| Document | Milestones |
| :--- | :--- |
| [GO.md](./GO.md) | M1, M4, M6 HR-*, M18 hot path |
| [CONCEPTS.md](./CONCEPTS.md) | M1 batching, M4 cache padding, M18 SoA |
| [IDEAS_MICROSERVICES_EXPANSION.md](./IDEAS_MICROSERVICES_EXPANSION.md) | M15–M17 |
| [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md) | All — R1/R8 zones |
| [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) | M1, M4, M6, M15, write paths |
| [GAPS.md](./GAPS.md) | Gap → milestone mapping |
