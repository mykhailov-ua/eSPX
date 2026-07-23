# Development Guide

Environment setup, engineering standards, tooling, and operational procedures.

---

## Requirements

*   Go 1.25+
*   Docker and Docker Compose
*   `buf` CLI (or `make proto`)

---

## Quick Start

```bash
cp .env.example .env
bash scripts/local-dev/dev_stack.sh build
bash scripts/local-dev/dev_stack.sh full
bash scripts/local-dev/dev_preflight.sh
```

`dev_stack.sh` modes:

| Mode | Contents |
| :--- | :--- |
| `infra` | Postgres, Redis ×6, ClickHouse |
| `full` | All services (trackers, processor, management, billing, …) |
| `sentinel` | Redis Sentinel topology |

---

## Engineering Standards

Normative rules for all changes. Hot-path detail: [GO.md](./GO.md). Architecture: [ARCHITECTURE.md](./ARCHITECTURE.md).

### Hot-path SLA

| Area | Target |
| :--- | :--- |
| Tracker handler | p95 < 50 ms, p99 < 80 ms, hard ceiling 100 ms |
| Redis unified-filter Lua | p99 < 10 ms per shard |
| Geo filter (sampled) | p99 < 10 µs |
| RTB `RunAuction` | p99 < 15 µs; p99 candidates scanned < 500 |
| Fraud boost in `FilterEngine` | 0 allocs/op on touched paths |

Load-test abort: control-cohort p99 > 80 ms for 30 s **or** budget invariant violation.

### Code zones

| Zone | Packages | Allocations | Errors |
| :--- | :--- | :--- | :--- |
| **Hot** | `internal/ingestion`, `internal/rtb` | 0 allocs/op on request path | `filterRejectKind`, `NoBidReason` |
| **Cold** | `management`, `adminapi`, `payment`, workers | Idiomatic Go | `errors.Is`, `writeServiceError` |
| **Edge** | `internal/edge`, `cmd/edge-*` | Kernel maps | Verifier-safe C |

**Forbidden on hot path:** `defer` in loops, closures in request loops, `interface{}` boxing, `sync.Map`, `fmt.Sprintf` / string `+` in loops, dynamic Prometheus labels.

### CI merge gates

```bash
go test ./... -short
make lint
bash scripts/ci/check_comments.sh
bash scripts/chaos-drills/test_chaos.sh      # write paths, outbox, Redis Lua
bash scripts/perf-gate/perf_gate_run.sh      # when internal/ingestion or internal/rtb touched
make test-alloc-gate                         # hot-path allocation regression
bash scripts/ci/check_compliance.sh          # edge/compliance grep
```

Chaos steady-state (R1): `/track` p99 < 80 ms; error rate < 0.1% (excl. valid rejects); budget drift within recon window.

Style and chaos matrices: `GUIDE_STYLE_CODE.md`, `GUIDE_CHAOS_RELIABILITY.md`.

---

## Slot migration (M1)

### COPY vs activation delta policy

Slot migration uses a **PG re-warm authoritative** cutover for budget keys:

1. **COPY** (`CampaignKeyMigrator`) — idempotent `DUMP`/`RESTORE` of hash-tagged campaign keys from `CampaignRedisKeyCatalog` (budget, quota, fcap, dedup, idempotency, rate-limit, impression timestamps, placement blocklists). Ephemeral keys may drift between COPY and cutover; that is acceptable.
2. **Fence** — when `MIGRATION_FENCE_ENABLED=true`, `BumpMigrationFences` sets `budget:migration_fence:{uuid}` on the **source** shard and increments `campaigns.migration_gen`. Lua returns code `11` (debit fenced).
3. **PG re-warm** — at activation, `RewarmCampaignBudgetKeys` seeds `{uuid}budget:campaign:{uuid}` on the **target** shard from Postgres `budget_limit - current_spend`. This is the cutover source of truth for spend counters.
4. **EXISTS gate** — activation rejects cutover when required keys are missing on the target after PG re-warm (`ErrSlotMigrationKeysMissing`).
5. **Epoch bump** — `ActivateSlotMapVersionWithMigration` sets `active_version`, reloads `StaticSlotSharder`, and publishes broker reload.
6. **Drain** — old-shard keys deleted after cutover.

Dual-write / lag catch-up (M1-08) is phase 2; fence + PG re-warm remains the default path.

### Rollback playbook

If a slot map activation causes routing or budget issues:

1. **Identify** active version and previous stable version: `GET /admin/slot-map` or `redis_slot_map_meta.active_version`.
2. **Rollback map**: `RollbackSlotMapVersion(ctx, adminID, previousVersion)` — reverts `active_version`, reloads sharder, publishes broker reload. Tracker traffic routes to the previous slot→shard mapping immediately.
3. **Target shard cleanup** (optional): for each campaign in rolled-back slots, `DrainCampaignKeys` on the **target** shard removes keys copied during the failed migration. Source shard keys may still exist if drain had not completed.
4. **PG re-warm source** (if source was drained): for affected campaigns, call admin `POST /admin/campaigns/{id}/warm-budget` or run `RewarmCampaignBudgetKeys` on the source shard from Postgres.
5. **Verify R5**: `VerifySlotMigrationR5` and `AssertBudgetInvariant` on a sample campaign per shard.
6. **Clear fences**: delete `budget:migration_fence:{uuid}` on any shard where copy left fence keys; confirm `migration_gen` in Postgres matches tracker registry.

Chaos coverage: `TestChaos_SlotMigrationRollbackAfterActivate`, `TestChaos_SO02_SlotMigrationPGRewarmCutover`, `TestChaos_LUA10_DebitFencedDuringSlotCopy`.

---

## K3s (Kubernetes)

### Cold path

Services in namespace `espx`. Databases stay in Docker Compose.

```bash
bash scripts/k8s/install_k3s.sh
bash scripts/k8s/k8s_cold_path_up.sh
```

### Hot path

Trackers and Nginx use `hostNetwork` for latency.

```bash
bash scripts/k8s/k8s_hot_path_up.sh
```

---

## Code Generation

| Command | Output |
| :--- | :--- |
| `make proto` | Protobuf (vtproto) in `internal/*/pb/` |
| `make gen` | sqlc queries in `internal/*/sqlc/` |

---

## Scripts (`scripts/`)

| Script | Purpose |
| :--- | :--- |
| `local-dev/dev_stack.sh` | Compose lifecycle |
| `local-dev/check_deps.sh` | Ports, migrations |
| `local-dev/smoke_local.sh` | Health check all services |
| `perf-gate/perf_gate_run.sh` | Benchmarks, zero-alloc gate |
| `chaos-drills/test_chaos.sh` | Fault injection suite |
| `edge-tuning/edge_nic_tune.sh` | NIC RX ring, IRQ |
| `redis-ops/` | Shard operations |
| `load-test/` | RPS load tests |

---

## Ports and Services

| Service | Port | Protocol |
| :--- | :--- | :--- |
| Nginx | 8180 | HTTP |
| Tracker | 8181–8184 | HTTP (gnet) |
| Processor | 8186 | HTTP |
| Management | 8188 | HTTP |
| Management gRPC | 51053 | gRPC |
| UDP control | 8190 → 8191 | UDP |
| Auth | 51051 | gRPC |
| Payment | 51052, 8187 | gRPC, HTTP |
| Billing | 51054 | gRPC |
| Notifier | 8085 | HTTP |
| Redis shards | 6479–6482 | TCP |
| PostgreSQL | 5430 | TCP |
| ClickHouse | 9000 | TCP |

---

## Environment Variables (Key)

Full list: `.env.example`. Cold-path durability: [CONCEPTS.md](./CONCEPTS.md) §10.

| Variable | Role |
| :--- | :--- |
| `DB_DSN` | Postgres connection |
| `REDIS_ADDRS` | Shard addresses |
| `FILTER_TIMEOUT_MS` | Filter deadline (100 ms prod) |
| `TTC_FAIL_CLOSED` | Reject clicks without impression timestamp |
| `TRACKER_PG_FALLBACK` | Off in production |
| `RTB_MODE` | `off` / `shadow` / `live` — see [RTB.md](./RTB.md) |
| `RTB_BUDGET_AUTHORITY` | `redis` or `rtb` |
| `RTB_TARGETING_INDEX` | Geo+device+category inverted index |
| `PROCESSOR_PG_GATE_SLOTS`, `PROCESSOR_CH_GATE_SLOTS` | Write concurrency (`0` = auto) |
| `CH_SPOOL_SEGMENT_MB`, `CH_SPOOL_MAX_SEGMENTS` | CH outage spool |
| `FRAUD_SCORING_ENABLED` | Cold-path ML workers |

---

## Testing

```bash
go test ./... -short                    # unit + short integration
go test ./internal/rtb/... -bench=BenchmarkAuction -benchmem -run='^$'
go test ./tests/e2e/... -count=1        # full stack (slow)
EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit
```

Hot-path change checklist:

1. `Benchmark*` — 0 allocs/op  
2. `go test -gcflags="-m"` — no escape on hot functions  
3. `go tool objdump` — no `panicIndex` in inner loops (BCE)  
4. Chaos tests if write path or Lua changed  

---

## Anti-Fraud Operations

**Emergency shutdown:** `FRAUD_SCORING_ENABLED=false`; restart `fraud-scorer` / `ivt-detector`.

**Manual corrections:**

* Reset campaign boost — management API → `ML_SCORE_BOOST` outbox  
* Unblock IP — remove from `ip_blacklist` + `UPDATE_BLACKLIST` outbox  

Actions recorded in `audit_logs`.

---

## Documentation Map

| Topic | Document |
| :--- | :--- |
| System design | [ARCHITECTURE.md](./ARCHITECTURE.md) |
| Open gaps | [GAPS.md](./GAPS.md) |
| RTB | [RTB.md](./RTB.md) |
| Redis / Lua | [REDIS.md](./REDIS.md) |
| Databases | [DATABASE.md](./DATABASE.md) |
