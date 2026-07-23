# eSPX Architecture

Subsystem topology, data flows, operational contracts, and platform capabilities. For local setup and CI, see [DEVELOPMENT.md](./DEVELOPMENT.md). For open work, see [GAPS.md](./GAPS.md).

## Documentation Navigation

| Document | Description |
| :--- | :--- |
| [CONCEPTS.md](./CONCEPTS.md) | Syscalls, memory, network, DOD, mathematical models |
| [GO.md](./GO.md) | Go hot path: gnet, zero-alloc, BCE, atomics, branch prediction |
| [RTB.md](./RTB.md) | In-process RTB auction, rollout modes, roadmap |
| [EDGE.md](./EDGE.md) | Ingress (L4/L7), UDP control, eBPF compliance |
| [REDIS.md](./REDIS.md) | Shard topology, Lua scripts, slot migration |
| [DATABASE.md](./DATABASE.md) | PostgreSQL, ClickHouse, durability, idempotency |
| [MANAGEMENT.md](./MANAGEMENT.md) | Admin API, outbox, pacing, settlement gRPC |
| [ADMINISTRATIVE.md](./ADMINISTRATIVE.md) | `adminapi` JSON surface, billing exports |
| [LICENSING.md](./LICENSING.md) | JWT volume bands, module flags, license server |
| [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md) | Plans, entitlements, RPD gates |
| [MULTI_REGION.md](./MULTI_REGION.md) | Regional cells, outbox relay |
| [REMEDIATION.md](./REMEDIATION.md) | Write-path remediation, semaphores, spool |
| [GAPS.md](./GAPS.md) | Known gaps and forward priorities |
| [PROPOSALS.md](./PROPOSALS.md) | Optional architectural proposals |

---

## Platform Overview

eSPX is an event-stream pacing platform for ad networks and arbitrage operators. The hot path accepts `/track` events, applies filters and atomic budget rules, and enqueues settlement. Money is authoritative in PostgreSQL; Redis holds fast edge state; ClickHouse holds telemetry.

**Design split:**

| Path | Latency budget | Packages | Persistence |
| :--- | :--- | :--- | :--- |
| **Hot** | `/track` p99 < 80 ms | `internal/ingestion`, `internal/rtb` | Redis Lua + streams |
| **Cold** | Seconds–minutes | `management`, `adminapi`, workers | Postgres outbox → Redis |
| **Edge** | Line-rate drop | `internal/edge`, `cmd/edge-*` | BPF maps, blocklists |

---

## Services and Binaries

| Binary | Port(s) | Role |
| :--- | :--- | :--- |
| `tracker` | 8181–8184 | gnet ingest, `FilterEngine`, optional in-process RTB, tiered Lua |
| `processor` | 8186 | Per-shard Redis streams → PG/CH; `SyncWorker`; CH mmap spool; CH janitor |
| `management` | 8188, 51053 | Admin HTTP/HTMX, settlement gRPC, outbox, recon, quota, UDP control :8190 |
| `auth` | 51051 | gRPC: Argon2id, PASETO, sessions, API keys |
| `payment` | 51052, 8187 | Payment intents gRPC, Stripe webhooks, settlement outbox |
| `billing` | 51054 | Invoice generation from `balance_ledger` |
| `notifier` | 8085 | Telegram, Slack, SMTP, SMS |
| `license-server` | vendor | Issue/renew/revoke license JWT (vendor DC) |
| `ivt-detector` | — | CH batch → fraud blacklist via management outbox |
| `fraud-scorer` | — | ML scoring → management outbox (optional compose profile) |
| `postback-sender` | — | S2S postback dispatch worker |
| `cost-sync` | — | External cost API sync (arbitrage ops) |
| `margin-guard` | — | Placement margin monitor → auto-pause via outbox |
| `edge-xdp`, `edge-bpf-sync` | — | Defensive XDP + BPF map sync from Redis |
| `installer` | — | `espx-install` CLI preflight and bootstrap |
| `broker`, `log-shipper`, `log-compactor`, `log-evacuator` | — | Optional mmap log pipeline |
| `admin`, `dlq` | — | Operator CLIs |

**Library deployables (no separate `cmd/`):** `internal/adminapi` (`/api/v1` JSON), `internal/licensing`, `internal/billing`, `internal/rtb`, `internal/edge/{allowlist,blocklist}`.

---

## Topology

Five layers. Application services use **host networking** in production k8s hot path; databases typically run on bridge/compose network with published ports.

```text
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│ Nginx :8180 │────▶│ Tracker x4   │────▶│ Redis x4 shards │
│ edge Lua    │     │ gnet + RTB   │     │ Lua + streams   │
└─────────────┘     └──────┬───────┘     └────────┬────────┘
                           │                      │
                    ┌──────▼───────┐       ┌──────▼────────┐
                    │  Processor   │       │  Management   │
                    │  PG + CH     │◀──────│  outbox/gRPC  │
                    └──────┬───────┘       └───────────────┘
                           │
              ┌────────────┴────────────┐
              │ PostgreSQL  │ ClickHouse │
              └─────────────────────────┘
```

1. **Ingress (Nginx :8180).** `/admin/*` → management; `/track/*` → trackers. OpenResty: rate limits, edge blacklist, Redis shard hint (CRC32).
2. **Ingestion (Tracker).** `PinnedWorkerPool` by campaign hash; shared `processTrack()`.
3. **Edge state (Redis x4).** Standalone masters + Sentinel. Client-side `StaticSlotSharder` (not Redis Cluster).
4. **Application.** Processor, management, gRPC services, cold-path workers.
5. **Persistence.** PostgreSQL 16 (ledger, config, outbox); ClickHouse 24 (telemetry, ML features).

---

## Data Plane: Request Path

```text
Ingress → Tracker.parse → ensureIngestGeo
       → [RTB if enabled] → FilterEngine.Check → Lua unified-filter (if not skipped)
       → stream XADD → HTTP response
```

### Filter order (`FilterEngine.Check`)

1. Emergency breaker  
2. Fraud / geo (MaxMind, fail-open where configured)  
3. Schedule, placement, L3, device, consent  
4. ML fraud boost (snapshot, 0 allocs)  
5. `UnifiedFilter` → Redis `EVALSHA` (budget, fcap, TTC, dedup, rate)

### RTB integration

When `RTB_MODE` is not `off`, an in-process auction runs **before** `FilterEngine` (see [RTB.md](./RTB.md)):

| Mode | Behavior |
| :--- | :--- |
| `shadow` | `RunAuctionEval`; metrics only; client `campaign_id` kept |
| `live` | `RunAuction`; winner replaces `campaign_id`; optional RTB budget authority |

Budget authority `redis` (default) keeps Lua as spend owner; `rtb` skips Lua budget debit and uses in-process `BudgetStore` with reconcile sampling.

### Redis Lua

| Script | Use |
| :--- | :--- |
| `budget-fast.lua` | Impressions: debit + stream |
| `unified-filter.lua` | Clicks: full validation + debit + stream |

Single `EVALSHA` per event when possible. p99 target < 10 ms per shard.

### Sharding (current production)

```text
slot  = crc32_castagnoli(campaign_id) & 1023
shard = slot_table[slot]   // StaticSlotSharder, default 4 masters
```

Shard 0 convention: pub/sub `campaigns:update`, auth lockout, brand creatives. Global keys replicated via outbox to all shards.

**Future:** elastic shard orchestrator (see [GAPS.md](./GAPS.md) §Sharding).

---

## Settlement Pipeline

### Stream processing (`cmd/processor`)

Per-shard consumer groups on `ad:events:stream`:

| Group | Role |
| :--- | :--- |
| PG | `events` partitions, `campaign_stats`, `sync_idempotency` |
| CH | Batch insert; mmap spool on CH outage |
| Fraud | Forward to fraud stream + CH |

`XAck` only after durable write (PG commit or CH spool `fsync`). Write gates: `ProcessorPgGate`, `ProcessorChGate`.

### Budget sync (`SyncWorker`)

1. Lua: move `budget:sync` → `inflight` under lock  
2. Postgres: `UpdateSpend` (ledger batch, coefficient N=32)  
3. Lua: commit — subtract confirmed amount from Redis  

Sub-millisecond debits in Redis; Postgres latency isolated to background.

### Transactional outbox

Postgres change + `outbox_events` in one transaction → `OutboxWorker` polls (`SKIP LOCKED`, adaptive interval) → Redis pipeline on all shards.

---

## Control Plane (`cmd/management`)

| Concern | Mechanism |
| :--- | :--- |
| Campaign registry | Postgres + pub/sub reload |
| Pacing | `PacingControllerWorker` → Redis via outbox |
| Schedule | Campaign status by time window |
| Quota | `QuotaManager` — PG chunks → `budget:quota` |
| Slot migration | Orchestrator + `migration_fence` |
| Reconciliation | `ReconWorker` — PG vs Redis budget drift |
| RTB deals | `/admin/rtb/deals`, floor optimizer, catalog pubsub |
| UDP ingress | `:8190` epoch broadcast to trackers |
| Settlement gRPC | Payment credits, fraud threats, ML boosts |

---

## Fraud and IVT

ML scoring stays off the tracker hot path. Trackers read precomputed Redis keys only.

```text
Tracker → fraud stream (lossy)
Processor → ClickHouse ml_features_1m
ivt-detector / fraud-scorer → management outbox → all Redis shards
```

`fraud-scorer` must not be imported from `internal/ingestion`.

---

## Billing and Payments

| Store | Role |
| :--- | :--- |
| `balance_ledger` (Postgres) | Sole money truth |
| `cmd/payment` | Stripe, webhooks, PCI-isolated schema |
| `cmd/billing` | Invoices from ledger aggregates |

Payment → management settlement gRPC for customer credit.

---

## Licensing and Commercial

Hybrid volume licensing (JWT bands S/M/L, module flags, weighted billable events). Hot path: `LicenseFilter` + `EntitlementsFilter` read atomic snapshots. Cold path: `VolumeMeterWorker` rolls up ClickHouse → `usage_meters`.

| Module flag | Effect |
| :--- | :--- |
| `openrtb_engine` / `rtb_live` | Reject `bid`/`rtb` events when off |
| `ml_fraud_boost` | Processor fraud micro-batcher |
| `ivt_ml_detector` | `ivt-detector` startup gate |
| `ebpf_xdp_edge` | Edge BPF sync |

Detail: [LICENSING.md](./LICENSING.md), [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md).

---

## Edge and Compliance

Defensive XDP filter, allowlist gate, blocklist sync, audit to Postgres. Regulatory scope: [EDGE.md](./EDGE.md) Part V, `GUIDE_COMPLIANCE.md`.

---

## Observability and SLAs

| Metric / area | Target |
| :--- | :--- |
| `ad_http_request_duration_seconds` (tracker) | p95 < 50 ms, p99 < 80 ms, ceiling 100 ms |
| Redis unified-filter Lua | p99 < 10 ms / shard |
| Geo filter (sampled) | p99 < 10 µs |
| `RunAuction` | p99 < 15 µs; candidates scanned p99 < 500 |
| Fraud boost snapshot | ~90 ns, 0 allocs/op |
| Budget invariant | `current_spend ≤ budget_limit` (±1 micro-unit) |

Grafana dashboards under `deploy/monitoring/grafana/`. RTB cutover: `rtb.json`.

---

## Engineering Constraints

Hot-path rules override style guides when they conflict:

- 0 heap allocations per request on parse, filter, auction  
- No `defer`, closures, `interface{}`, `sync.Map`, or string `+` in hot loops  
- Monotonic deadlines (`FilterDeadlineMono`); no wall clock in filter loops  
- Pre-bound Prometheus labels on hot path  

CI: `make test-alloc-gate`, `scripts/perf-gate/`, `scripts/chaos-drills/test_chaos.sh` when write paths change.

Full reference: [GO.md](./GO.md), `.cursorrules`.

---

## Optional Log Broker

`cmd/broker` — mmap segment log for regional resilience; not a Kafka replacement. Used with log shipper/compactor/evacuator for S3 evacuation.
