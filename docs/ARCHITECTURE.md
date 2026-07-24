# Architecture

Platform topology, request flow, and SLA contracts. Scope stops at system boundaries; module detail lives in linked documents.

| Document | Scope |
| :--- | :--- |
| [DATA.md](./DATA.md) | Redis, PostgreSQL, ClickHouse |
| [GO.md](./GO.md) | Tracker hot path |
| [EDGE.md](./EDGE.md) | Nginx/OpenResty ingress |
| [EBPF.md](./EBPF.md) | XDP L4 filter |
| [RTB.md](./RTB.md) | In-process auction |
| [MANAGEMENT.md](./MANAGEMENT.md) | Control plane, auth, licensing |
| [CAPABILITIES.md](./CAPABILITIES.md) | Shipped milestones |
| [BACKLOG.md](./BACKLOG.md) | Open gaps |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | Local setup, CI, runbooks |
| [STYLE.md](./STYLE.md) | Repository layout and code rules |
| [CHAOS.md](./CHAOS.md) | Fault injection and invariants |
| [COMPLIANCE.md](./COMPLIANCE.md) | Defensive vs forbidden edge behavior |
| [BOUNDARIES.md](./BOUNDARIES.md) | Microservice vs in-process decisions |

---

## Overview

eSPX ingests ad events on `/track`, applies filters and atomic budget rules, and enqueues settlement. PostgreSQL holds financial truth; Redis holds hot state; ClickHouse holds telemetry.

| Path | p99 budget | Packages | Persistence |
| :--- | :--- | :--- | :--- |
| Hot | `/track` < 80 ms | `internal/ingestion`, `internal/rtb` | Redis Lua, streams |
| Cold | seconds–minutes | `management`, workers | Postgres outbox → Redis |
| Edge | line-rate drop | `internal/edge`, `cmd/edge-*` | BPF maps, blocklists |

---

## Services

| Binary | Port(s) | Role |
| :--- | :--- | :--- |
| `tracker` | 8181–8184 | gnet ingest, `FilterEngine`, RTB, Lua |
| `processor` | 8186 | Stream consumer → PG/CH; `SyncWorker`; CH spool |
| `management` | 8188, 51053 | Admin HTTP, settlement gRPC, outbox, recon |
| `auth` | 51051 | gRPC: Argon2id, PASETO, API keys |
| `payment` | 51052, 8187 | Stripe webhooks, settlement outbox |
| `billing` | 51054 | Invoices from `balance_ledger` |
| `notifier` | 8085 | Alerts |
| `ivt-detector`, `fraud-scorer` | — | CH batch → management outbox |
| `edge-xdp`, `edge-bpf-sync` | — | XDP filter, BPF map sync |
| `broker`, `log-shipper`, … | — | Optional mmap log pipeline |

Libraries without `cmd/`: `internal/adminapi`, `internal/licensing`, `internal/billing`, `internal/rtb`.

---

## Topology

```text
Nginx :8180 → Tracker ×N → Redis ×4 (Lua, streams)
                ↓              ↑
           Processor → PG, CH   Management (outbox)
```

1. Ingress: Nginx :8180; `/admin/*` → management; `/track/*` → trackers. See [EDGE.md](./EDGE.md).
2. Ingestion: `PinnedWorkerPool` by `campaign_id` hash. See [GO.md](./GO.md).
3. Redis: 4 standalone masters, client-side `StaticSlotSharder`. See [DATA.md](./DATA.md) Part I.
4. Settlement: per-shard `ad:events:stream` → processor. See [DATA.md](./DATA.md) Part IV.
5. Persistence: PostgreSQL 16, ClickHouse 24.

Production k8s hot path uses host networking; databases typically on compose bridge with published ports.

---

## Request path

```text
Ingress → parse → ensureIngestGeo
       → [RTB if RTB_MODE≠off] → FilterEngine.Check → Lua (or local quanta)
       → XADD → HTTP response
```

### FilterEngine order

1. Emergency breaker  
2. Fraud / geo (MaxMind; fail-open where configured)  
3. Schedule, placement, L3, device, consent  
4. ML fraud boost (Redis snapshot; 0 allocs/op)  
5. `UnifiedFilter` or `TrySpendLocal` + `budget-fast.lua` with `skip_budget=1` when [M8 local quanta](./CAPABILITIES.md#m8--local-budget-quanta) applies

### RTB hook

| `RTB_MODE` | Behavior |
| :--- | :--- |
| `off` | Skip |
| `shadow` | `RunAuctionEval`; metrics only |
| `live` | `RunAuction`; winner replaces `campaign_id` |

Detail: [RTB.md](./RTB.md).

### Sharding (default)

```text
slot  = crc32(campaign_id) & 1023
shard = slot_table[slot]   # 4 masters, StaticSlotSharder
```

Elastic triplets: opt-in. [DATA.md](./DATA.md) Part I §7.

---

## Settlement

| Stage | Mechanism |
| :--- | :--- |
| Stream | `ad:events:stream`; consumer groups PG / CH / fraud |
| Ack rule | `XAck` after PG commit or CH spool `fsync` |
| Budget sync | `SyncWorker`: Redis dirty set → PG `UpdateSpend` → Redis commit |
| Outbox | PG txn + `outbox_events`; `SKIP LOCKED` workers → Redis |

Write-path durability: [DATA.md](./DATA.md) Part IV.

---

## Fraud path

```text
Tracker → fraud stream (MPSC: 512 critical + 3584 analytical, M14)
       → processor → CH
ivt-detector / fraud-scorer → management outbox → Redis shards
```

- Critical lane (L1 reject, L3 blocklist): never aggregated; short spin then drop.
- Analytical lane: adaptive /24 aggregation at ≥80% fill (M11).
- Consumer lag > `FRAUD_CONSUMER_LAG_SEC`: tracker widens aggregation windows (`aggregating=force`, M14).

`internal/ingestion` must not import `internal/fraudscoring`.

---

## Resilience (M14)

| Fault | Behavior |
| :--- | :--- |
| Shard-0 pub/sub down | Stale-serve known campaigns; `503 registry_stale` for unknown; optional broker `campaigns:update` fallback |
| Shard-0 ingest outage | `503 shard_unavailable` or M2 triplet reroute; shards 1–3 unaffected |
| Deep JSON / hostile H2 | Reject at parse (`MaxJSONDepth`); close after `H2_INCOMPLETE_MAX` incomplete spins |
| Campaign pause / tracker SIGTERM | Flush unused local quanta to Redis + broker return delta |
| Fraud ring storm | Critical signals preserved in dedicated lane; backlog metrics + Grafana |

Runbooks: [DEVELOPMENT.md](./DEVELOPMENT.md). Chaos catalog: [CHAOS.md](./CHAOS.md).

---

## SLAs

| Metric | Target |
| :--- | :--- |
| `ad_http_request_duration_seconds` | p95 < 50 ms, p99 < 80 ms, max 100 ms |
| Redis unified-filter Lua | p99 < 10 ms / shard |
| Geo filter (sampled) | p99 < 10 µs |
| `RunAuction` | p99 < 15 µs; candidates p99 < 500 |
| Fraud boost in `FilterEngine` | ~90 ns; 0 allocs/op |
| Budget invariant | `current_spend ≤ budget_limit` (±1 micro-unit) |

Production: `FILTER_TIMEOUT_MS` ≤ 100.

---

## Engineering constraints (hot path)

- 0 heap allocations per request on parse, filter, auction  
- No `defer`, closures, `interface{}`, `sync.Map`, string `+` in request loops  
- Monotonic deadlines (`FilterDeadlineMono`)  
- Pre-bound Prometheus labels  

CI: `make test-alloc-gate`, `scripts/perf-gate/`, `scripts/chaos-drills/test_chaos.sh`. Rules: [GO.md](./GO.md), [STYLE.md](./STYLE.md).

---

## Licensing (summary)

Hot path reads JWT snapshot only. Cold path: `VolumeMeterWorker` → `usage_meters`. Detail: [MANAGEMENT.md](./MANAGEMENT.md) §8–9.

---

## Optional broker

`cmd/broker`: mmap segment log; used with shipper/compactor/evacuator for regional log evacuation. Not a Kafka replacement.
