# Known Gaps and Open Work

Outstanding problems and forward priorities as of the current repository state. Platform capabilities are documented in [ARCHITECTURE.md](./ARCHITECTURE.md). RTB-specific roadmap: [RTB.md](./RTB.md) §6.

---

## Priority Overview

| Priority | Theme | Risk if ignored |
| :--- | :--- | :--- |
| **P0** | RTB exchange surface | No OpenRTB 2.6 bid endpoint; deal outcomes not written to CH |
| **P1** | Buyer UX & reports | No placement ROI dashboards; limited admin reporting |
| **P2** | Redis scale | Fixed N=4 shards; hot-campaign skew; no elastic orchestrator |
| **P3** | Day-2 operability | Health/readiness split incomplete; backlog metrics fragmented |
| **P4** | Data & compliance | PII in CH; optional edge tarpit; vendor telemetry |
| **P5** | Geography & payments | Crypto gateway; Postgres DR automation |

---

## 1. RTB and Programmatic

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-RTB-01 | **No OpenRTB 2.6 hot parser** | Substring scan for 3.0 on `/track` only | Full 2.6 fields on hot path |
| GAP-RTB-02 | **No `/openrtb/bid` endpoint** | Auction embedded in `/track` | Standalone gnet handler + bid response |
| GAP-RTB-03 | **`rtb_deal_outcomes` writer** | CH table + optimizer read; no inserts | Lossy ring → batch insert win/loss |
| GAP-RTB-04 | **PMP deal enforcement** | `geo_mask`, `cat_mask`, `seats`, pacing in DB only | Filter on bid path |
| GAP-RTB-05 | **`ReserveMicro` unset** | Always 0 in catalog sync | Wire from campaign/management |
| GAP-RTB-06 | **Pre-bid IVT** | Fraud runs after RTB in live mode | Lightweight gate before auction |
| GAP-RTB-07 | **`tmax` deadline** | No auction time budget | Monotonic deadline + scan cap |
| GAP-RTB-08 | **Supply chain** | No `schain` generation/validation | ads.txt/sellers.json audit tooling |

Detail and implementation notes: [RTB.md](./RTB.md).

---

## 2. Sharding and Redis

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-SHARD-01 | **Fixed StaticSlot, N=4** | `crc32 & 1023 → slot_table` | Dynamic home + elastic triplets |
| GAP-SHARD-02 | **No Shard Orchestrator** | `AutoscaleShards` from Redis INFO | PG `campaign_routing`, `routing_epoch` |
| GAP-SHARD-03 | **Hot-campaign skew** | Uneven load needs slot COPY | Capacity-aware assign + micro-migration |
| GAP-SHARD-04 | **Shard 0 convention** | Pub/sub, auth lockout on shard 0 | Survive shard-0 outage for ingest |
| GAP-SHARD-05 | **UDP-only routing cutover** | Epoch broadcast; loss → stale throttle | TCP snapshot + HMAC + ACK |
| GAP-SHARD-06 | **Monolithic Lua** | Single `unified-filter.lua` | Optional split; tier degrade under load |

Reference: [REDIS.md](./REDIS.md).

---

## 3. Hot Path and Consistency

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-HOT-01 | **Debit ∥ XADD tradeoff** | Atomic in one Lua script | Broker path before splitting XADD |
| GAP-HOT-02 | **PG↔Redis window** | **M3 shipped:** unified snapshot recon (`ReconcileBudgetSnapshot`), atomic Lua snapshot, grace window, outbox-only `RECONCILIATION_ADJUST`, SKIP LOCKED flush | Broker pending delta term (M8-04) for local-quanta visibility |
| GAP-HOT-03 | **JumpHash paths** | `cmd/tracker-jumphash`, `HybridBalancer` tests | Must not mix with StaticSlot in prod |
| GAP-HOT-04 | **Migrate races** | **M1 shipped:** `CampaignRedisKeyCatalog`, PG re-warm cutover, `EXISTS` gate, fence on orchestrator start | M1-08 dual-write for hot slots (phase 2) |

---

## 4. Day-2 Operations

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-OPS-01 | **healthz/readyz split** | Partial; tracker metrics port always 200 | Liveness without I/O; readiness with PG/Redis/spool/lag |
| GAP-OPS-02 | **Coarse hot reload** | `campaigns:update` → full `Registry.Sync()` | Per-campaign `UpdateAndWarmCampaign` |
| GAP-OPS-03 | **CH admin query governance** | Some paths bypass `CHQuery` | Readonly DSN + per-query caps everywhere |
| GAP-OPS-04 | **Backlog observability** | DLQ/spool exist; metrics fragmented | Unified PEL age, spool segments, gate saturation |
| GAP-OPS-05 | **Blacklist replication SLO** | Outbox fan-out | Lag metric p99 < 5 s |

**Shipped:** `CHPartitionJanitor` in processor (retention, recompress, emergency drop).

---

## 5. Product and Admin UX

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-PROD-01 | **Buyer / finance dashboards** | Billing JSON API exists | Reports/dashboards in `adminapi` |
| GAP-PROD-02 | **Reports registration** | `reports_*` not fully wired in `register.go` | Complete `/api/v1` surface |
| GAP-PROD-03 | **No OpenAPI** | godoc only | External admin contract (deferred by design) |
| GAP-PROD-04 | **`usage_daily` flush** | `usage_meters` hourly; daily worker optional | Enable when billing needs daily snapshot |

**Shipped (arbitrage ops binaries):** `postback-sender`, `cost-sync`, `margin-guard`, `installer` CLI — operational maturity varies; not all wired in default compose.

---

## 6. Geography and Payments

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-GEO-01 | **Multi-region ops** | `RegionOutboxRelay`, D3 v2 dedup adapter (M4), per-region RPD | Full game-day automation |
| GAP-GEO-02 | **Postgres DR** | Runbook in [DATABASE.md](./DATABASE.md) | Productized failover |
| GAP-PAY-01 | **Crypto gateway** | Design in [CRYPTO_GATEWAY.md](./CRYPTO_GATEWAY.md) | `CryptoProvider` + webhooks |

---

## 7. Data, Fraud, Compliance

| ID | Gap | Current state | Target |
| :--- | :--- | :--- | :--- |
| GAP-DATA-01 | **Raw PII in ClickHouse** | `ip_address` in schema | Hash pipeline + salt rotation |
| GAP-DATA-02 | **IVT interval bots** | Ratio/cluster rules | Inter-arrival σ rules |
| GAP-CMP-01 | **Optional tarpit** | Deferred | Edge slow path behind flag |
| GAP-ENG-01 | **Management monolith** | ~190 flat Go files | Filename-tagged modules only; `adminapi` extracted |
| GAP-ENG-02 | **Broker not in default compose** | `cmd/broker` optional | Enables clean Lua/telemetry split |
| GAP-ENG-03 | **Vendor telemetry** | Default off | Opt-in bundle |

---

## 8. Database (Optional Tuning)

Most correctness items are shipped. Remaining optional work:

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-DB-01 | Logger group-commit fsync | `pkg/logger/flush_persist.go` — raise batch size or time-based fsync |
| GAP-DB-02 | CH spool group-commit | Only if PEL retains unacked; never disable spool fsync |
| GAP-DB-03 | Weighted processor gates | Metrics-driven `AcquireWeighted` |

Rules and verification: [DATABASE.md](./DATABASE.md).

---

## 9. Chaos and Verification Backlog

| Domain | Status | Backlog |
| :--- | :--- | :--- |
| UDP control | Unit tests for epoch/reorder | Loss 20%, TCP snapshot |
| Redis outage | Shard 0, Sentinel | Migrate blocks, triplet A/B/R |
| Lua | `migration_fence` | SCRIPT FLUSH under load |
| Orchestrator | — | False migrate, routing epoch races |
| FD / CPU | Partial | Write-path FD pressure at scale |

New write paths require chaos proofs per `GUIDE_CHAOS_RELIABILITY.md` R10.

---

## 10. Suggested Execution Order

```text
RTB Phase 1–2 (GAP-RTB-03..08)     → M7 (exchange minimum + measurement)
GAP-PROD-01/02                      → buyer dashboards (M6-W)
GAP-OPS-01/04                       → operability (M6)
M9 (Lua RTT) + M10 (XDP)           → hot-path relief (parallel)
M1                                 → slot migration hardening (before M2)
GAP-SHARD-*                         → M2 scale (after M1)
GAP-DATA-01, GAP-PAY-01             → compliance & payments (M14, M8)
```

Milestone detail: [MILESTONE.md](./MILESTONE.md) §M7–M10.

---

## Maintenance

| Action | When |
| :--- | :--- |
| Close gap | Capability shipped; move summary to [ARCHITECTURE.md](./ARCHITECTURE.md) |
| Add gap | New regression or discovered debt |
| Review | After major feature merges |

This file is the **problem catalog** only — not a delivery checklist.
