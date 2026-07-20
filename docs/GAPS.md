# eSPX — Known Gaps and Open Problems

This document lists **outstanding gaps** as of the current roadmap stage. **Shipped** capabilities are in [SHIPPED.md](./SHIPPED.md). **Remediation plans and DoD** are in [MILESTONE.md](./MILESTONE.md) and per-milestone design docs.

**Stage summary:** M1, M2, M5 closed; M3 core closed. **Next (easy → hard):** M9 installer → M6-W reports → M15–M17 Arbitrage Ops → M6 Day-2 → M4 sharding (scale). See [MILESTONE.md §2](./MILESTONE.md#2-roadmap-overview--execution-order-easy--hard).

---

## Priority overview

Execution order follows complexity grading in [MILESTONE.md §2](./MILESTONE.md#2-roadmap-overview--execution-order-easy--hard) — buyer-visible work before infrastructure scale.

| Priority | Theme | Primary milestone | Risk if ignored |
| :--- | :--- | :--- | :--- |
| **P0** | Buyer demo & onboarding | [M9](./MILESTONE.md#m9--cli-installer--preflight-tier-s-exec-1), [M6-W](./MILESTONE.md#m6-w--buyer-reports--dashboards-tier-s-exec-2) | Cannot sell or deploy; no placement ROI UI |
| **P1** | Arbitrage Ops pack (Pro tier) | [M15](./MILESTONE.md#m15--s2s-postback-dispatcher-tier-m-exec-3)–[M17](./MILESTONE.md#m17--margin-guard--placement-auto-pauser-tier-m-exec-5) | No parity with ClickFlare/Voluum/TheOptimizer |
| **P2** | Production operability | [M6](./MILESTONE.md#m6--day-2-operations--analytics-pipeline-tier-m-exec-6) | Bad K8s routing; silent backlog growth; ops blind spots |
| **P3** | Commercial packaging | [M3-T](./MILESTONE.md#m3-t--commercial-pu-packaging-tier-m-exec-7) | Shipped — JWT S/M/L, PU weights, module gates |
| **P4** | Redis scale ceiling | [M4](./MILESTONE.md#m4--shard-orchestrator--elastic-triplets-tier-xl-exec-12) | p99 SLA breach under skew; no horizontal Redis scale |
| **P5** | Geography, alternative payments | [M7](./MULTI_REGION.md), [M8](./CRYPTO_GATEWAY.md) | No enterprise multi-cell; crypto top-ups missing |
| **P6** | Ledger depth, PII, CH lifecycle | M11–M14 | GDPR/CH cost; finance batch efficiency |

---

## 1. Sharding and Redis (strategic)

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-SHARD-01 | **Fixed StaticSlot, N=4** | `campaign_id → crc32 & 1023 → slot_table`; `ExpectedRedisShardCount=4` in production config | Dynamic home via Shard Orchestrator; +3 Redis per cluster |
| GAP-SHARD-02 | **No Shard Orchestrator** | `AutoscaleShards` migrates slots reactively from Redis INFO | PG `campaign_routing`, `routing_epoch`, triplet (2 primary + 1 reserve), ingress 40/40/20 |
| GAP-SHARD-03 | **Hot-campaign skew** | Uneven load needs heavy slot COPY | Capacity-aware assign + continuous micro-migration |
| GAP-SHARD-04 | **Shard 0 convention** | Pub/sub `campaigns:update`, auth lockout on shard 0 | Survive shard-0 outage for ingest; global keys mirrored ([`redis_global.go`](../internal/management/redis_global.go) partial) |
| GAP-SHARD-05 | **Primitive autoscale signal** | Max normalized INFO metrics; no EWMA, no quorum gate | Two-tier health (tracker atomics + scrape); anti-flap on Sentinel failover |
| GAP-SHARD-06 | **UDP-only critical cutover** | Epoch broadcast `:8190→:8191`; loss → stale throttle | TCP snapshot + HMAC + ACK for routing table (M4) |
| GAP-SHARD-07 | **No inter-cluster scale criteria** | `N=4` fixed; no `N×3` policy | [MILESTONE.md M4 §4.3](./MILESTONE.md#43-cluster-capacity-scoring) |
| GAP-SHARD-08 | **Cross-AZ/DC cluster placement** | Not enforced in installer | `max_inter_node_rtt_ms`, `forbid_cross_dc` preflight |
| GAP-SHARD-09 | **Scale false positives / cascade** | No `C_ema`, cooldown, chaos inhibit | U1–U6, `T_cooldown_up`, R-FP-* in MILESTONE.md |
| GAP-SHARD-10 | **Unresolved divergence on scale** | Epoch/migrate races partial | R-DIV-*, R-NET-* open; chaos backlog |

**References:** [REDIS.md](./REDIS.md) (current), [MILESTONE.md M4](./MILESTONE.md#m4--shard-orchestrator--elastic-triplets-tier-xl-exec-12), chaos scenario A (`shard_0_outage`).

---

## 2. Hot path and consistency

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-HOT-01 | **Monolithic Lua** | `unified-filter.lua`: debit, fcap, TTC, pacing, rate, dedup, XADD in one script | `budget-debit.lua` + Go pre-gates; dynamic tier under load (M4) |
| GAP-HOT-02 | **Debit ∥ XADD tradeoff** | Atomic debit+stream in one RTT; decomposition deferred | Broker ingest path (M6 PIPE) before splitting XADD ([REMEDIATION §6](./REMEDIATION.md)) |
| GAP-HOT-03 | **PG↔Redis window** | SyncWorker async; recon worker exists | Probabilistic reconciliation (REMEDIATION M1); M12 ledger batch |
| GAP-HOT-04 | **Migrate races** | `migration_fence` + per-click idempotency | Batch/migrate idempotency L2/L3 + `routing_epoch` fence (M4) |
| GAP-HOT-05 | **JumpHash in codebase** | `HybridBalancer` / legacy paths | Must not mix with StaticSlot in prod; remove from hot path after M4 |

**SLA touchpoints:** Lua p99 < 10 ms, `/track` p99 < 80 ms ([MILESTONE §0](./MILESTONE.md#0-normative-standards-all-milestones)).

---

## 3. Day-2 operations (M6)

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-OPS-01 | **No healthz/readyz split** | Single `/health`; tracker metrics port **always 200** | Liveness without I/O; readiness includes PG, Redis, spool, stream lag |
| GAP-OPS-02 | **Coarse hot reload** | `campaigns:update` → full `Registry.Sync()` | `UpdateAndWarmCampaign(id)` on pub/sub (HR-PUB) |
| GAP-OPS-03 | **No CH partition janitor** | TTL in DDL only; PG `PartitionManager` for Postgres | `CHPartitionJanitor` in processor; env retention knobs |
| GAP-OPS-04 | **CH admin query governance** | Global `max_execution_time`; incomplete `freshness` on DTOs | `chquery` wrapper, `CH_READONLY_DSN`, per-query memory cap |
| GAP-OPS-05 | **Backlog observability** | DLQ/spool exist; metrics fragmented | Unified gauges: `XLEN`, PEL age, spool segments, gate saturation on `/ops/*` |
| GAP-OPS-06 | **Processor not LB-aware** | Readiness ignores spool threshold | `readyz` fails when spool segments > budget |
| GAP-OPS-07 | **Blacklist replication lag** | Outbox fan-out; no SLO metric | `HR-BL` lag p99 < 5 s + integration test |

**References:** [MILESTONE M6](./MILESTONE.md#m6--day-2-operations--analytics-pipeline-tier-m-exec-6), [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) R8.

---

## 4. Commercial product and admin UX

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-PROD-01 | **Buyer / finance dashboards** | Core licensing + billing JSON API shipped | W1–W3 in `adminapi` — [M6-W](./MILESTONE.md#m6-w--buyer-reports--dashboards-tier-s-exec-2) |
| GAP-PROD-02 | **Reports registration** | `reports_*`, `dashboards_*`, `views_*` not in `register.go` | [M6-W](./MILESTONE.md#m6-w--buyer-reports--dashboards-tier-s-exec-2) |
| GAP-PROD-03 | **Commercial packaging** | Shipped M3-T | [M3_T_TECHNICAL_REPORT.md](./M3_T_TECHNICAL_REPORT.md) |
| GAP-PROD-06 | **S2S postback dispatcher** | Spec only (`IDEAS` §3.1) | [M15](./MILESTONE.md#m15--s2s-postback-dispatcher-tier-m-exec-3) |
| GAP-PROD-07 | **Cost API sync** | Spec only (`IDEAS` §3.2) | [M16](./MILESTONE.md#m16--cost-sync--rsoc-revenue-tier-m-exec-4) |
| GAP-PROD-08 | **Margin guard auto-pauser** | Pro tier in SUBSCRIPTIONS; not implemented | [M17](./MILESTONE.md#m17--margin-guard--placement-auto-pauser-tier-m-exec-5) |
| GAP-PROD-09 | **RSOC feed integrations** | Not in prior roadmap | [M16](./MILESTONE.md#m16--cost-sync--rsoc-revenue-tier-m-exec-4) (Tonic, System1) |
| GAP-PROD-04 | **`usage_daily` flush** | `usage_meters` exist; daily flush worker deferred | M3 optional / M6 |
| GAP-PROD-05 | **No OpenAPI** | godoc only | External admin panel contract (deferred by design in M2) |

**References:** [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md), [ADMINISTRATIVE.md](./ADMINISTRATIVE.md), [MILESTONE M6-W / M3-T](./MILESTONE.md#2-roadmap-overview--execution-order-easy--hard).

---

## 5. Geography and payments

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-GEO-01 | **Single region** | Shipped M7 — `RegionOutboxRelay`, per-region RPD, cell-isolated Redis | [MULTI_REGION.md](./MULTI_REGION.md) |
| GAP-GEO-02 | **Postgres DR automation** | Runbook in [DATABASE.md](./DATABASE.md) §III | Operator-owned R1–R4; not productized |
| GAP-PAY-01 | **Crypto gateway** | Design only | M8 `CryptoProvider` + webhooks |
| GAP-PAY-02 | **Billing payment provider** | `PlaceholderProvider`; Stripe stays in `cmd/payment` | Intentional M2.8 split; real checkout paths via payment service |

---

## 6. Data, fraud, compliance (M11–M14)

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-DATA-01 | **Raw PII in ClickHouse** | `ip_address` in CH schema | M14 hash pipeline + salt rotation |
| GAP-DATA-02 | **IVT interval bots** | Ratio/cluster rules in `ivt-detector` | M11 `IVT-INTERVAL` (σ inter-arrival) |
| GAP-DATA-03 | **Ledger batch consolidation** | SyncWorker per delta; pause on zero partial | M12 ≤1 PG txn per campaign per 10s |
| GAP-DATA-04 | **CH compaction / emergency drop** | M13 `CHPartitionJanitor` recompress + `CH_EMERGENCY_DROP_PERCENT` | Shipped (M13) |
| GAP-CMP-01 | **Optional tarpit** | CMP-DEF-03 deferred | Edge-only slow path behind flag |

---

## 7. Engineering and platform debt

| ID | Gap | Current state | Target / owner |
| :--- | :--- | :--- | :--- |
| GAP-ENG-01 | **Management monolith size** | Single `cmd/management`, flat ~190 Go files | `adminapi` extracted; further split by filename tags only |
| GAP-ENG-02 | **SEM-P3 management PG sem** | Processor gates done; management pool unbounded vs workers | `mgmtPgSem` on admin SQL under load ([REMEDIATION §7](./REMEDIATION.md)) |
| GAP-ENG-03 | **No interactive installer** | `cmd/installer/` draft; hand-edited `.env` | M9 `espx-install` |
| GAP-ENG-04 | **Broker not in compose** | `cmd/broker` optional; blocks clean Lua/telemetry split | M6 PIPE / broker consumers |
| GAP-ENG-05 | **HR-KEYS CI test** | Hash-tag colocation documented | Cross-slot integration test in CI |
| GAP-ENG-06 | **Vendor telemetry** | Default off; not implemented | M10 opt-in bundle |

---

## 8. Chaos and verification gaps

Full matrix: [MILESTONE.md M4 §4.4](./MILESTONE.md#44-chaos-matrix-required-proofs).

| Domain | Covered | Missing / backlog |
| :--- | :--- | :--- |
| **UDP unit** | epoch gap, stale, reorder (`udp_control_chaos_test.go`) | UDP-02 loss 20%, TCP snapshot, CONFIG_REQUEST storm |
| **Redis outage** | Shard 0, Sentinel failover | REDIS-03 migrate blocks, triplet A/B/R |
| **Lua** | migration_fence | LUA-01 SCRIPT FLUSH, LUA-06 tier degrade, LUA-08 routing_epoch |
| **Orchestrator** | — | SO-01 false migrate, SO-02 campaign routing |
| **Network** | container terminate, PG partition | NET-02 RTT+20ms, cross-AZ |
| **FD / CPU** | broker throttle patterns | FD-01 write_path_fd_pressure, CPU-02 redis throttle |
| **Cascading** | staggered redis+pg | CAS-01..05 game days |

| Scenario | Status | Notes |
| :--- | :--- | :--- |
| Shard 0 outage | Covered | `tests/chaos/shard_outage_chaos_test.go` |
| Orchestrator false migrate | **Missing** | SO-01: `orchestrator_no_false_migrate` |
| Routing epoch + migrate race | **Partial** | LUA-08 / SO-02 |
| `SCRIPT FLUSH` under load | Backlog | LUA-01 / Scenario I |
| Chaos Kong (region) | Runbook: `scripts/chaos-drills/m7/README.md` | M7 game day (manual) |
| UDP loss 20% | Planned | UDP-02 `udp_loss_20pct` (compose) |
| Redis single-thread COPY | Partial | REDIS-03 during slot migrate |
| FD exhaustion at scale | Planned | FD-01 at K>3 clusters |

New write paths for M4+ require chaos per [R10 #11, #13](../GUIDE_CHAOS_RELIABILITY.md).

---

## 9. Mapping gaps → milestones

Execution order: [MILESTONE.md §5](./MILESTONE.md#5-execution-order--dependencies).

```text
GAP-ENG-03, GAP-PROD-01/02       →  M9, M6-W        (exec #1–2)
GAP-PROD-06/07/08/09             →  M15, M16, M17   (exec #3–5, Arbitrage Ops)
GAP-OPS-* , GAP-HOT-02 (broker)  →  M6              (exec #6)
GAP-PROD-03, GAP-PROD-04         →  M3-T            (exec #7)
GAP-DATA-02                      →  M11             (exec #8)
GAP-HOT-03, GAP-DATA-03          →  M12             (exec #9)
GAP-DATA-04                      →  M13             (exec #10)
GAP-PAY-01                       →  M8              (exec #11)
GAP-SHARD-* , GAP-HOT-01/04/05   →  M4              (exec #12)
GAP-GEO-*                        →  M7              (exec #13)
GAP-ENG-06                       →  M10             (exec #15)
GAP-DATA-01, GAP-CMP-01          →  M14             (exec #16)
```

---

## 10. Maintenance

| Action | When |
| :--- | :--- |
| Close gap row | Milestone DoD checkbox merged; move summary to [SHIPPED.md](./SHIPPED.md) |
| Add gap row | New regression or discovered debt; link milestone or ADR |
| Review | After each milestone close; keep in sync with [MILESTONE.md](./MILESTONE.md) |

**Do not duplicate:** full DoD tables stay in [MILESTONE.md](./MILESTONE.md). This file is the **problem catalog**, not the execution checklist.
