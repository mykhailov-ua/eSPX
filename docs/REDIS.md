# Redis: Topology, Lua Validation, and Risks

The eSPX operational data layer consists of 4 isolated Redis Master nodes (production today). Client-side sharding by `campaign_id` is used. Atomic financial validation runs through embedded Lua scripts (`EVALSHA`).

**Roadmap:** Milestone 4 (exec #12) replaces fixed StaticSlot with a [Shard Orchestrator](./MILESTONE.md#m4--shard-orchestrator--elastic-triplets-tier-xl-exec-12) and **N×(2 primary + 1 reserve)** clusters. Until M4 ships, the sections below describe the **current** production model (documented in [SHIPPED.md](./SHIPPED.md)).

---

## 1. Topology

*   **Shard count:** 4 (Standalone Master + Replicas + Sentinel x3).
*   **Model:** Isolated masters without Redis Cluster. This avoids `MOVED` / `CROSSSLOT` redirects. Each `EVALSHA` command targets exactly one master.
*   **Failover:** Sentinel quorum — 2. Failure detection takes ~5s; promotion to a replica takes ~10–15s.
*   **Circuit Breaker:** Opens after 150 consecutive errors; transitions to half-open after 5s.
*   **Routing:** `campaign_id` → `CRC32C & 1023` (slot) → `rdbs[shard]`. All keys in a single Lua request must belong to one shard.

---

## 2. Global and Local Replication

### Shard 0 (Global State)
Used for shared state:
*   Registry update notifications (Pub/Sub).
*   User lockout markers and session revocation.
*   Creative structures.

### Global Keys (Replicated)
Data copied to all shards via `outbox`:
*   Configuration values (`config:values`), blacklists (`blacklist:*`), fraud-score boosts.

### Local Keys (Shard-Local)
Data stored strictly on one shard:
*   Campaign budgets (`budget:campaign:{id}`), quotas.
*   Deduplication data (`dup:*`, `idempotency:*`).
*   Event streams (`ad:events:stream`).
*   Migration barriers (`budget:migration_fence`).

---

## 3. Lua Scripts

### Processing Tiers
*   **Tier B (`budget-fast.lua`):** For impressions. Performs budget debit and stream write. Skips frequency capping (fcap) and pacing checks.
*   **Tier C (`unified-filter.lua`):** For clicks. Full validation: time-to-click (TTC), fcap, pacing, rate limits, and migration barriers.

### Constraints
Lua scripts must use only non-blocking commands (`GET`, `SET`, `INCR`, `XADD`). Execution time (p99) must be < 10 ms per shard.

---

## 4. Script Lifecycle

*   **Load:** Scripts are embedded in the tracker binary. On startup, `SCRIPT LOAD` runs on all shards.
*   **Execution:** The hot path uses `EVALSHA`. If the script is missing (`NOSCRIPT`), it falls back to sending the full script body via `EVAL`.
*   **Risks:** Script eviction (`SCRIPT FLUSH`) or Redis restart under load causes latency spikes from mass script-body resubmission.

---

## 5. Lua Validation Risks

### P0 Risks (Security and Finance)
*   **R-LUA-01 (TOCTOU):** State between the Go check and Lua execution may change. Resolved by atomicity inside Lua.
*   **R-LUA-03 (Double debit during migration):** Risk of active keys on both old and new shards. Resolved by setting barriers (`migration_fence`).
*   **R-LUA-04 (Master thread blocking):** Long-running Lua operations increase latency for all requests to that shard.

### P1 Operational Risks
*   **R-LUA-08 (NOSCRIPT):** Loss of preloaded scripts on restart.
*   **R-LUA-09 (Slot drift):** Mismatched slot-map update timing between tracker and edge.

---

## 6. Fail Policy

*   **GeoIP / Blacklists (Tracker):** Fail-open (allow traffic on error).
*   **TTC (click check):** Configurable (`TTC_FAIL_CLOSED`); default is fail-open.
*   **Blacklists (Edge):** Fail-closed (block, HTTP 503).
*   **Redis Circuit Breaker:** Fail-closed (HTTP 503).
*   **Lua error / Filter timeout:** Fail-closed (no debit or impression).
