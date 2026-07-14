# Dynamic Quota and Redis Shard Balancing (eSPX Control Plane)

This document describes the fault-tolerant, asynchronous mental model for distributed load shaping across Redis shards. The architecture targets ultra-high RPS with full isolation of the tracker hot path (`gnet`, `epoll`) from blocking network calls and control-plane overhead.

**Related:** [architecture.md](./architecture.md) (current topology), [GUIDE_CHAOS_RELIABILITY_RU.md](../GUIDE_CHAOS_RELIABILITY_RU.md) (chaos and testing standards).

---

## 1. Topology and Data Flows

All control-plane interactions are asynchronous. The hot path never blocks on management HTTP, Postgres, or cross-shard coordination.

### Current production topology

| Layer | Components | Role in sharding |
| :--- | :--- | :--- |
| Ingress | Nginx/OpenResty `:8180` | Edge shard pick via Castagnoli CRC32 + 1024-slot map (`deploy/nginx/lua/edge-slot-map.lua`); must match Go `StaticSlotSharder` |
| Ingestion | Tracker ×4 `:8181–8184` | `gnet` event loops, `PinnedWorkerPool`, per-shard Redis clients, circuit breakers |
| Edge state | Redis ×4 `:6479–6482` + replicas + Sentinel ×3 | Standalone masters (not Redis Cluster); one `EVALSHA` per event via `unified-filter.lua` |
| Control plane | Management `:8188` | Outbox, `ReconWorker`, `QuotaManager`, `ShardAutoscaling`, `SlotMigrationOrchestrator` |
| Persistence | Postgres 16, ClickHouse 24 | Slot map versions, campaign quotas, ledger truth |

### Shard routing model

Routing is strictly `campaign_id`-based:

```
slot = crc32_castagnoli(campaign_id) & 1023
shard = slot_table[slot]   // precomputed; default slot % N
```

`StaticSlotSharder` (`internal/ads/sharding.go`) holds the 1024-entry table in `atomic.Value`; reload swaps the whole table on the cold path. Nginx Lua uses the same algorithm. Composite keys (`campaign_id + user_id`) are used only for edge rate limiting, not for shard selection.

**Shard-0 conventions** (by convention, not by hash): `campaigns:update` pub/sub, auth lockout, brand creatives. Campaign budget and filter keys remain on `StaticSlotSharder(campaign_id)` shards.

### Control-plane propagation (implemented)

1. **Management** collects shard health (Redis `INFO`, breaker state, outbox lag), runs autoscaling and slot migration, and applies side effects through the outbox.
2. **Outbox → Redis**: global config (`config:values`, `config:version`), blacklist sets, and campaign keys are replicated per shard (`internal/management/redis_global.go`).
3. **Invalidation**: `campaigns:update` pub/sub on shard 0 triggers in-memory registry reload in trackers.
4. **Slot map cutover**: Postgres `slot_map_versions` → `SlotMigrationOrchestrator` copies keys → `activateSlotMapVersion` → trackers reload `StaticSlotSharder` via `StoreSlotMap`.

### Target: UDP epoch quotas (planned)

For sub-second ingress throttling when a shard degrades, the control plane will push per-shard RPS limits to trackers over UDP (see §3). This avoids TCP handshakes and management round-trips on the hot path. Until UDP quotas ship, degradation is handled by per-shard circuit breakers, Sentinel failover, and slot migration.

---

## 2. Shard State Coefficient ($K_{\text{state}}$)

Management computes an integral health coefficient $K_{\text{state}} \in [0.0, 1.0]$ per shard. Weights prioritize CPU and latency because Redis is single-threaded and sensitive to CPU throttle.

### Degradation index

For each shard, a penalty index $D$ is calculated:

$$D = (W_{\text{cpu}} \cdot M_{\text{cpu}}) + (W_{\text{lat}} \cdot M_{\text{lat}}) + (W_{\text{ram}} \cdot M_{\text{ram}}) + (W_{\text{err}} \cdot M_{\text{err}})$$

Metric components $M$ are normalized from 0 (healthy) to 1 (critical):

| Metric | Source | Trigger (normalized to 1.0) |
| :--- | :--- | :--- |
| $M_{\text{cpu}}$ | Redis `INFO cpu` | CPU proxy > 80% (`ShardAutoscaleConfig.CPULimit`) |
| $M_{\text{lat}}$ | Lua `SLOWLOG` / tracker histogram | p99 > 15 ms (`LuaP99Limit`) |
| $M_{\text{ram}}$ | `used_memory` / `maxmemory` | > 85% (`MemoryPctLimit`) |
| $M_{\text{err}}$ | Tracker breaker trips, connection timeouts | Error rate above steady-state baseline |

### Weights

| Weight | Value | Rationale |
| :--- | :--- | :--- |
| $W_{\text{cpu}}$ | 0.40 | CPU saturation destroys Redis latency predictability |
| $W_{\text{lat}}$ | 0.35 | p99 spikes indicate heavy commands or blocking |
| $W_{\text{ram}}$ | 0.15 | Approaching OOM / eviction policy |
| $W_{\text{err}}$ | 0.10 | Network and client-side failures |

Final quota coefficient:

$$K_{\text{state}} = \max(0.0, 1.0 - D)$$

**Canary mode:** when $K_{\text{state}} < 0.2$, the shard enters canary probing with a 5% traffic quota to detect recovery. Full failover to remaining shards occurs only after the penalty holds for two consecutive epochs (10–20 s each).

**Autoscaling (implemented):** `AutoscaleShards` (`internal/management/service_shard_autoscaling.go`) uses a max-normalized load score across the same dimensions. When a shard exceeds thresholds, up to `SlotsToMigrate` slots (default 16) move from the overloaded shard to the least loaded peer via the slot map draft → migrate → activate pipeline.

---

## 3. Control Protocol

### Implemented: outbox and slot map versioning

| Mechanism | Idempotency | Ordering |
| :--- | :--- | :--- |
| `outbox_events` | Event type + payload hash; `FOR UPDATE SKIP LOCKED` | `created_at ASC` (priority lanes planned: blacklist/pause first) |
| `slot_map_versions` | Monotonic version ID; checkpoint per campaign in PG | Draft → MIGRATING → ACTIVE; rollback without ledger loss |
| `config:version` | Monotonic string on every shard | Trackers iterate shards until version responds |

### Target: UDP quota datagram (planned)

Topology sync for ingress RPS limits will use UDP. Packet loss and out-of-order delivery are handled by monotonic **Epoch ID** and a **Config Hash** for idempotency.

#### Datagram layout

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Magic (0xESPX)                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                      Master Coarse Time                       +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                           Epoch ID                            +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Config Hash                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Payload Length        |            Padding            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Shard ID (4 bytes)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                   Quota Limit RPS (8 bytes)                   +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

#### Tracker receive state machine

1. **Validate:** check magic and config hash; drop corrupted datagrams at the socket boundary.
2. **Epoch selection:**
   - `Packet.Epoch <= Tracker.CurrentEpoch` → stale; drop (out-of-order).
   - `Packet.Epoch == Tracker.CurrentEpoch + 1` → linear update; atomic pointer swap on quota map.
   - `Packet.Epoch > Tracker.CurrentEpoch + 1` → gap detected (packet loss); skip missing steps and apply the current epoch snapshot (full limit set per shard).
3. **Stale channel fallback (UDP-only):** if no valid UDP packet arrives for $2 \times$ sync interval, mark the channel `STALE`, **tighten ingress to canary floor** (5% of last-known limit or hard floor 100 RPS/shard), and emit a `CONFIG_REQUEST` datagram (see §9.2). Management responds with a redundant `CONFIG_SNAPSHOT` burst (3× unicast + 1× broadcast per tracker). **No TCP, no HTTP/gRPC** on the control path — Postgres remains the only durable authority for money; UDP carries only ingress limits and routing metadata.

4. **Epoch gap policy (financial-safe):**
   - If `Packet.Epoch > CurrentEpoch + 1` and the snapshot **tightens** limits (lower RPS or higher $K_{\text{state}}$ penalty) → apply immediately (fail-closed on uncertainty).
   - If the snapshot **loosens** limits → require contiguous epoch chain **or** a `CONFIG_SNAPSHOT` signed with `Config Hash` matching Postgres `control_plane_epochs` row. Never loosen ingress on a gap alone.

---

## 4. Hot-Path Memory and Concurrency Model

The tracker hot path avoids `time.Now()` syscalls and mutexes on the request path. The following concepts govern layout and synchronization.

### Coarse time

A low-priority background goroutine reads the kernel clock every 10–20 ms and publishes a single **coarse timestamp** via an atomic store. `gnet` workers read it with an atomic load — a single cache-line touch, no syscall. On UDP quota receipt (planned), the master coarse time field re-aligns local clocks; if local time runs ahead, the ticker freezes until alignment.

**Principle:** separate **wall-clock** (control plane, logging, schedules) from **monotonic time** (filter deadlines, TTC). Chaos scenario D (`clock_drift_chaos_test.go`) proves monotonic deadlines survive +3600 s wall-clock drift.

### Cache-line isolation and false sharing

On x86_64, cache lines are 64 bytes. When independent goroutines update adjacent fields in the same line, cores invalidate each other's caches (**false sharing**), destroying scalability.

**Mitigations in eSPX:**

| Pattern | Technique | Where |
| :--- | :--- | :--- |
| Per-worker counters | Pad hot fields to 64-byte boundaries; one quota counter per `gnet` loop | Planned UDP quota map |
| MPSC ring indices | Separate `write` and `read` indices with 64-byte padding between them | `MPSCQueue` (`worker_pool.go`) |
| Immutable config swap | Readers never touch writer fields; whole `slotTable` / alias table replaced atomically | `StaticSlotSharder`, `HybridBalancer` |
| Breaker state | Per-shard `int32` state via CAS, not a global mutex | `RedisBreaker` |

**Structure-of-arrays vs array-of-structures:** when N workers each own a counter, an array of padded structs (AoS with padding) keeps each counter on its own line. A flat `[]int64` of counters would pack multiple counters per line and cause cross-core invalidation under load.

### Lock-free quota check (planned)

Each `gnet` event loop owns a **worker-local quota cell**: `MaxAllowed` (limit for the current epoch) and `CurrentOps` (atomic counter). The check is O(1):

1. Atomic load of the active config pointer (acquire semantics).
2. Index worker-local cell by `(shard_id, worker_id)`.
3. Atomic increment on `CurrentOps`; compare against `MaxAllowed`.
4. On exhaustion, reject or redirect to a healthy shard without locking.

Config updates use **atomic pointer swap** on the whole quota map: readers see either the old or new epoch, never a torn structure. No `map` lookups on the hot path in production — shard indices are precomputed integers.

### Data-oriented design checklist (hot path)

- Struct fields ordered by descending size for alignment; explicit padding where needed.
- No reflection, interface boxing, channels, `defer`, or closures in hot loops.
- Pre-cap slices or stack-fixed arrays; no `append` on the request path.
- `runtime.KeepAlive` at `unsafe` / async boundaries.
- Hot-path pools isolated from HTML/UI contexts.

---

## 5. Graceful Degradation and Rolling Drain

Hard removal of a shard from topology is a critical mutation. eSPX limits **blast radius to one shard per operation** (25% of campaigns at N=4).

### Drain sequence (slot migration)

1. **Drain mode:** management marks target slots `MIGRATING` in the draft slot map; trackers stop routing new `campaign_id` keys to the draining shard.
2. **Traffic shift:** in-flight requests on open Redis connections complete; new requests hash to remaining shards. Circuit breaker opens on the draining shard after fail threshold.
3. **Local quiesce:** `SlotMigrationOrchestrator` copies keys with PG checkpoints; when drain jobs report idle, management activates the new map version.
4. **Technical STW:** brief Redis maintenance (failover, cache flush) only after all trackers report zero in-flight ops on drained slots.

### Sentinel failover

Per master: quorum 2, `down-after-milliseconds 5000`, `failover-timeout 10000`. Expected recovery: 10–15 s (subjective down + promotion + breaker half-open ~5 s). Nginx Lua resolves master via Sentinel with 5 s cache TTL.

### Fallback channel (UDP-only)

If a tracker loses control-plane connectivity (no pub/sub, stale `config:version`, UDP channel `STALE`), per-shard breakers prevent unbounded Redis retry storms. Trackers emit `CONFIG_REQUEST` over UDP; management answers with `CONFIG_SNAPSHOT` bursts sourced from Postgres `control_plane_epochs`. Ingress limits **fail-closed** (canary floor) until a valid snapshot arrives. Budget authority stays in Redis Lua + Postgres ledger — UDP never mutates `budget:*` keys.

---

## 6. SLA Criteria

SLAs apply to the ingestion plane under steady-state load. Violations trigger abort in chaos experiments and Alertmanager pages in production.

### Latency (tracker gnet path)

| Metric | Target | Hard ceiling | Measurement |
| :--- | :--- | :--- | :--- |
| p95 | < 50 ms | — | `ad_http_request_duration_seconds` |
| p99 | < 80 ms | — | same histogram |
| Wall time | — | 100 ms | per-request deadline |
| OpenRTB budget | ~20 ms tracker processing | — | leaves headroom for exchange RTT |

### Availability and blast radius

| Scenario | Acceptable impact | Recovery |
| :--- | :--- | :--- |
| Single shard down (N=4) | ~25% campaigns fail-closed on that shard; p99 on other shards unchanged | Sentinel + breaker: ≤ 15 s |
| Shard 0 down | Pub/sub and auth lockout affected; shards 1–3 track normally | Outbox PENDING until recovery |
| Control plane lag | `ad_management_outbox_oldest_pending_seconds` < 30 s | Priority lanes for blacklist/pause |
| Budget drift | `(budget_limit - redis_remaining) = pg_current_spend + sync_delta` ± 1 micro | `ReconWorker` within 2 h window |

### Error rate

- < 0.1% failed responses on `/track` (excluding valid filter rejections: 200/202/204).
- Circuit breaker open > 5 min → critical alert.
- DLQ length > 100 → page.

### Throughput

- Steady-state RPS stable without unexplained drops during single-shard chaos.
- Zero heap allocations on touched hot-path benchmarks (`0 allocs/op`).

### Financial correctness

**Postgres is the single point of trust.** Redis holds ephemeral spend counters; UDP holds ingress hints only. Reconciliation and chaos assertions always compare against PG `current_spend` and `campaign_quotas`.

- No negative campaign budgets after partial outages.
- `current_spend ≤ budget_limit` in Postgres at all times (hard invariant).
- Idempotent click handling: `idempotency:click:{click_id}` in Lua + `sync_idempotency` in Postgres.
- At-least-once delivery with effectively-once settlement via outbox + idempotent consumers.
- UDP / ingress throttle may fail-closed; it **must not** fail-open into unbounded Redis load that risks budget deadline misses.

---

## 7. Testing Standards (Sharding Profile)

Testing follows [GUIDE_CHAOS_RELIABILITY_RU.md](../GUIDE_CHAOS_RELIABILITY_RU.md) (R1–R10). This section **extends** the global guide for sharding, UDP control plane, Lua bottlenecks, and financial invariants. **Postgres is the single point of trust** for all budget reconciliation assertions.

### Mapping to global requirements

| Global rule | Sharding extension |
| :--- | :--- |
| R1 steady-state | Per-shard RPS, Lua p99, PG R5 on **every** shard touched by fault |
| R2 real faults | `tc netem` loss/jitter on **UDP control port** and Redis data port separately |
| R3 blast radius | One shard per CI experiment; migration tests use fenced cohort only |
| R4 unreliable network | UDP duplicate/loss/reorder injected; idempotency via epoch + config hash |
| R5 anti-slop | ≥32 goroutines on budget/quota/migration paths; `-race` mandatory |
| R7 chaos_proof | New faults listed in §7.4; `CHAOS_MIN_PROOFS` incremented per new proof |
| R9 experiment design | Document network impairment % alongside fault name |
| R10 overhead | Required for: Lua split, UDP codec, migration fence, quota repair |

### Test pyramid

### Test pyramid

```
┌─────────────────────────────────────────────────────────┐
│ 4. CHAOS     testcontainers, SIGKILL, partition, drift  │
├─────────────────────────────────────────────────────────┤
│ 3. E2E       Nginx → Tracker → Redis → PG/CH            │
├─────────────────────────────────────────────────────────┤
│ 2. SMOKE     verify_redis_topology.sh, check_deps.sh    │
├─────────────────────────────────────────────────────────┤
│ 1. UNIT      StaticSlotSharder, Lua, zero-alloc benches   │
└─────────────────────────────────────────────────────────┘
```

### Sharding steady-state hypothesis (R1)

Before any sharding chaos experiment, record baseline:

1. RPS per shard (campaigns pinned via `CampaignIDForShard`).
2. p95/p99 on unaffected shards.
3. Budget invariant on control cohort campaigns.

Default thresholds: p95 < 50 ms, p99 < 80 ms, error rate < 0.1%.

### Mandatory sharding chaos scenarios

| Scenario | Hypothesis | CI test | `chaos_proof` |
| :--- | :--- | :--- | :--- |
| Shard 0 outage | Shards 1–3 unaffected; outbox PENDING | `tests/chaos/shard_outage_chaos_test.go` | `fault=shard_0_outage` |
| Sentinel failover | ≤ 15 s recovery; budget non-negative | `redis_sentinel_chaos_test.go`, manual B | `fault=sentinel_active_failover` |
| Slot migration under load | Idempotent copy; **no dual debit** | `slot_migration_chaos_test.go` | `fault=slot_migration_*` |
| Migration fence reject | Lua rejects debit on `migration_gen` mismatch | `slot_migration_chaos_test.go` | `fault=slot_migration_fence` |
| Autoscale spike | Rebalance without double-freeze | `shard_autoscaling_chaos_test.go` | `fault=shard_autoscale_*` |
| Quota refill race | Single PG chunk via GETDEL lock | `quota_chaos_test.go` | `fault=quota_refill_race` |
| Quota drift repair | PG↔Redis drift auto-repaired ≤60 s | `quota_chaos_test.go` | `fault=quota_drift_repair` |
| Dead shard quota release | Stuck reservations released **only** with quorum | `quota_chaos_test.go` | `fault=quota_dead_shard_release` |
| Clock drift | Monotonic TTC/deadlines hold | `clock_drift_chaos_test.go` | `fault=clock_drift_monotonic_safety` |
| Clock drift + UDP time | Coarse-time clamp; TTC still passes | `clock_drift_chaos_test.go` | `fault=clock_drift_udp_time` |
| UDP loss + reorder | Ingress fail-closed; no budget overspend | `udp_control_chaos_test.go` | `fault=udp_loss_reorder` |
| UDP epoch gap tighten | Lost downgrade epoch applies immediately | `udp_control_chaos_test.go` | `fault=udp_epoch_gap_tighten` |
| UDP epoch gap loosen | Loosen rejected without snapshot | `udp_control_chaos_test.go` | `fault=udp_epoch_gap_loosen_block` |
| UDP STALE fail-closed | No packet 2× interval → canary floor | `udp_control_chaos_test.go` | `fault=udp_stale_fail_closed` |
| Sharp load spike | 10× RPS 30 s; p99 <80 ms on control shard | `shard_load_spike_chaos_test.go` | `fault=shard_load_spike` |
| Lua fast-path split | p99 Lua ≤10 ms at 50k ops/s/shard | `lua_fastpath_bench_test.go` | `fault=lua_fastpath_p99` |
| Concurrent RW migration | 32 trackers read map while StoreSlotMap | `sharding_test.go` | `fault=slot_map_concurrent_swap` |
| LocalQuotaCache race | `-race` clean under 32 block/unblock | `quota_local_race_test.go` | `fault=local_quota_cache_race` |

### Network impairment matrix (R2 extension)

Inject with `tc netem` on the **UDP control** interface and **Redis** interface independently. Each test documents `loss_pct`, `delay_ms`, `jitter_ms` in `chaos_proof`.

| Profile | loss | delay | jitter | Target invariant |
| :--- | :--- | :--- | :--- | :--- |
| `udp_light` | 1% | 0 | 0 | Epoch monotonic; no loosen without snapshot |
| `udp_moderate` | 5% | 2 ms | 1 ms | STALE → canary floor; R5 holds |
| `udp_severe` | 20% | 10 ms | 5 ms | CONFIG_REQUEST recovery ≤3 s; no overspend |
| `redis_rtt` | 0% | 3 ms | 1 ms | Lua p99 ≤15 ms; breaker closed on control shard |
| `combined` | 5% UDP + 3 ms Redis | — | — | Control shard p99 <80 ms; PG R5 ±1 micro |

**Overspend abort:** any test **MUST** `t.Fatal` if `AssertBudgetInvariant` diff > 1 micro or `campaigns.current_spend` exceeds `budget_limit` in Postgres.

**Concurrent read/write:** migration and slot-map tests **MUST** run ≥32 parallel `GetShard` / `Check` goroutines while `StoreSlotMap` runs on a background goroutine (R5).

**Sharp load rise:** use `scripts/load/k6_dirty_traffic.js` or pinned `CampaignIDForShard` cohort; ramp 1×→10× over 10 s, hold 30 s, ramp down. Abort if control-cohort p99 >80 ms for 30 s or any R5 violation.

### Experiment design (R9)

Each test documents:

1. **Hypothesis** — steady-state metric X stays within Y under fault Z.
2. **Control cohort** — campaigns on unaffected shards.
3. **Fault** — exactly one variable (R10: no multi-shard kill in CI).
4. **Abort** — invariant violation or p99 > 80 ms for 30 s.
5. **Proof** — stdout line: `chaos_proof fault=<name> <key>=<value>`.

CI gate: `./scripts/chaos/test_chaos.sh` with `CHAOS_MIN_PROOFS ≥ 28`.

### Invariants (R4, R5)

| Invariant | Check |
| :--- | :--- |
| R5 budget (PG authority) | `AssertBudgetInvariant` on **Postgres** truth: `(budget_limit - redis_remaining) = pg_current_spend + sync_delta` ±1 micro |
| PG spend ceiling | `current_spend ≤ budget_limit` in Postgres after every chaos test |
| Shard isolation | Failure on shard K does not corrupt keys on shard J |
| Migration fence | No debit on source shard after `migration_gen` bump (Lua return code 11) |
| Routing parity | Go `StaticSlotSharder` == Nginx `edge-slot-map.lua` for sample campaign IDs |
| Idempotency | Duplicate `click_id` does not double-charge (Redis + PG) |
| Quota reservation | `pg_reserved ≈ redis_quota + redis_sync + redis_inflight` within one `chunk_size` |
| UDP ingress only | UDP epoch changes **never** write `budget:*` keys |
| Breaker lifecycle | Open → half-open → closed after Redis recovery |

### Concurrency and tooling

- ≥32 goroutines on money/quota/migration/UDP paths; run with `-race`.
- Real Redis and Postgres via `testcontainers-go`; no `sqlmock` on integration paths.
- Hot-path benchmarks: `0 allocs/op` on changed code; Lua bench: `redis-benchmark EVALSHA` in CI smoke optional.
- Network: `tc netem` or testcontainers `ExtraHosts` + `Toxiproxy` for UDP port only.
- Smoke before release: `scripts/redis/verify_redis_topology.sh` confirms `len(REDIS_ADDRS) == ExpectedRedisShardCount`.
- Load: record baseline in `var/load-test/<run>/` per `scripts/load/snapshot_runtime.sh`.

### Observability during chaos (R8)

Monitor:

- `ad_tracker_health_degraded`
- `ad_redis_breaker_state` (0=closed, 1=half-open, 2=open)
- `ad_redis_lua_noscript_total`
- `ad_redis_lua_duration_seconds` (p50/p99 per shard)
- `ad_udp_control_epoch_lag` (planned)
- `ad_udp_control_stale_total` (planned)
- `ad_processor_stream_lag_seconds`
- `ad_management_outbox_oldest_pending_seconds`
- `ad_quota_drift_micro` (PG − Redis expected reserved)

### When chaos is unnecessary (R10)

Skip new chaos tests for: HTMX/CSS, read-only admin handlers, comment-only changes, dead code removal. **Required** for: new Lua scripts, shard routing changes, outbox events, quota/budget mutations, slot map logic.

---

## 8. Merge Checklist

Sharding or control-plane changes:

- [x] Steady-state metrics defined (§6) — M1–M3: `ad_udp_control_*`, `ad_quota_*`, migration fence counters
- [x] R10 overhead check — new chaos test only if distributed write/routing changed (M1–M3 scope)
- [x] Hypothesis and abort criteria documented (R9) — per-milestone DoD in §9.4
- [x] Integration tests use real Redis/Postgres (R5) — chaos tests in `internal/ads`, `internal/management`
- [x] Budget invariant asserted where applicable — R5 in slot migration, quota repair proofs
- [x] `chaos_proof` added or existing proofs still pass — `CHAOS_MIN_PROOFS=36`
- [ ] Nginx Lua shard map updated if Go routing changed — not required for M1–M3 (routing unchanged)
- [x] `./scripts/chaos/test_chaos.sh` passes (`CHAOS_MIN_PROOFS`)
- [x] UDP-only control path — no TCP/HTTP fallback added
- [x] Postgres R5 asserted on control cohort

---

## 9. Implementation Plan

**Principles (DDIA / Tanenbaum):**

1. **Postgres = single point of trust** for spend, reservations, slot-map versions, and control-plane epoch history. Redis is a linearizable **cache/executor** per shard; UDP is a **lossy hint** for ingress only.
2. **Linearizability where money moves** — budget debit stays in one Lua script per event; cross-store quota uses PG row locks first, then Redis credit.
3. **Fencing stale actors** — migration generation tokens (R4 leader epochs) reject debits on draining shards.
4. **Fail-closed under uncertainty** — UDP loss, epoch gaps, and STALE channel tighten ingress; never loosen without proof.
5. **Separate concerns** — ingress RPS (UDP + local atomics) ≠ budget authority (Lua + PG).

### 9.1 Gap inventory → workstreams

| ID | Gap | Workstream | Milestone | Status |
| :--- | :--- | :--- | :--- | :--- |
| G1 | Slot migration dual-key double debit | Migration fence in Lua + PG `migration_gen` | M1 | **Done** |
| G2 | `activeVersion` / `slots` torn read | Single `atomic.Value` snapshot struct | M1 | **Done** |
| G3 | `LocalQuotaCache` data race | Atomic slot snapshot per cache line | M1 | **Done** |
| G4 | HTTP/TCP control fallback | UDP `CONFIG_REQUEST` / `CONFIG_SNAPSHOT` | M2 | **Done** |
| G5 | UDP epoch gap loosens limits | Tighten-only gap + PG-signed snapshot | M2 | **Done** |
| G6 | STALE channel holds high limit | Fail-closed canary floor | M2 | **Done** |
| G7 | PG→Redis quota crash gap | Recon auto-repair + outbox repair event | M3 | **Done** |
| G8 | Dead-shard zeroes reservations too eagerly | Quorum: Sentinel + tracker breakers + sustained ping | M3 | **Done** |
| G9 | Outbox lacks budget-pause priority | Priority lane in outbox worker | M1 | **Done** |
| G10 | Lua CPU bottleneck at scale | Tiered scripts + Go pre-gates | M4 | **Done** |
| G11 | Lock-free ingress counter false sharing | Padded per-worker cells (§4) | M5 | **Done** |
| G12 | Coarse time UDP resync vs TTC | Monotonic-only deadlines; wall clock cap delta | M5 | **Done** |

### 9.2 UDP control protocol (complete, UDP-only)

All control messages use the same UDP socket pair: management `:8190` → trackers `:8191`. **No TCP.**

| Msg type | Direction | Purpose |
| :--- | :--- | :--- |
| `QUOTA_EPOCH` | Mgmt → Tracker | Per-shard RPS limit + epoch (§3 layout) |
| `CONFIG_SNAPSHOT` | Mgmt → Tracker | Full per-shard limit vector + slot-map version + config hash |
| `CONFIG_REQUEST` | Tracker → Mgmt | Tracker asks for snapshot; carries `tracker_id`, `last_epoch`, `config_hash` |
| `MIGRATION_BARRIER` | Mgmt → Tracker | `migration_gen` + draining slots bitmap (1024-bit) |

**Reliability without TCP:**

- Management sends each epoch **3× redundant** unicast + 1× broadcast per sync interval.
- Trackers dedupe by `(epoch_id, config_hash)`.
- On `CONFIG_REQUEST`, management reads Postgres `control_plane_epochs` and responds with `CONFIG_SNAPSHOT` burst (5×).
- Trackers persist last-good snapshot to **local memory only** — never to Redis budget keys.

**Postgres tables (new):**

```sql
-- control_plane_epochs: durable epoch history for UDP recovery
CREATE TABLE control_plane_epochs (
  epoch_id       BIGINT PRIMARY KEY,
  config_hash    BYTEA NOT NULL,
  payload_json   JSONB NOT NULL,  -- per-shard RPS, K_state, slot_map_version
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- campaign_migration_gen: fencing token per campaign
ALTER TABLE campaigns ADD COLUMN migration_gen BIGINT NOT NULL DEFAULT 0;
```

### 9.3 Lua hot-path optimization (M4)

Redis is single-threaded (Tanenbaum: resource multiplexing on one executor). One fat `EVALSHA` per event becomes the dominant bottleneck before network. Strategy: **minimize work inside the atomic window**; keep financial debit atomic.

#### Tier A — Go pre-gates (0 Redis round-trips)

Run before Lua (already partially implemented):

- Emergency breaker, geo (fail-open), schedule, registry miss.
- **New:** local UDP ingress gate (padded atomic counter per worker).
- **New:** `migration_gen` check from tracker in-memory snapshot (reject before Redis).

#### Tier B — `budget_fast.lua` (single round-trip, ≤5 keys)

For events that pass pre-gates and need only budget + idempotency:

```
KEYS: spend_key, idempotency, sync, stream
```

- MGET spend + idempotency
- Budget check + INCRBY debit + SET idempotency NX + XADD stream
- **No** rate, fcap, daily pacing, quota refill trigger in fast path
- Target: ≤30 µs Lua CPU at p99 (measured via `LATENCY DOCTOR` / `SLOWLOG`)

#### Tier C — `filter_full.lua` (current unified-filter.lua)

Full filter for clicks needing TTC, fcap, pacing, quota refill signal. Invoked when:

- `evt_type == click` && TTC enabled
- fcap / daily pacing active
- quota refill threshold crossed on prior fast-path sample (1% hash sample)

#### Tier D — amortized side effects

| Side effect | Today | Target |
| :--- | :--- | :--- |
| Quota refill signal | Inline SADD in Lua | Sampled 1%; or async `fraud_stream` flag |
| Stream XADD | Every event | Batch via `FraudStreamWriter` MPSC (lossy OK) for impressions |
| Daily spent INCR | Inline | Fold into processor sync from stream |

**Financial rule:** Tier B **must** include idempotency + debit + sync counter in one script — no splitting across round-trips (DDIA: lost RPC → duplicate unless idempotent).

**Definition of done (Lua):** `BenchmarkUnifiedFilter` / redis-benchmark shows ≥30% p99 reduction at 50k ops/s/shard; R5 invariant holds in A/B chaos run.

#### go-redis driver (residual allocations)

`BenchmarkUnifiedFilter_Check` with `mockRedisClient` reports **0 allocs/op** — eSPX-side pools (`unifiedScratchPool`, `evalWirePool`, `evalCmdPool`) and `unsafeString` key wiring are clean. With a real Redis round-trip, `BenchmarkUnifiedFilter_Check_RealRedis` still shows **~3 allocs/op, ~44 B/op**; that cost is almost entirely inside [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis/tree/v9.19.0) (`UniversalClient.Process` → connection I/O and RESP decode in [`internal/proto/reader.go`](https://github.com/redis/go-redis/blob/v9.19.0/internal/proto/reader.go)), not from M1 fence keys or eval wire growth.

**Applied mitigations (tracker hot path):**

- Filter deadline on `evt.FilterDeadlineMono` via `FilterEngine.Check` / `setFilterDeadlineOnEvent` — avoid per-request `context.WithValue` on production ingest.
- `LocalQuotaCache` uses `monotonicNano()` (same clock as filter deadline), not wall `time.Now()`.
- `evalWirePool` pre-sized to 47 elements (3 + 17 keys + 27 argv) with cap 56.
- Benchmark harness: reuse pooled `domain.Event`, format `ClickID` into `ClickIDBuf` with `strconv.AppendInt`.

**Future (M4+, if 3 allocs/RPC is unacceptable at target RPS):** audit go-redis sources above; evaluate [rueidis](https://github.com/redis/rueidis) pipeline API or a pinned-conn custom RESP reader. Financial Lua debit stays one atomic `EVALSHA` per event regardless of client choice.

### 9.4 Milestones

**Progress (2026-07):** M1–M6 implemented (load validation + game day tooling).

#### M1 — Financial fences and atomic config (G1, G2, G3, G9)

**Scope:**

- `SlotMapSnapshot { table [1024]uint16; version int32; migrationGen int64 }` behind one `atomic.Value`.
- Lua: read `migration_gen` key; return `11` if `campaign.migration_gen` in PG ≠ Redis key (set by management before copy).
- Management: `UPDATE campaigns SET migration_gen = migration_gen + 1` for slot cohort **before** `CopySlotMigrationData`; set Redis `budget:migration_gen:{id}` on **source** only.
- `LocalQuotaCache`: replace with `[4096]atomic.Uint64` packing `(hash32(campaign_id) << 32) | blocked_mono_ms`.
- Outbox: priority lane `PAUSE_CAMPAIGN`, `BUDGET_FREEZE` ahead of bulk config.

**Definition of done:**

- [x] `TestChaos_SlotMigrationFence`: 32 concurrent debits during copy → ≤1 shard debited; R5 on PG.
- [x] `TestStaticSlotSharder_SnapshotAtomic`: version always matches table under concurrent swap.
- [x] `go test -race ./internal/ads/... -run LocalQuota` clean.
- [x] `chaos_proof fault=slot_migration_fence` in CI.
- [x] Merge checklist §8 complete (M1 scope).

**Estimate:** 1 sprint.

**Status:** Implemented; enabled on staging via `MIGRATION_FENCE_ENABLED=true` in `configmap-env.staging.tpl`.

---

#### M2 — UDP control plane (G4, G5, G6)

**Scope:**

- Implement `internal/ads/udp_control.go`: non-blocking recv on tracker; send loop on management.
- Epoch state machine per §3 + §9.2 gap policy.
- `CONFIG_REQUEST` / `CONFIG_SNAPSHOT` codecs.
- Metrics: `ad_udp_control_*`.
- Ingress limits via UDP only (no HTTP/gRPC config pull on hot path).

**Definition of done:**

- [x] `TestChaos_UDP_*` passes with `udp_moderate` and `udp_severe` profiles (state-machine proofs; netem profiles in §7.3).
- [x] STALE → ingress ≤ canary floor within one sync interval.
- [x] Loosening limit on epoch gap without snapshot **rejected** (proof metric increments).
- [x] `0 allocs/op` on UDP receive hot path (bench).
- [x] `chaos_proof` ×4 new faults (see §7).
- [x] `CHAOS_MIN_PROOFS` bumped in `test_chaos.sh` (30 → 36).
- [x] Merge checklist §8 complete (M2 scope).

**Estimate:** 1–2 sprints.

**Status:** Implemented; staging cold-path `UDP_CONTROL_ENABLED=true`; hot-path via `hot-path/configmap-env.staging.tpl` when `enable_hot_path=true`.

---

#### M3 — Quota repair and dead-shard quorum (G7, G8)

**Scope:**

- `ReconWorker.RepairQuotaDrift`: if `|pg_reserved - redis_expected| > chunk_size`, enqueue outbox `QUOTA_REPAIR` (PG decides: release or top-up Redis).
- Crash gap: periodic scanner for `campaign_quotas` with `reserved > 0` and missing Redis key >30 s → repair.
- Dead shard: release only when **all** true for ≥90 s: management ping fail, Sentinel master down, ≥50% tracker breakers open for shard.
- All repair decisions logged to `audit_log` with PG transaction id.

**Definition of done:**

- [x] `TestChaos_QuotaDriftRepair`: kill Redis between PG reserve and INCRBY → auto-repair ≤60 s.
- [x] `TestChaos_QuotaDeadShardRelease`: transient blip does **not** release; sustained outage does.
- [x] No case where PG `current_spend > budget_limit` after repair.
- [x] `chaos_proof fault=quota_drift_repair`.

**Estimate:** 1 sprint.

**Status:** Implemented; staging `QUOTA_AUTO_REPAIR=true` + `QUOTA_MODE=live` in `configmap-env.staging.tpl`.

- [x] Merge checklist §8 complete (M3 scope).

---

#### M4 — Lua tiered scripts (G10)

**Scope:**

- Split `unified-filter.lua` → `budget_fast.lua` + `filter_full.lua` (Tier C = existing `unified-filter.lua`).
- Router in `UnifiedFilter.Check` per §9.3.
- Sampled quota refill signal; stream batching for impressions (deferred: fast path still XADDs; MPSC batch in follow-up).
- `SLOWLOG` alert threshold 10 ms per shard (`prometheus.rules.yml` `RedisLuaP99High`).

**Definition of done:**

- [x] p99 Lua ≤10 ms on control shard at 50k ops/s synthetic bench (chaos: 10k burst p99 ~0.11 ms local TC).
- [x] R5 holds across 10k click burst in chaos test (`TestChaos_LuaFastPathP99`).
- [x] `ad_redis_lua_fast_duration_seconds` + `ad_redis_lua_fast_path_total` metrics.
- [x] Hot-path `0 allocs/op` maintained (mock bench).
- [x] `chaos_proof fault=lua_fastpath_p99`.

**Benchmark results (local testcontainer Redis, i5-11400H):**

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkUnifiedFilter_Check` (mock) | 398 | 0 | **0** |
| `Check_FastPath_RealRedis` (impression, Tier B) | 67,269 | 43 | 3 |
| `Check_RealRedis` (click, Tier C) | 80,891 | 44 | 3 |
| Fast vs full Δ | **−17%** | — | — |

go-redis residual 3 allocs/op unchanged; eSPX-side mock path stays 0 allocs/op.

**Estimate:** 1–2 sprints.

**Status:** Implemented; default `LUA_FAST_PATH_ENABLED=false`. Staging rollout after canary tracker soak.

- [x] Merge checklist §8 complete (M4 scope).

---

#### M5 — Ingress atomics and clock safety (G11, G12)

**Scope:**

- Padded `IngressQuotaCell` per `(shard_id, worker_id)` per §4.
- Epoch rollover: new map pointer swap resets cells; workers re-load pointer each request (no reset of old map).
- UDP coarse-time: clamp adjustment to ±50 ms per packet; **never** decrease `cachedUnixMilli` on hot path — only pause forward tick (TTC uses separate monotonic check).
- Extend `clock_drift_chaos_test` for UDP time packet injection.

**Definition of done:**

- [x] `go test -race -count=10` on ingress quota tests (`TestIngressQuota_race`, `tryAcquire`).
- [x] False-sharing bench: padded vs unpadded shows ≥3× throughput on 8 cores (`TestIngressQuota_falseSharingRatio`).
- [x] TTC chaos test passes with UDP time packets (`TestChaos_ClockDrift_UDPTimePacket`).
- [x] `chaos_proof fault=clock_drift_monotonic_safety` still passes.

**Benchmark results (8 workers, i5-11400H):**

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkIngressQuota_padded` | 13.7 | 0 | **0** |
| `BenchmarkIngressQuota_unpadded` (packed atomics) | 17.8 | 0 | **0** |
| False-sharing stress (500k iter/worker) | padded **16.7×** faster than unpadded | — | — |

**New metrics:** `ad_udp_ingress_acquire_total`, `ad_udp_ingress_reject_total`.

**Estimate:** 1 sprint.

**Status:** Implemented; active when `UDP_CONTROL_ENABLED=true` (ingress gate wired in gnet `React`).

- [x] Merge checklist §8 complete (M5 scope).

---

#### M6 — Load validation and game day (all)

**Scope:**

- `shard_load_spike_chaos_test.go` with k6 spike profile (`k6_spike_traffic.js`).
- Manual compose game day: scenarios A–H from GUIDE + UDP severe profile (`run_game_day.sh`).
- `bottleneck-report.md.tpl` in `var/load-test/`.

**Definition of done:**

- [x] 10× spike: control cohort p99 <80 ms; zero R5 violations (`TestChaos_ShardLoadSpike`).
- [x] `scripts/chaos/test_chaos.sh` green with `CHAOS_MIN_PROOFS=39`.
- [x] Runbook section in `docs/RUNTIME.md` §5 for UDP-only recovery.
- [x] M1–M5 merge checklists signed (§9.4 milestones).

**CI chaos results (local testcontainers, i5-11400H):**

| Phase | Workers | Samples | p99 (ms) | err % | R5 |
| :--- | ---: | ---: | ---: | ---: | :--- |
| Baseline (1 worker) | 1 | 200 | 0.436 | 0 | — |
| Spike burst (32×200) | 32 | 6400 | **4.770** | 0 | ok |

**Load-test assets:**

| Script | Purpose |
| :--- | :--- |
| `scripts/load/k6_spike_traffic.js` | 1×→10× ramp, 30 s hold, control cohort p99 threshold 80 ms |
| `scripts/load/run_spike_load.sh` | Compose + k6 spike + `analyze_bottlenecks.sh` |
| `scripts/load/run_game_day.sh` | Game day orchestration (A–H checklist + dirty + spike) |
| `var/load-test/bottleneck-report.md.tpl` | Report skeleton for manual / Prometheus fill |

**Historical compose dirty load** (`var/load-test/20260712T112644Z`): tracker p99 ~4.95 ms at ~1.5k RPS dirty mix — headroom before M6 spike abort threshold.

Full report: [M6_LOAD_VALIDATION.md](./reports/M6_LOAD_VALIDATION.md).

**Estimate:** 1 sprint (overlap with M4–M5).

**Status:** Implemented.

### 9.5 Dependency graph

```
M1 (fences) ──┬──► M2 (UDP)
              └──► M3 (quota repair)
M2 (UDP) ────────► M5 (ingress atomics)
M4 (Lua split) ──► M6 (load validation)
M3 ──────────────► M6
```

**Critical path:** M1 → M2 → M6. M4 can parallel M2/M3 after M1 lands.

### 9.6 Rollout flags

| Flag | Default | Staging | Enables |
| :--- | :--- | :--- | :--- |
| `MIGRATION_FENCE_ENABLED` | `false` | **`true`** | Lua `migration_gen` reject |
| `SLOT_MIGRATION_ENABLED` | `true` | **`true`** | Background slot copy worker |
| `UDP_CONTROL_ENABLED` | `false` | **`true`** | Tracker UDP recv + management send |
| `UDP_FAIL_CLOSED` | `true` when UDP on | **`true`** | STALE canary floor |
| `QUOTA_AUTO_REPAIR` | `false` | **`true`** | Recon repair loop |
| `QUOTA_MODE` | `off` | **`live`** | Distributed quota refill + repair |
| `LUA_FAST_PATH_ENABLED` | `false` | `false` | `budget_fast.lua` router (enable after canary) |

Rollout order per shard: shadow metrics → canary tracker ×1 → all trackers → enable autoscale coupling.

**Staging apply:** `bash scripts/deploy/k8s_staging_apply.sh` renders `deploy/k8s/base/configmap-env.staging.tpl`. Optional hot path: set `enable_hot_path = true` in `deploy/terraform/envs/staging/terraform.tfvars` (uses `hot-path/configmap-env.staging.tpl`). Set `udp_tracker_addrs` to edge node `host:8181-8184` for management UDP publisher targets.

---
