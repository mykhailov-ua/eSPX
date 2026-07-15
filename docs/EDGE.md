# Edge layer: ingress (L4/L7) and hot state (Redis)

The ad-tech edge has two coupled layers: **network ingress** (OpenResty :8180, optional XDP) and **Redis edge state** (four client-sharded masters, Lua atomicity). Trackers on :8181–8184 bridge both via gnet + `EVALSHA`.

**See also:** [GO.md](./GO.md) (tracker runtime), [ARCHITECTURE.md](./ARCHITECTURE.md), [DEVELOPMENT.md](./DEVELOPMENT.md) (edge scripts, game day, rollout flags Part III §8).

---

# Part I — Network ingress (L4/L7)

## 1. Request pipeline (L7)

`deploy/nginx/lua/access-check.lua` — two phases:

### Phase 1 (before body)

| Step | Mechanism |
| :--- | :--- |
| Rate limit | `limit_req` (~100 r/s baseline) |
| Circuit breaker | Local breaker state |
| Blacklist | `blacklist_cache` from `edge-blacklist-sync.lua` (Redis shard 0, 5 s poll) |
| Conn limits | `limit_conn` 200/IP, 8192 global |

Abusive IPs are rejected before the tracker TCP stack. Blacklist sets must be replicated to all shards (`scripts/redis-ops/redis_reconcile_post_deploy.sh`).

### Phase 2 (after body)

- `read_body`, byte DFA validation
- Per-campaign edge rate limit (`edge-phase2.lua` / `edge-rl.lua`)
- Upstream proxy to tracker pool

### Worker 0 (`init-worker.lua`)

- `edge-config.lua` — `config:values` → `lua_shared_dict edge_config`
- Slot-map sync (`edge-slot-map.lua`), upstream health
- `GET /metrics/edge`, `GET /admin/*` → management :8188

## 2. Shard pick (must match Go)

```
slot  = crc32_castagnoli(campaign_id) & 1023
shard = slot_table[slot]
```

`deploy/nginx/lua/edge-slot-map.lua` — same Castagnoli CRC + 1024 slots as `StaticSlotSharder`. Composite `campaign_id + user_id` is for **edge rate limiting only**.

Sentinel: 5 s `sentinel_cache` TTL per shard master; fallback to `REDIS_ADDRS`.

## 3. Anti-fraud at the edge

| Control | Layer | Behavior |
| :--- | :--- | :--- |
| IP blacklist | L7 Lua | Cached from `blacklist:*` on shard 0 |
| Conn limits | L7 nginx | SYN/backlog backstop |
| Per-campaign rate | L7 Lua | Fixed window from `edge_config` |
| Shard routing | L7 Lua | Correct tracker upstream |
| Per-shard RPS cap | Tracker UDP | `UDP_CONTROL_ENABLED` — Part III §1 |
| Geo / TTC / budget / ML boost | Go + Redis Lua | [GO.md](./GO.md) §5 |

## 4. Host tuning (Phase 0)

| Artifact | Role |
| :--- | :--- |
| `deploy/edge/99-espx-edge.conf` | sysctl, TCP buffers, SYN cookies |
| `scripts/edge-tuning/edge_sysctl.sh` | Install/verify |
| `scripts/edge-tuning/edge_nic_tune.sh` | RX ring, IRQ/RSS |
| `scripts/edge-tuning/edge_baseline.sh` | SLA snapshot |

k8s hot path: ConfigMaps `nginx-edge-conf`, `nginx-edge-lua` via `k8s_hot_path_up.sh`.

## 5. Optional XDP/eBPF (L4)

Kernel DROP for blocklisted IPs and SYN floods. BPF: `deploy/edge/xdp/bpf/edge_filter.c`; maps sync from Redis via `edge-bpf-sync`.

```bash
docker build -f deploy/edge/xdp/Dockerfile -t espx-edge-xdp .
make -C deploy/edge/xdp   # manual object build
```

**Rollback:** detach XDP; revert nginx Lua. SLA targets: p95 < 50 ms, p99 < 80 ms on tracker path.

## 6. SLA and load validation

Game day: `scripts/load-test/run_game_day.sh`. Abort if control-cohort p99 &gt; 80 ms for 30 s.

---

# Part II — Redis hot state

## 1. Topology

- **N = 4** standalone masters (`config.ExpectedRedisShardCount`)
- Not Redis Cluster — no `MOVED`, no cross-shard multi-key Lua
- Compose: masters + replicas + Sentinel ×3

## 2. Routing mathematics (Static Slot Map)

$$
\text{slot} = \text{CRC32C}(\text{campaign\_id}) \land 1023,\quad
\text{shard} = \text{slot\_table}[\text{slot}]
$$

```go
slot := crc32Castagnoli(&campaignID) & 1023
shard := staticSlotSharder.slots[slot]
```

| Consumer | Cost / notes |
| :--- | :--- |
| Go `StaticSlotSharder` | ~5.6 ns/op, 0 allocs/op (HW CRC) |
| Nginx `edge-slot-map.lua` | Must match Go |
| `JumpHashSharder` (tests) | ~84% divergence vs StaticSlot at N=4 |

| | StaticSlot | JumpHash |
| :--- | :--- | :--- |
| Hot-path | O(1) table | Float loop |
| N+1 remap | ~67% keys | ~1/N keys |

## 3. Shard health coefficient ($K_{\text{state}}$)

$$
D = 0.40\,M_{\text{cpu}} + 0.35\,M_{\text{lat}} + 0.15\,M_{\text{ram}} + 0.10\,M_{\text{err}},\quad
K_{\text{state}} = \max(0,\ 1 - D)
$$

Feeds autoscaling and UDP ingress epochs. Canary when $K_{\text{state}} &lt; 0.2$ (~5% probe quota).

## 4. Failover and circuit breakers

**Sentinel:** quorum 2, 5 s subjective down, ~10–15 s recovery.

**Go breaker:** open after 150 errors, half-open probe at 5 s (`redis_breaker.go`).

## 5. Lua scripts and tiered routing

| Tier | Script | When |
| :--- | :--- | :--- |
| B | `budget-fast.lua` | Impressions, `LUA_FAST_PATH_ENABLED` |
| C | `unified-filter.lua` | Clicks, TTC, fcap, debit + `XADD` |

**12 KEYS** (unified filter): `rl:ip`, `dup`, `budget:campaign`, `idempotency`, `budget:sync:*`, `budget:dirty_*`, `ad:events:stream`, `budget:daily_spent`, `fcap`, `imp_ts`.

Return `-1` → budget reload; return `11` → migration fence reject.

**Quota keys** (`QUOTA_MODE=shadow|live`): `budget:quota`, `budget:refill_needed`, `budget:refill_lock`, `budget:migration_gen`.

Hot path: `EVALSHA` only; `SCRIPT LOAD` at tracker startup.

## 6. Shard 0 and global replication

**Shard 0 (by policy):** `campaigns:update` pub/sub, auth lockout, brand creatives.

**Replicated to all shards:** `config:values`, `config:version`, `blacklist:*`, `brand:creatives:*`, `ml:score:boost:{campaign_id}`.

Outbox pipelines writes; `SettingsWatcher` reads first responsive shard.

## 7. Key layout

| Pattern | Scope |
| :--- | :--- |
| `budget:campaign:{id}` | Shard-local live budget |
| `budget:sync:*`, `budget:dirty_*` | PG sync deltas |
| `dup:*`, `idempotency:*`, `fcap:*`, `imp_ts:*` | Shard-local filters |
| `ad:events:stream` | Per-shard stream |
| `config:*`, `blacklist:*` | All shards |

Campaign migration: `scripts/redis-ops/redis_migrate_campaign.sh` (pause → migrate → verify).

---

# Part III — Control plane (UDP, migration, chaos)

### 1. Control Protocol

### Implemented: outbox and slot map versioning

| Mechanism | Idempotency | Ordering |
| :--- | :--- | :--- |
| `outbox_events` | Event type + payload hash; `FOR UPDATE SKIP LOCKED` | `created_at ASC` (priority lanes planned: blacklist/pause first) |
| `slot_map_versions` | Monotonic version ID; checkpoint per campaign in PG | Draft → MIGRATING → ACTIVE; rollback without ledger loss |
| `config:version` | Monotonic string on every shard | Trackers iterate shards until version responds |

### UDP quota datagram (implemented)

Topology sync for ingress RPS limits uses UDP when `UDP_CONTROL_ENABLED=true`. Packet loss and out-of-order delivery are handled by monotonic **Epoch ID** and a **Config Hash** for idempotency.

#### Datagram layout

| Field | Size | Notes |
| :--- | :--- | :--- |
| Magic | 4 B | `0xESPX` |
| Master coarse time | 8 B | Wall-clock alignment |
| Epoch ID | 8 B | Monotonic |
| Config hash | 32 B | Idempotency / snapshot match |
| Payload length | 2 B | |
| Padding | 2 B | |
| Shard ID | 4 B | |
| Quota limit RPS | 8 B | Per-shard ingress cap |

#### Tracker receive state machine

1. **Validate:** check magic and config hash; drop corrupted datagrams at the socket boundary.
2. **Epoch selection:**
   - `Packet.Epoch <= Tracker.CurrentEpoch` → stale; drop (out-of-order).
   - `Packet.Epoch == Tracker.CurrentEpoch + 1` → linear update; atomic pointer swap on quota map.
   - `Packet.Epoch > Tracker.CurrentEpoch + 1` → gap detected (packet loss); skip missing steps and apply the current epoch snapshot (full limit set per shard).
3. **Stale channel fallback (UDP-only):** if no valid UDP packet arrives for $2 \times$ sync interval, mark the channel `STALE`, **tighten ingress to canary floor** (5% of last-known limit or hard floor 100 RPS/shard), and emit a `CONFIG_REQUEST` datagram (see Part III §7.2). Management responds with a redundant `CONFIG_SNAPSHOT` burst (3× unicast + 1× broadcast per tracker). **No TCP, no HTTP/gRPC** on the control path — Postgres remains the only durable authority for money; UDP carries only ingress limits and routing metadata.

4. **Epoch gap policy (financial-safe):**
   - If `Packet.Epoch > CurrentEpoch + 1` and the snapshot **tightens** limits (lower RPS or higher $K_{\text{state}}$ penalty) → apply immediately (fail-closed on uncertainty).
   - If the snapshot **loosens** limits → require contiguous epoch chain **or** a `CONFIG_SNAPSHOT` signed with `Config Hash` matching Postgres `control_plane_epochs` row. Never loosen ingress on a gap alone.

#### 1.1 UDP-only recovery runbook

When `UDP_CONTROL_ENABLED=true`, ingress limits arrive **only** over UDP. There is no HTTP/gRPC config pull on the hot path.

| Signal | Meaning |
|--------|---------|
| `ad_udp_ingress_reject_total` rising | Epoch stale or quota map not refreshed |
| `ad_tracker_health_degraded` | Redis or filter path unhealthy |
| Ingress 429 on `/track` | Per-shard RPS cap from `IngressQuotaCell` |
| STALE channel | No valid packet within `2 × sync_interval` |

**Recovery:**

1. Confirm blast radius (one shard / one tracker if possible).
2. Verify `UDP_TRACKER_ADDRS` lists every tracker `host:8191`.
3. On STALE, tracker emits `CONFIG_REQUEST`; management responds with `CONFIG_SNAPSHOT` burst from Postgres `control_plane_epochs`.
4. With `UDP_FAIL_CLOSED=true`, STALE applies canary floor — do not raise limits via Redis `budget:*` (UDP never writes budget keys).
5. Verify `ad_udp_ingress_acquire_total` recovers, control p99 &lt; 80 ms for 30 s, `AssertBudgetInvariant` ±1 micro.

| Network profile | loss | delay | Expected |
|-----------------|------|-------|----------|
| `udp_light` | 1% | 0 | Epoch monotonic |
| `udp_moderate` | 5% | 2 ms | STALE → canary floor |
| `udp_severe` | 20% | 10 ms | CONFIG_REQUEST recovery ≤3 s |

Abort game day if control p99 &gt; 80 ms for 30 s or R5 violation. See [DEVELOPMENT.md](./DEVELOPMENT.md) (load testing).

---

### 2. Hot-Path Memory and Concurrency Model

See [GO.md](./GO.md) for gnet worker pools, zero-alloc policy, BCE, and monotonic deadlines. Summary:

### Coarse time

A background goroutine reads the kernel clock every 10–20 ms and publishes a coarse timestamp via atomic store. gnet workers read it with atomic load. On UDP quota receipt, the master coarse time field re-aligns local clocks; if local time runs ahead, the ticker pauses until alignment.

**Principle:** separate **wall-clock** (control plane, logging, schedules) from **monotonic time** (filter deadlines, TTC). Chaos scenario D (`clock_drift_chaos_test.go`) proves monotonic deadlines survive +3600 s wall-clock drift.

### Cache-line isolation and false sharing

On x86_64, cache lines are 64 bytes. Adjacent fields in the same line cause false sharing when independent goroutines update them, reducing scalability on multi-core hosts.

**Mitigations in eSPX:**

| Pattern | Technique | Where |
| :--- | :--- | :--- |
| Per-worker counters | Pad hot fields to 64-byte boundaries; one quota counter per gnet loop | UDP quota map (`IngressQuotaCell`) |
| MPSC ring indices | Separate `write` and `read` indices with 64-byte padding between them | `MPSCQueue` (`worker_pool.go`) |
| Immutable config swap | Readers never touch writer fields; whole `slotTable` / alias table replaced atomically | `StaticSlotSharder`, `HybridBalancer` |
| Breaker state | Per-shard `int32` state via CAS, not a global mutex | `RedisBreaker` |

**Structure-of-arrays vs array-of-structures:** when N workers each own a counter, an array of padded structs (AoS with padding) keeps each counter on its own line. A flat `[]int64` of counters would pack multiple counters per line and cause cross-core invalidation under load.

### Lock-free quota check

Each gnet event loop owns a worker-local quota cell: `MaxAllowed` (limit for the current epoch) and `CurrentOps` (atomic counter). The check is O(1):

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

### 3. Graceful Degradation and Rolling Drain

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

### 4. SLA Criteria

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

### 5. Testing Standards (Sharding Profile)

Testing follows [GUIDE_CHAOS_RELIABILITY_RU.md](../GUIDE_CHAOS_RELIABILITY_RU.md) (R1–R10). This section **extends** the global guide for sharding, UDP control plane, Lua bottlenecks, and financial invariants. **Postgres is the single point of trust** for all budget reconciliation assertions.

### Mapping to global requirements

| Global rule | Sharding extension |
| :--- | :--- |
| R1 steady-state | Per-shard RPS, Lua p99, PG R5 on **every** shard touched by fault |
| R2 real faults | `tc netem` loss/jitter on **UDP control port** and Redis data port separately |
| R3 blast radius | One shard per CI experiment; migration tests use fenced cohort only |
| R4 unreliable network | UDP duplicate/loss/reorder injected; idempotency via epoch + config hash |
| R5 anti-slop | ≥32 goroutines on budget/quota/migration paths; `-race` mandatory |
| R7 chaos_proof | New faults listed in Part III §5.4; `CHAOS_MIN_PROOFS` incremented per new proof |
| R9 experiment design | Document network impairment % alongside fault name |
| R10 overhead | Required for: Lua split, UDP codec, migration fence, quota repair |

### Test pyramid

| Level | Scope | Examples |
| :---: | :--- | :--- |
| 4 | Chaos | testcontainers, SIGKILL, partition, clock drift |
| 3 | E2E | Nginx → tracker → Redis → Postgres/ClickHouse |
| 2 | Smoke | `verify_redis_topology.sh`, `check_deps.sh` |
| 1 | Unit | `StaticSlotSharder`, Lua, zero-alloc benches |

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

**Sharp load rise:** use `scripts/load-test/k6_dirty_traffic.js` or pinned `CampaignIDForShard` cohort; ramp 1×→10× over 10 s, hold 30 s, ramp down. Abort if control-cohort p99 >80 ms for 30 s or any R5 violation.

### Experiment design (R9)

Each test documents:

1. **Hypothesis** — steady-state metric X stays within Y under fault Z.
2. **Control cohort** — campaigns on unaffected shards.
3. **Fault** — exactly one variable (R10: no multi-shard kill in CI).
4. **Abort** — invariant violation or p99 > 80 ms for 30 s.
5. **Proof** — stdout line: `chaos_proof fault=<name> <key>=<value>`.

CI gate: `./scripts/chaos-drills/test_chaos.sh` with `CHAOS_MIN_PROOFS ≥ 46`.

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
- Smoke before release: `scripts/redis-ops/verify_redis_topology.sh` confirms `len(REDIS_ADDRS) == ExpectedRedisShardCount`.
- Load: record baseline in `var/load-test/<run>/` per `scripts/load-test/snapshot_runtime.sh`.

### Observability during chaos (R8)

Monitor:

- `ad_tracker_health_degraded`
- `ad_redis_breaker_state` (0=closed, 1=half-open, 2=open)
- `ad_redis_lua_noscript_total`
- `ad_redis_lua_duration_seconds` (p50/p99 per shard)
- `ad_udp_control_epoch_lag`
- `ad_udp_control_stale_total`
- `ad_processor_stream_lag_seconds`
- `ad_management_outbox_oldest_pending_seconds`
- `ad_quota_drift_micro` (PG − Redis expected reserved)

### When chaos is unnecessary (R10)

Skip new chaos tests for: HTMX/CSS, read-only admin handlers, comment-only changes, dead code removal. **Required** for: new Lua scripts, shard routing changes, outbox events, quota/budget mutations, slot map logic.

---
### 6. Change validation

Control-plane and sharding changes require:

- Steady-state metrics defined (Part III §4): `ad_udp_control_*`, `ad_quota_*`, migration fence counters
- Integration tests on real Redis/Postgres (`testcontainers-go`); no `sqlmock` on money paths
- Budget invariant `AssertBudgetInvariant` (±1 micro-unit) on affected cohorts
- `chaos_proof` lines for new fault classes; `./scripts/chaos-drills/test_chaos.sh` with `CHAOS_MIN_PROOFS=46`
- Nginx `edge-slot-map.lua` updated when Go routing changes
- UDP-only control path: no TCP/HTTP config pull on the hot path

Chaos tests are not required for HTMX/CSS-only changes, read-only admin handlers, or comment-only diffs. Required for new Lua scripts, shard routing, outbox event types, quota/budget mutations, and slot-map logic.

---

### 7. Design constraints

1. Postgres is authoritative for spend, reservations, slot-map versions, and control-plane epoch history. Redis is a per-shard linearizable cache/executor. UDP carries ingress limits only.
2. Budget debit stays in one Lua script per event. Cross-store quota uses Postgres row locks before Redis credit.
3. Migration generation tokens reject debits on draining shards.
4. UDP loss, epoch gaps, and STALE channel tighten ingress; limits do not loosen without a signed snapshot.
5. Ingress RPS (UDP + local atomics) is separate from budget authority (Lua + Postgres).

#### Resolved control-plane gaps

| Gap | Mitigation |
| :--- | :--- |
| Slot migration dual debit | Lua `migration_gen` fence + Postgres `migration_gen` column |
| Torn slot-map read | Single `atomic.Value` snapshot (`SlotMapSnapshot`) |
| `LocalQuotaCache` race | Atomic slot snapshot per cache line |
| HTTP/TCP control fallback | UDP `CONFIG_REQUEST` / `CONFIG_SNAPSHOT` only |
| UDP epoch gap loosens limits | Tighten-only on gap; loosen requires PG-signed snapshot |
| STALE channel holds high limit | Fail-closed canary floor (5% or 100 RPS/shard) |
| PG→Redis quota crash gap | `ReconWorker` auto-repair + `QUOTA_REPAIR` outbox |
| Dead-shard reservation release | Quorum: Sentinel + tracker breakers + sustained ping (≥90 s) |
| Outbox backlog for safety events | Priority lanes: blacklist, pause, freeze before bulk sync |
| Lua CPU at scale | Tiered scripts (A/B/C) + Go pre-gates |
| Ingress counter false sharing | Padded `IngressQuotaCell` per worker |
| UDP coarse time vs TTC | Monotonic-only filter deadlines; wall clock clamp ±50 ms/packet |

#### UDP message types

Port pair: management `:8190` → trackers `:8191`. No TCP on the control path.

| Type | Direction | Purpose |
| :--- | :--- | :--- |
| `QUOTA_EPOCH` | Mgmt → Tracker | Per-shard RPS limit + epoch (Part III §1) |
| `CONFIG_SNAPSHOT` | Mgmt → Tracker | Full limit vector + slot-map version + config hash |
| `CONFIG_REQUEST` | Tracker → Mgmt | Snapshot request with `tracker_id`, `last_epoch`, `config_hash` |
| `MIGRATION_BARRIER` | Mgmt → Tracker | `migration_gen` + draining slots bitmap |

Reliability: each epoch sent 3× unicast + 1× broadcast per sync interval; dedupe by `(epoch_id, config_hash)`. On `CONFIG_REQUEST`, management reads Postgres `control_plane_epochs` and responds with a 5× `CONFIG_SNAPSHOT` burst. Trackers keep last-good snapshot in process memory only.

Postgres: `control_plane_epochs` (epoch history), `campaigns.migration_gen` (fencing token).

#### Lua tiering

Redis executes scripts single-threaded. Minimize work inside the atomic window; keep debit atomic.

| Tier | Script / layer | Scope |
| :--- | :--- | :--- |
| A | Go pre-gates | Emergency breaker, geo, schedule, UDP ingress gate, `migration_gen` check — 0 Redis RTT |
| B | `budget_fast.lua` | ≤5 keys: spend, idempotency, sync, stream — impressions when `LUA_FAST_PATH_ENABLED` |
| C | `unified-filter.lua` | TTC, fcap, pacing, quota refill — clicks and complex paths |

Tier B target: ≤30 µs Lua CPU at p99. Tier C when TTC/fcap/pacing active or 1% quota-refill sample.

Idempotency, debit, and sync counter stay in one script per tier.

Benchmark (testcontainer Redis, i5-11400H):

| Benchmark | ns/op | allocs/op |
| :--- | ---: | ---: |
| `BenchmarkUnifiedFilter_Check` (mock) | 398 | 0 |
| `Check_FastPath_RealRedis` (Tier B) | 67,269 | 3 |
| `Check_RealRedis` (Tier C) | 80,891 | 3 |

go-redis adds ~3 allocs/op on real Redis RTT. eSPX mock path: 0 allocs/op.

Ingress quota (`UDP_CONTROL_ENABLED`): `BenchmarkIngressQuota_padded` 13.7 ns/op, 0 allocs/op; padded layout 16.7× throughput vs packed atomics (8 workers, 500k iter/worker).

#### Load validation

| Asset | Purpose |
| :--- | :--- |
| `scripts/load-test/k6_spike_traffic.js` | 1×→10× ramp, 30 s hold; abort if control p99 > 80 ms |
| `scripts/load-test/run_spike_load.sh` | Compose + k6 + `analyze_bottlenecks.sh` |
| `scripts/load-test/run_game_day.sh` | Scenarios A–H + dirty + spike |
| `internal/ingestion/shard_load_spike_chaos_test.go` | CI spike proof (`fault=shard_load_spike`) |

CI spike (testcontainers, 32 workers, 6400 samples): p99 4.77 ms, 0% errors, R5 ok.

---

### 8. Rollout flags

| Flag | Default | Staging | Enables |
| :--- | :--- | :--- | :--- |
| `MIGRATION_FENCE_ENABLED` | `false` | `true` | Lua `migration_gen` reject |
| `SLOT_MIGRATION_ENABLED` | `true` | `true` | Background slot copy worker |
| `UDP_CONTROL_ENABLED` | `false` | `true` | Tracker UDP recv + management send |
| `UDP_FAIL_CLOSED` | `true` when UDP on | `true` | STALE canary floor |
| `QUOTA_AUTO_REPAIR` | `false` | `true` | Recon repair loop |
| `QUOTA_MODE` | `off` | `live` | Distributed quota refill + repair |
| `LUA_FAST_PATH_ENABLED` | `false` | `false` | `budget_fast.lua` router |

Rollout order per shard: shadow metrics → canary tracker ×1 → all trackers → autoscale coupling.

Staging apply: `bash scripts/k8s/k8s_staging_apply.sh` renders `deploy/k8s/base/configmap-env.staging.tpl`. Hot path: `enable_hot_path = true` in `deploy/terraform/envs/staging/terraform.tfvars`. Set `udp_tracker_addrs` to edge node `host:8181-8184` for management UDP targets.
