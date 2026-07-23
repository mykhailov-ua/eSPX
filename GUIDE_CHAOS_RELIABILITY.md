# Guide: Chaos Engineering and System Reliability

**Status:** Mandatory  
**Target Audience:** All engineers working with ingestion, billing, or infrastructure  
**Related Documents:** `.cursorrules`, `GUIDE_STYLE_CODE.md`, `scripts/chaos-drills/test_chaos.sh`, `.github/workflows/sentinel-chaos.yaml`

## Goal

Define how eSPX validates fault tolerance under real-world failure scenarios. Align with the [Principles of Chaos Engineering](https://principlesofchaos.org/) and Netflix-style failure taxonomy. Replace mock-heavy happy-path tests with experiments proving steady-state behavior, limited blast radius, and financial correctness after partial outages. **Also define when chaos tests are redundant** to prevent wasting CI execution time on non-distributed risks (R10).

Fundamental Concepts:
1. **Chaos Engineering** ([Principles of Chaos](https://principlesofchaos.org/), Netflix): Steady-state hypothesis, real-world failure injection, CI automation, and blast radius minimization.
2. **Distributed Systems** (DDIA): Unreliable networks, clocks, and processes; crash-recovery model; filtering stale leaders (fencing).
3. **OpenRTB 3.0 Latency Budget**: Tracker processing must remain within ~20 ms to satisfy the overall bidding/tracking SLA.

## Scope

**In Scope:**
1. Ingestion hot path (`internal/ingestion`), stream processor, and management transactional outbox.
2. Redis Sentinel failover, stream backpressure, and campaign budget invariants.
3. Chaos validation in CI (`chaos_proof` lines) and redundancy criteria (R10).

**Out of Scope:**
1. Internal mechanics of third-party SDKs.
2. Fine-tuning load-testing agent configurations.

---

## Industry Standards Alignment

### Principles of Chaos Engineering (Canonical Loop)

Based on [principlesofchaos.org](https://principlesofchaos.org/) (Netflix, 2015-2019). Every eSPX experiment **MUST** follow these steps:
1. **Define Steady State** — A measurable output indicating normal behavior. In eSPX: RPS, p95/p99 latency, error rates, and budget drift (R1).
2. **Hypothesize Stability** — Assert that the steady state will continue in both control and experimental groups. Formulate this before injecting the fault; use control shards/campaigns where possible.
3. **Inject Real-World Variables (Faults)** — SIGKILL, network partitions, packet loss, latency, clock drift, or database unavailability (R2).
4. **Disprove the Hypothesis** — Compare metrics; abort the experiment and roll back if the threshold is breached; resolve the underlying vulnerability.

Additional principles:
1. **Steady State = System Output** — Measure throughput, error rates, and latency — not internal metrics like goroutine counts. In eSPX: Prometheus metrics + `chaos_proof` logs.
2. **Real-World Event Variety** — Prioritize faults by likelihood and impact. In eSPX: Failure classification is detailed below.
3. **Run Under Realistic Local Load** — The traffic profile must resemble production, not simple mock units. In eSPX: `docker compose` stack + OpenRTB `test: 1`; CI uses the `testcontainers-go` library as a baseline.
4. **Automate Experiments Continuously** — Manual game days do not scale. In eSPX: `scripts/chaos-drills/test_chaos.sh`, `.github/workflows/sentinel-chaos.yaml`.
5. **Minimize Blast Radius** — Limit the impact of any failure; implement emergency abort triggers. In eSPX: One shard, one campaign, and circuit breakers (R3).

### Netflix Simian Army Alignment

We map Simian Army failure classes directly to our experiments and code paths:
1. **Chaos Monkey** (instance termination) — `redis_container_terminate`, `postgres_container_terminate`, tracker/processor SIGKILL recovery. (Levels L2–L5).
2. **Latency Monkey** (dependency degradation) — Slow Redis commands, `replication_high_latency`, filter timeouts, and circuit breakers. (Levels L2–L4).
3. **Chaos Gorilla** (multishard outage) — Shard 0 failure (Scenario A), single Redis master outage (Scenario B). (Level L3).
4. **Chaos Kong** (region outage) — **Manual local drill only** — not automated in CI. Document the disaster recovery runbook; do not stop all compose services simultaneously in CI. (Levels L1–L5).
5. **Conformity Monkey** (config drift / SPOF) — Topology validation (`verify_redis_topology.sh`), shard count checks against `REDIS_ADDRS`, and circuit breaker metrics. (Ops).
6. **Doctor Monkey** (unhealthy instances) — Health checks (`ad_tracker_health_degraded` metric) and DLQ length alerts. (Levels L2–L4).

FIT (Failure Injection Testing, Netflix 2014): Injecting faults at the request level at the system boundary. In eSPX: Corrupted track request payloads, Stripe webhook replays, and the RTB chaos matrix (`TestChaos_A1_*` ... `TestChaos_H2_*`).

ChAP (Chaos Automation Platform): Comparing control and experimental cohorts. In eSPX: Processing campaigns on shard N while shard 0 is down; comparing p99 latency and budget spend on unaffected shards.

---

## Requirements

### R1. Steady-State Hypothesis

Before running any chaos experiment, define the target metrics for the steady state. eSPX defaults:
1. **RPS on `/track`** — Stable, without unexplained drops.
2. **Latency** — p95 < 50 ms, p99 < 80 ms, hard wall-time ceiling of 100 ms.
3. **Error Rate** — < 0.1% failed responses (excluding valid filter rejections: 202/204/200).
4. **Budget Drift** — Redis spend + sync deltas ≈ Postgres spend within a 1-hour window (`ReconWorker`).

You **MUST** measure the baseline before fault injection. You **MUST** abort the experiment if the steady state degrades beyond the acceptable limit.

### R2. Real-World Fault Injection

You **MUST** simulate failures directly in the local docker-compose environment:
- Sending SIGKILL to Redis master or replica nodes.
- Network partitions, packet loss, and latency jitter.
- CPU throttling and I/O bottlenecks.
- Clock drift (NTP offsets, leap second simulation).
- Postgres/ClickHouse database unavailability.

Unit test mocks **MUST NOT** be the sole proof of system resilience. Validate behavior under `docker compose` with OpenRTB `test: 1` before merging PRs.

### R3. Blast Radius Isolation

You **MUST** isolate experiments:
- Client-sharding: Redis shard failure affects only ~1/N campaigns.
- Circuit breakers on trackers, processors, and the ingress boundary (Nginx/OpenResty).
- CI chaos suites abort if invariant checks fail or proof count limits are violated.

### R4. Distributed Systems Invariants

1. **Unreliable Network** — Idempotency on all write paths. Clicks: `idempotency:click:{click_id}` key in Lua scripts + `sync_idempotency` in Postgres. ClickHouse: `insert_deduplicate=1` on `ReplicatedMergeTree` with a SHA-256 block token.
2. **Unreliable Clocks** — Hot-path timeouts **MUST** use monotonic time (`nanotime`), not wall-clock time. Do not order distributed events by absolute time; use leader epochs and offsets instead.
3. **Process Pauses (GC, Swapping)** — Distributed writes **MUST** use fencing tokens (monotonically increasing epochs). Stale leaders **MUST** be rejected at the storage layer.
4. **Failure Model** — Assume crash-recovery, not Byzantine failures. Validate payloads at the boundary (Nginx DFA) before parsing in the gnet network layer.

### R5. Anti-AI-Slop Standards

**FORBIDDEN:** relying on mocks for budget logic, click deduplication, or Lua scripts in integration tests.
1. **Real Databases** — Integration tests **MUST** run against real Redis and Postgres instances via `testcontainers-go`.
2. **Concurrency** — Business logic tests **MUST** spawn at least 20 concurrent goroutines competing for the same campaign/user/click.
3. **Zero-Allocation Policy** — Hot-path benchmarks **MUST** run with `0 allocs/op` (`go test -benchmem`). Heap allocations during parsing, filtering, or responding are considered defects.
4. **Self-Healing** — Tests **MUST** prove automatic recovery after dependency restoration: reconnecting, closing circuit breakers, and draining stream backlogs without operator intervention.
5. **State Invariants** — Upon chaos test completion, state consistency must satisfy: `Σ Redis spend + Σ sync deltas = Postgres spend`. Any micro-unit discrepancy **MUST** fail the test.

### R6. Testing Pyramid

```
┌─────────────────────────────────────────────────────────┐
│ 4. CHAOS         testcontainers, SIGKILL, partition...  │
├─────────────────────────────────────────────────────────┤
│ 3. E2E           Nginx → Tracker → Redis → PG/CH        │
├─────────────────────────────────────────────────────────┤
│ 2. SMOKE         Dependencies, health, topology checks  │
├─────────────────────────────────────────────────────────┤
│ 1. UNIT          Zero-alloc, table-driven, Lua tests    │
└─────────────────────────────────────────────────────────┘
```

**Unit:** Boundary checks, tables cases, invalid UTF-8, MaxInt64 inputs, and allocation benchmarks.  
**Smoke:** `check_deps.sh`, `smoke_local.sh`, and `verify_redis_topology.sh`. Verifies ports, database availability, and Sentinel configurations.  
**E2E:** End-to-end flow from HTTP request to persistent storage. Validates routing maps by `campaign_id` and idempotent `click_id` replays.  
**Chaos:** Scenarios from the catalog; each successful run **MUST** log a `chaos_proof` metric. Refer to R10 before adding chaos tests.

### R7. `chaos_proof` Logging Protocol

Every passing chaos test **MUST** print to stdout:
```text
chaos_proof fault=<fault_name> <key>=<value> ...
```
The CI process (`scripts/chaos-drills/test_chaos.sh`) counts unique proof lines. It **MUST** meet the `MIN_PROOFS` threshold (default 52, set via `CHAOS_MIN_PROOFS`; M3 licensing proofs included). The build **MUST** fail if the count is below this threshold.

### R8. Observability During Chaos

During experiments, monitor:
1. **`ad_tracker_health_degraded`** — 1 while a shard is down; 0 after recovery.
2. **`ad_redis_breaker_state`** — 0: Closed, 1: Half-Open, 2: Open.
3. **`ad_redis_lua_noscript_total`** — Brief spike on deploy or after `SCRIPT FLUSH`.
4. **`ad_processor_stream_lag_seconds`** — Bounded lag that decreases post-partition recovery.
5. **`ad_management_outbox_oldest_pending_seconds`** — Alert triggers if > 30 s.

Alerting thresholds (Alertmanager): p99 > 80 ms (for 30 s), circuit breaker open > 5 min, DLQ size > 100.

### R9. Experiment Design

Each chaos experiment or manual game day **MUST** document:
1. **Hypothesis** — One sentence: Steady-state metric X remains within Y under fault Z.
2. **Control Group** — Unaffected shards, campaigns, or baseline runs before the fault.
3. **Variable (Fault)** — Specific fault type from the classification table.
4. **Abort Criteria** — Metrics or invariant breaches requiring immediate termination.
5. **Proof** — Stdout line matching `chaos_proof fault=<name> ...` on success (R7).

Use monotonic deadlines on the hot path (R4). Run control and experimental groups concurrently where possible (e.g., shards 1-3 run normally while shard 0 fails).

Experiment scopes:
1. **CI (`TestChaos_*`)** — Executed on push via `test_chaos.sh` using local testcontainers.
2. **Local Compose Scenarios (A-H)** — Manual runs on a full `docker compose` stack before release.

### R10. When Chaos Tests Are Redundant (Overhead)

Chaos engineering introduces compute overhead and risks flaky tests. You **MUST NOT** add or require chaos tests if code changes cannot affect the distributed steady state.

**Change Type → Required Test Category → Chaos Required?**
1. Pure function; no I/O, no goroutines, no shared state — Unit + hot-path `-benchmem` — **No**.
2. HTMX templates, CSS, static dashboard UI changes — Smoke / visual check — **No**.
3. Read-only admin handler; no new store calls — `go test -short` + integration DB test if SQL changed — **No**.
4. Default config value change; topology unchanged — `verify_redis_topology.sh`, `check_deps.sh` — **No**.
5. Refactoring preserving external behavior — Verify existing chaos tests pass; no new faults injected — **No** (unless hidden races are suspected).
6. Dead code removal, renaming, comments — `go test -short` — **No**.
7. Third-party SDK update; no changes on the calling side — Vendor tests + `-short` — **No**.
8. Helper CLI tool (`admin`, `dlq`) off the payment/ingest path — Unit + manual smoke — **No**.
9. Feature-flagged disabled code — Unit tests; defer chaos tests until enabled — **Defer**.
10. New write path, outbox, stream, or budget mutation — Integration + invariants + `chaos_proof` — **Yes**.
11. New Redis Lua script or shard routing logic — Real Redis integration + shard outage test — **Yes**.
12. Authorization, payment, or settlement changes — Concurrent fault injection — **Yes**.
13. Slot migration, autoscaling, or fencing logic — Dedicated `TestChaos_*` suite — **Yes**.

Redundancy guidelines:
- **Duplicate Faults:** If a client's fault behavior is already covered (e.g., standard `redis_container_terminate`), do not duplicate SIGKILL without testing a new unique invariant.
- **Uncontrolled Blast Radius:** Simulating total cluster destruction that freezes the host is forbidden in CI; document as a manual game day instead.
- **Mocks in Integration Tests:** Violates R5; rewrite tests with real stores rather than forcing fake proofs.

If financial correctness or ingestion SLA can regress, chaos tests are **not** redundant.

---

## Standard Testing Practices

Standard tests must pass before code is merged.

### 1. Unit Tests
* **Table-Driven Tests:** All business rules (fcap, OpenRTB parsing, API key validation, pacing) must use table-driven cases.
* **Parallel Execution:** Use `t.Parallel()`. Test suites must not share mutable global state.
* **Boundary Values:** Explicitly verify `nil`, empty inputs, `math.MaxInt64`, negative values, and corrupted UTF-8.

### 2. Integration Tests
* **No Database Mocks:** Mock libraries like `sqlmock` are forbidden. Use real databases via `testcontainers-go`.
* **Transaction Isolation:** DB integration tests must run in isolated transactions with automatic rollback (`defer tx.Rollback()`) or clean test schemas.
* **Concurrency:** Methods modifying balances or limits must run under at least 20 concurrent goroutines with race detection enabled (`-race`).

### 3. Benchmarks and Memory Analysis
* **Zero-Allocation Policy:** Ingestion hot-path code (`internal/ingestion`) must maintain zero allocations:
  ```bash
  go test -bench=. -benchmem ./internal/ingestion/...
  ```
  Target: **0 allocs/op**.
* **Compiler Analysis (Escape & Inline):** Verify variables do not escape to the heap:
  ```bash
  go build -gcflags="-m -m" ./internal/... 2>&1 | grep "escapes to heap"
  ```
  Optimize functions for compiler inlining on the hot path:
  ```bash
  go build -gcflags="-m" ./internal/... 2>&1 | grep "can inline"
  ```
* **Bounds Check Elimination (BCE):** Verify bounds check elimination in hot loops:
  ```bash
  go build -gcflags="-d=ssa/prove/debug=1" ./internal/...
  ```

### 4. Infrastructure Smoke Tests
* Run before merging:
  - `scripts/ci/check_deps.sh` (ports and backend versions).
  - `scripts/redis-ops/verify_redis_topology.sh` (Sentinel topology and replica count match `REDIS_SHARD_COUNT`).

---

## Instructions

### Adding a New Chaos Test
1. Confirm the chaos test is not redundant (R10).
2. Select a fault type; formulate the steady-state hypothesis (R9).
3. Implement using `testcontainers-go` or container network tools (`tc`, `iptables`). No mocks.
4. Run at least 20 parallel operations to test for races.
5. Assert invariants (budget correctness, queue length, circuit breaker state).
6. Log a matching `chaos_proof fault=<name> ...` line on success.
7. Document the new fault in the catalog if it introduces a new class.

### Chaos Redundancy Checklist (R10)
Does the change introduce or modify:
- Distributed writes, lock leases, fencing tokens, or transactional outbox rows?
- Concurrency windows where budget totals can diverge?
- New dependencies on the `/track`, stream read, or settlement paths?
- Scenarios where Redis/PG failure causes stale config reads?

If all answers are **no** and changes are local or read-only, unit tests or `-short` suites are sufficient.

### Running Chaos Tests Locally
```bash
./scripts/chaos-drills/test_chaos.sh
# Override threshold:
CHAOS_MIN_PROOFS=52 ./scripts/chaos-drills/test_chaos.sh
```

---

## Reference Materials

### OpenRTB Latency Degradation Factors
The tracker has a ~20 ms processing budget. Common bottlenecks and mitigations:
1. **Go GC Pauses** — Causes worker thread starvation. Mitigation: Hot-path zero-allocations, vtproto message pools, DFA scanners, and `unsafe.String` conversions.
2. **Redis Lua Blockages** — Redis executes Lua single-threaded. Mitigation: Minimize CPU work inside `unified-filter.lua`; align keys to single shards via `StaticSlotSharder`.
3. **Redis RTT Latency** — Sequential network commands. Mitigation: Execute exactly one atomic Lua script per request (one network hop).
4. **Postgres Deadlocks** — Concurrent updates on campaigns. Mitigation: Sort update arrays by `campaign_id, event_date` before running CTE updates.
5. **Connection Pool Saturation** — Blocks worker threads. Mitigation: Use isolated pools and scale timeouts dynamically based on remaining request wall time (`filter_context.go`).
6. **Outbox Latency** — Stale edge config values. Mitigation: Poll outbox rows using `SKIP LOCKED`.

### Fault Architecture Levels

```
[ LEVEL 1: Ingress (OpenResty) ]
       │ HTTP /track
       ▼
[ LEVEL 2: Ingestion (Tracker - gnet) ]
       │ EVALSHA / Stream XADD
       ▼
[ LEVEL 3: Edge State (Redis Shards) ] ◄── Sentinel
       │ Async stream reads
       ▼
[ LEVEL 4: Core Logic (Processor, Management) ]
       │ Batch inserts / ledger / outbox
       ▼
[ LEVEL 5: Persistence (Postgres, ClickHouse) ]
```

1. **Level 1 — Ingress (:8180)** — IP blacklists, edge rate limiting, and DFA body scans. Outages: Stale blacklist cache (~5 s TTL); Sentinel disconnect. Mitigations: Two-phase validation; local dictionary TTL fallback to `REDIS_ADDRS`.
2. **Level 2 — Ingestion (:8181–8184)** — gnet loops, Go filters, and Lua triggers. Outages: MPSC ring overflow; slow GeoIP. Mitigations: Bounded MPSC rings with fast drop; GeoIP fail-open with bypass metrics.
3. **Level 3 — Redis State (:6479–6482)** — Budgets, fcap, dedup, and streams. Outages: Master container termination; NOSCRIPT errors. Mitigations: Sentinel promotion under 15 s; client circuit breakers; automatic EVAL fallback + `ad_redis_lua_noscript_total` alerts.
4. **Level 4 — Core Logic (:8186 / :8188)** — Stream parsing, PG/CH writes, and outbox workers. Outages: Processor lag; Postgres transaction failure. Mitigations: `XAutoClaim` for pending rows; DLQ routing after `MAX_RETRIES`; and `sync_idempotency` checks.
5. **Level 5 — Persistence (PG/CH)** — Balance ledger and telemetry tables. Outages: Row lock conflicts; ClickHouse write amplification. Mitigations: `SELECT FOR UPDATE NOWAIT` / advisory locks; batch writes of ≥ 1000 rows or 1 s flush windows.

### Failure Classification
Experiments are prioritized by **likelihood × severity** (Principles of Chaos):
1. **Crash-Stop** — SIGKILL on tracker, processor, or Redis master. Invariants: Breaker opens; no deadlocks; automatic recovery.
2. **Network Faults** — Partitions, packet loss, and latency injection. Invariants: Idempotency holds; no split-brain; stream queues remain bounded.
3. **Resource Exhaustion** — CPU throttling and I/O bottlenecks. Invariants: Queue backlogs are processed successfully once limits are lifted; no data loss.
4. **Clock Faults** — NTP time jumps and leap second simulations. Invariants: Monotonic deadlines and TTC checks remain correct.
5. **Slow Dependencies** — Redis slow logs and Postgres lock waits. Invariants: Timeout triggers before memory overflow; fail-open defaults operate.
6. **Dependency Outage** — PG/CH/Stripe unavailable. Invariants: Outbox remains `PENDING`; auth fails closed; no duplicate balance mutations.
7. **Corrupted Inputs** — Bad request bodies, stale webhooks, and corrupted snapshots. Invariants: HTTP 400 or filter reject; no ledger commits; catalog remains clean.
8. **Concurrency** — Concurrent outbox workers, webhook replays, and budget spend racing. Invariants: Exactly-once delivery; lease locks respected; non-negative budget balances.
9. **Configuration Drift** — Wrong shard count, missing Sentinel nodes. Invariants: Process fails to start or registers error metrics.
10. **Cascading Failures** — Retry storms, DLQ overflow. Invariants: Steady state recovers after the primary fault is cleared.

### Testing Matrix
1. **Unit** — Data structure checks, concurrent operations without network. Environment: In-process. Proofs: None; hot-path `-benchmem` verified.
2. **Integration** — Dependency outage, slow network connections. Environment: testcontainers Redis + Postgres. Proofs: Required for payment/ingest paths.
3. **CI Chaos (`TestChaos_*`)** — Crash-stop, partitions, resource limits, clock drift. Environment: Docker + `BROKER_CHAOS_LAB=1`. Proofs: Required; counted towards `MIN_PROOFS`.
4. **RTB Matrix** — OpenRTB schemas, edge budget limits. Environment: In-memory. Proofs: Required; table-driven.
5. **Compose Scenarios** — Cascading failures, multi-shard outages, Sentinel. Environment: `docker compose` stack. Proofs: Manual verification in stdout.

Testing Rules:
- **Bottom-Heavy Priority:** Prove invariants at unit and integration levels first; container chaos is reserved for process-crossing boundaries (R10).
- **Single Fault per Run:** Multi-dependency outages (E, G) are manual; CI tests check one variable at a time.
- **Control Cohorts:** Compare latency and spend across unaffected shards during the outage (ChAP approach).
- **Automated Aborts:** Use timeouts and `t.Fatal` inside tests — do not rely on manual pipeline cancellation.

### Chaos Test Catalog (Summary)

**Ingestion / Ads**
1. `redis_container_terminate` — `fault_injection_test.go` — Fast tracker recovery.
2. `postgres_container_terminate` — `fault_injection_test.go` — Processor breaker opens; stream remains intact.
3. Stream backpressure on PG outage — `fault_injection_test.go` — Backpressure limits memory growth.
4. Recovery after Redis restart — `fault_injection_test.go` — Breaker closes post-recovery.
5. Concurrent tracking on Redis outage — `fault_injection_test.go` — Goroutine counts remain bounded.
6. `clock_drift` / monotonic TTC — `clock_drift_chaos_test.go` — TTC remains correct under clock shifts.
7. Poison pill to DLQ — `breaker_fault_test.go` — DLQ transition after retry limits.
8. Shard 0 outage — `tests/chaos/shard0_outage_chaos_test.go` — Shards 1-3 remain unaffected.

**Management / Outbox**
1. Redis down → Outbox PENDING — `fault_container_test.go` — No duplicate replication.
2. PG down → Lockout — `fault_container_test.go` — `SKIP LOCKED` query safety.
3. Concurrent outbox workers — `fault_injection_test.go` — Exactly-once delivery.
4. Recovery post PG deadlock — `fault_injection_test.go` — Transaction retry succeeds.
5. Concurrent balance exhaustion — `fault_injection_test.go` — Balance remains non-negative.
6. Network partition during slot migration — `slot_migration_chaos_test.go` — Idempotent copy and rollback.
7. Load spike / deadlock during autoscaling — `shard_autoscaling_chaos_test.go` — Zero-hang rebalancing.

**Auth / Payment / Billing**
1. Auth dependency outage — `auth/fault_injection_test.go` — Fails closed under DB failure.
2. Stripe signature expired — `webhook_chaos_test.go` — Replay webhook rejected.
3. Webhook / payment outbox race — `payment/fault_injection_test.go` — Idempotency holds.
4. Settlement server down → outbox block — `fault_container_test.go` — Prevents orphan credits.
5. Ledger drift check — `ledger_drift_chaos_test.go` — Compares PG ledger vs Redis state.

**Edge / RTB / Broker**
1. Edge blacklist / fraud filter — `edge/perimeter/edge_chaos_test.go` — Early-stage blocking.
2. RTB Matrix A1-H2, G1-G5 — `rtb/chaos_test.go`, `chaos_persistence_test.go` — Auction invariants.
3. Fencing modes (`split_brain`, `stale_leader`) — `pkg/broker/server/*` — Epoch filtering.
4. `redis_sentinel_failover` packet loss — `chaos_ha_network_test.go` — High-availability recovery.
5. `cpu_throttle_replication` load — `chaos_durability_lab_test.go` — Replication catch-up after throttle.

---

## Chaos Scenarios

### Scenario A: Shard 0 Outage (Pub/Sub, Auth Lockout)
**Hypothesis:** Shards 1-3 track normally; campaigns on shard 0 fail closed or return 503; management outbox halts updates; tracker does not crash.  
**CI Test:** `tests/chaos/shard0_outage_chaos_test.go`
1. Send traffic to all shards; record baseline RPS and latency.
2. Stop the `redis-0` master container.
3. Verify: Shard 0 circuit breaker opens; shards 1-3 p99 latency remains < 80 ms; management outbox updates pause.
4. Restart `redis-0`; Sentinel restores master status.
5. Verify: Breaker closes; outbox flushes pending items; shard 0 tracking recovers.
6. Proof: `chaos_proof fault=shard_0_outage status=recovered`.

### Scenario B: Sentinel Failover Under Load
**Hypothesis:** Shard recovery completes within 15 s; zero panics or memory leaks; budget spends remain consistent.
1. Target high traffic RPS to campaigns on shard 2.
2. Stop the `redis-2` master container.
3. Verify: Requests timeout for ~5 s; breaker opens; Sentinel promotes new master in 10-12 s; breaker transitions to Closed.
4. Verify budget limits on Postgres match Redis state (excluding active SyncWorker deltas).
5. Proof: `chaos_proof fault=sentinel_active_failover duration_ms=<n> budget_consistent=true`.

### Scenario C: Network Partition Between Processor and Postgres
**Hypothesis:** Events are not lost; stream length grows; processor memory usage remains stable; exactly-once PG entries post-recovery.  
**CI Test:** `fault_injection_test.go` (stream accumulation case)
1. Send continuous traffic to `/track`; verify events enqueue in Redis stream.
2. Block port `5432` in the processor container.
3. Verify: PG timeouts; processor breaker opens; `XREADGROUP` pauses; stream length grows linearly.
4. Remove the port block.
5. Verify: Breaker closes; queue is drained; no duplicate entries in ledger (`sync_idempotency`).
6. Proof: `chaos_proof fault=processor_pg_partition backpressure_active=true idempotency_verified=true`.

### Scenario D: Time Shift on Tracker
**Hypothesis:** Monotonic time ensures correct filter timeouts and TTC validation even if system wall clock shifts.  
**CI Test:** `clock_drift_chaos_test.go`
1. Shift clock by +3600 s on a tracker node container.
2. Trigger impression, then click after 5 s.
3. Verify: TTC checks pass (5 s elapsed monotonically); `FILTER_TIMEOUT_MS` enforces deadlines normally.
4. Proof: `chaos_proof fault=clock_drift_monotonic_safety drift_seconds=3600 ttc_passed=true`.

### Scenario E: Staggered Dependency Outage (Redis + PG)
**Hypothesis:** Sequential dependency failures do not cause overspend; outbox and streams recover state in order when databases recover.
1. Disable Redis shard for active campaign; `/track` requests reject immediately (breaker).
2. Stop Postgres while Redis is down.
3. Restore Redis; verify: stream write resumes, processor remains blocked waiting for PG.
4. Restore Postgres; verify: queue drains sequentially, deduped via `sync_idempotency`.
5. Proof: `chaos_proof fault=staggered_redis_pg_outage budget_consistent=true order=redis_then_pg`.

### Scenario F: ClickHouse Slowdown / Outage
**Hypothesis:** Ingestion and PG ledger are unaffected; CH lag remains bounded; tracker does not block on analytics failures.
1. Restrict ClickHouse write bandwidth or stop it on the processor node.
2. Send steady traffic to `/track`.
3. Verify: Processor registers CH write errors; PG events write normally; tracker p99 latency remains stable.
4. Restore ClickHouse; verify stream catch-up without duplicates (`insert_deduplicate`).
5. Proof: `chaos_proof fault=clickhouse_outage pg_ledger_ok=true ch_catchup=true`.

### Scenario G: Management Outbox Storm Under Global Replication
**Hypothesis:** Global replication updates (HSET to all shards) do not starve campaign pacing updates; maximum wait time returns to < 30 s after the storm clears.
1. Update 100+ campaigns concurrently via the management API.
2. Inject a 200 ms latency delay on `PUBLISH` commands on shard 0.
3. Verify: `ad_management_outbox_oldest_pending_seconds` spikes then returns to normal; no duplicate key sets.
4. Proof: `chaos_proof fault=outbox_storm_under_slow_pubsub max_pending_ms=<n> duplicates=0`.

### Scenario H: Nginx Blacklist Cache Staleness
**Hypothesis:** Max delay for a blacklist update is ~5 s (cache TTL); Nginx blocks the IP without restarting.
1. Allow client IP; verify HTTP 200 on `/track`.
2. Blacklist the IP via the management API.
3. Verify: Ingress blocks requests from the IP after cache TTL expires.
4. Proof: `chaos_proof fault=edge_blacklist_staleness ttl_respected=true`.

### Scenario I (Backlog): `SCRIPT FLUSH` Under Traffic
**Hypothesis:** Brief `NOSCRIPT` error spike; EVAL fallback operates; budget limits are still enforced; `ad_redis_lua_noscript_total` returns to baseline.
1. Execute `SCRIPT FLUSH` on a shard master under load.
2. Verify: Latency spikes briefly; campaign spend remains correct.
3. Proof: `chaos_proof fault=lua_script_flush noscript_spike=true budget_ok=true`.

### Scenario J (Backlog): Leap Second / Clock Step Backward
**Hypothesis:** Daypart schedule filters do not miss active windows or double-trigger when system clock steps back.
1. Step host clock back by 1 s during daypart window boundary.
2. Verify: Ingestion path remains stable; schedule logic handles time step.
3. Proof: `chaos_proof fault=clock_step_backward schedule_stable=true`.

### M2 Extended Matrix (UDP, Redis Engine, Lua, FD, CPU)

**Full catalog:** [GAPS.md](docs/GAPS.md) §9 — chaos backlog including elastic shard orchestrator scenarios:

| Domain | IDs | Focus |
| :--- | :--- | :--- |
| UDP control plane | UDP-01…26 | Loss, reorder, epoch gap, stale/canary, TCP snapshot |
| Redis single-thread | REDIS-01…15 | Blocking commands, migrate COPY, triplet A/B/R |
| Lua (Redis 5.1, no JIT) | LUA-01…11 | SCRIPT FLUSH, tier degrade, routing_epoch fence |
| Edge LuaJIT | EDGE-LUA-01…02 | OpenResty only (not Redis) |
| Network | NET-01…10 | RTT, partition, churn, cross-AZ SLA |
| FD exhaustion | FD-01…06 | ulimit, lazy dial, EMFILE on migrate |
| CPU / host | CPU-01…08 | cgroup throttle, false sharing, scrape overhead |
| Shard Orchestrator | SO-01…08 | False migrate, scale-up, quorum gate |
| Cascading | CAS-01…05 | Multi-fault game days |

Implement per [R10 #11, #13](GUIDE_CHAOS_RELIABILITY.md); update `CHAOS_MIN_PROOFS` when new CI proofs land.

---

### Performance Benchmarking Guidelines

Hot-path benchmarks in `internal/ingestion` MUST enforce zero heap allocations. Developers execute benchmarks using `go test -bench=. -benchmem` and verify that `allocs/op` equals zero. Any PR introducing non-zero allocations on `/track` ingestion is blocked by performance gate automation.

### Alerting & Circuit Breaker Thresholds

Chaos automation monitors core observability rules configured in Alertmanager:
- **p99 Latency SLA Breach**: Alert triggers when `ad_http_request_duration_seconds` p99 latency exceeds 80 ms for over 30 seconds.
- **Persistent Circuit Breaker**: Alert triggers when `ad_redis_breaker_state == 2` (Open) for over 5 minutes.
- **Dead-Letter Queue Backlog**: Alert triggers when `ad_processor_dlq_length` exceeds 100 pending messages for over 1 minute.

---

## Checklist

### Hot-Path or Financial Feature Changes
- [ ] Steady-state metrics defined (R1)
- [ ] Confirm chaos tests are not redundant or write a new experiment (R10)
- [ ] Document hypothesis and abort criteria (R9)
- [ ] Integration tests run against real Postgres/Redis (R5)
- [ ] Concurrency testing uses at least 20 goroutines (R5)
- [ ] Affected path benchmarks maintain 0 allocs/op (R5)
- [ ] Validate budget and click idempotency invariants (R5)
- [ ] Log a new `chaos_proof` metric or confirm existing suites pass (R7)
- [ ] Update manual compose runbooks if failure behavior changed

### Redundant Changes (R10)
- [ ] Confirm no new write paths, lock leases, or outbox triggers are added
- [ ] `go test ./... -short` passes
- [ ] Existing hot-path benchmarks are unchanged
- [ ] No new `chaos_proof` lines are required

### CI Merge Gates
- [ ] `./scripts/chaos-drills/test_chaos.sh` passes (`MIN_PROOFS` met)
- [ ] `go test ./... -short` passes
- [ ] Hot-path benchmark metrics are stable
