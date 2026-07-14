# MILESTONE: ML Async Analytics (Cold-Path Anti-Fraud)

**Status:** Planned  
**Parent spec:** [ML_ANALYTICS.md](ML_ANALYTICS.md)  
**Testing:** [GUIDE_CHAOS_RELIABILITY_RU.md](GUIDE_CHAOS_RELIABILITY_RU.md) (R1–R10)  
**Service boundaries:** [docs/MICROSERVICES.md](docs/MICROSERVICES.md) §3  
**Hot-path runtime:** [docs/RUNTIME.md](docs/RUNTIME.md), `.cursorrules`

## Scope

Cold-path ML scoring and async enforcement for IVT/SIVT. Hot path (`internal/ads`, gnet, Lua) receives **only** precomputed Redis keys (`ml:score:boost`, `ml:threat:*`, `blacklist:fraud`). No synchronous ML inference on `/track`.

**In scope:** `internal/mlanalytics/`, `cmd/ml-analytics` (later), ClickHouse MVs, management outbox handlers, minimal tracker config snapshot reads.  
**Out of scope:** Real-time gRPC fraud server, transformer models on tracker, OpenRTB 3.0 signing, replacing `ivt-detector` SQL rules (ML complements them).

## Current baseline

| Component | Role today |
| :--- | :--- |
| `cmd/ivt-detector` | ClickHouse SQL rules → gRPC `BlockIP` → outbox `UPDATE_BLACKLIST` |
| `internal/ivtdetector/` | `SuspiciousFinder`, idempotency, outbox backpressure |
| `FraudStreamWriter` | Lossy MPSC fraud telemetry (shard 0 stream) |
| `filter_layer` / `DeviceFilter` | Rule-based `fraud_score` → `FraudRLTier` |
| Management outbox | Global blacklist replication per shard |

Propagation contract (unchanged): **PG TX → `outbox_events` → Redis all shards → tracker/edge reload**.

---

## Code style standards

Two execution contexts apply. **Never mix cold-path idioms into hot-path packages.**

### Currency micro-units (required)

All money in eSPX is **`int64` micro-units**: **1 major currency unit = 1_000_000 micro** (6 decimals). Canonical reference: [ML_ANALYTICS.md](ML_ANALYTICS.md) §3.4, [docs/architecture.md](docs/architecture.md).

| Rule | Requirement |
| :--- | :--- |
| ML features | Read `*_micro` / `budget_limit` / `current_spend` as `int64`; ratios OK; no `float64` dollars in CH/PG feature SQL |
| Training export | Parquet columns `spend_micro`, `budget_limit_micro` as integer; document scale in `metadata.json` |
| Scorer types | Feature vector slots for money: `float32` only after explicit normalize (`micro / 1e6` or ratio); prefer integer ratio features |
| Enforcement | ML worker **must not** mutate `budget:*` Redis keys; ghost IVT is non-billable, not a micro-unit adjustment |
| Chaos / DoD | `AssertBudgetInvariant` tolerance **±1 micro-unit**; abort if `current_spend > budget_limit` in Postgres |
| Naming | Distinguish **currency micro-unit** from **processor micro-batch** (M-ML5, 100 ms windows) in code comments and metrics |

```go
// Cold path — prefer int64 micro-units until model boundary
type CampaignSpendFeatures struct {
    SpendMicro1h   int64   // from CH, never float dollars
    BudgetLimitMicro int64
    SpendRatio     float64 // derived: spend / budget, dimensionless
}
```

### Cold path — idiomatic Go (`internal/mlanalytics`, `cmd/ml-analytics`, management handlers)

| Rule | Requirement |
| :--- | :--- |
| Errors | Wrap with `%w`; return early; no panic on I/O |
| Context | `context.Context` on all CH/PG/gRPC calls; deadlines per scan cycle |
| Concurrency | `errgroup` or worker pool with bounded parallelism; no unbounded goroutine per row |
| Data access | `pgx.Batch` for bulk inserts; parameterized CH queries; sqlc for PG |
| Config | `internal/config` env structs; `ML_ANALYTICS_ENABLED` default `false` |
| Logging | `log/slog` structured; no hot-loop debug in scorer |
| Interfaces | Small `Scorer`, `FeatureStore`, `ThreatEnqueuer` — test with stubs |
| ML runtime | `go-lgbm` / ONNX **only** in `internal/mlanalytics`; never imported by `cmd/tracker` |
| Memory | Pre-cap slices: `make([]float32, 0, batchSize)`; reuse `[][]float32` batch buffers across cycles |
| Tests | Table-driven unit tests; testcontainers for CH/PG; `-race` on idempotency paths |

### Hot path — data-oriented design (`internal/ads` ML touchpoints only)

Applies to `filter_layer` boost lookup, config snapshot reload, and any new fraud fields in `fraudStreamSlot`. Same bar as [docs/RUNTIME.md](docs/RUNTIME.md) and `.cursorrules`.

| Rule | Requirement |
| :--- | :--- |
| Zero heap alloc | `0 allocs/op` on `/track` filter path touched by ML; verify `go test -benchmem` |
| No reflection / boxing | No `interface{}` on request path; no `fmt.Sprintf` in filter hot loop |
| Forbidden in hot loops | `defer`, channels, closures, `time.Now()` (use coarse/monotonic atomics) |
| Struct layout | Fields ordered descending size; explicit `_ [N]byte` padding to 64-byte lines |
| False sharing | Independent counters per worker: pad each `atomic.Uint64` / `int64` to cache line (see `FraudStreamWriter`, `worker_pool.go` MPSC indices) |
| Atomic increments | Per-gnet-worker cells for boost-application counters; never increment a shared global from all workers on one cache line |
| Pre-allocation | Fixed arrays in slot structs (`[fraudSlotIPMax]byte` pattern); stack buffers for boost lookup keys |
| Arena-like pools | Reuse conn-local scratch via pinned worker context — **not** `sync.Pool` on global request path |
| BCE | Length guard at loop entry: `_ = table[n-1]` before hot index loop; verify `-d=ssa/prove/debug=1` |
| Config reads | `atomic.Value` or atomic pointer swap for immutable `MLBoostSnapshot`; readers never lock |
| Unsafe | `unsafe.String` only with `runtime.KeepAlive` at boundary; document lifetime |
| Inlining | Keep boost helpers low cyclomatic complexity; check `-gcflags="-m"` for `can inline` |
| Import fence | `go list -deps ./cmd/tracker` must not reach `github.com/zhongdai/go-lgbm` or `onnxruntime_go` |

### Warm path — processor (`cmd/processor`, Phase M-ML5 only)

| Rule | Requirement |
| :--- | :--- |
| Latency budget | Micro-batch adds ≤10 ms p99 vs baseline |
| Pooling | Bounded channel + fixed worker count; backpressure when stream lag > ceiling |
| Style | Idiomatic Go (same as cold path); **not** zero-alloc unless proven on profiler |

### CI gates (all milestones touching hot path)

```bash
go test -benchmem -bench=BenchmarkFilter ./internal/ads/...
go build -gcflags="-m -m" ./internal/ads/... 2>&1 | grep -E 'escapes to heap|ml'
go list -deps ./cmd/tracker | grep -E 'mlanalytics|go-lgbm|onnxruntime' && exit 1 || true
```

Perf-gate workflow (`perf-gate.yml`) blocks merge on hot-path regression >5% or any new alloc on touched benchmarks.

---

## SLA requirements

### Global (all milestones)

| Domain | Metric | Target | Hard ceiling | Measurement |
| :--- | :--- | :--- | :--- | :--- |
| **Hot path latency** | Tracker p95 | < 50 ms | — | `ad_http_request_duration_seconds` |
| | Tracker p99 | < 80 ms | 100 ms wall | same |
| | ML-added latency | **0 ms** | — | A/B bench on control cohort |
| **Hot path throughput** | `/track` RPS | No regression vs baseline | — | load-test snapshot |
| **Hot path errors** | Failed responses | < 0.1% | — | excluding valid filter rejects |
| **Financial** | Budget R5 | PG authority ±**1 currency micro-unit** (§3.4) | abort test | `AssertBudgetInvariant` |
| | `current_spend` | ≤ `budget_limit` always | `t.Fatal` | Postgres after chaos |
| **ML scoring** | Batch 10k rows | < 2 s | 5 s | `ml_scoring_duration_seconds` |
| **ML freshness** | Threat intel lag | < 2× scan interval | 15 min default | time(enforce) − time(event) |
| **Control plane** | Outbox oldest pending | < 30 s steady | 120 s alert | `ad_management_outbox_oldest_pending_seconds` |
| **Enforcement** | FP override latency | < 60 s | — | operator remove → Redis clear |
| **Availability** | ML worker down | `/track` unaffected | — | chaos `ml_worker_down` |
| **Model deploy** | Per-shard cutover | ≤ 120 s | rollback at 180 s | `ml_model_sync_*` metrics |

### CAP / degradation SLAs

| Condition | Required behaviour |
| :--- | :--- |
| ML worker crash | Trackers serve last-known Redis keys; no 5xx spike |
| CH unavailable | Skip scoring cycle; no outbox storm; metric `ml_scoring_errors_total` |
| Outbox backlog ≥ limit | Pause ML enforcement (`ErrOutboxBackpressure`) |
| `ml:model:version` stale > 2× sync | **Tighten** suspect-tier RL only; never loosen block without signed snapshot |
| Single shard in `ML_SYNC` | Other shards filter on `V-1`; control p99 unchanged |

### Constrained load testing (reduced environment)

Observe **CPU starvation**, **heap pressure**, **network stack exhaustion**, and **ML cold-path backpressure** on a laptop or small CI runner — without provisioning production-sized hosts. This is a **regression / saturation discovery** profile, not a production SLA sign-off.

**Existing repo tooling:** `docker-compose.load-test.yml`, `scripts/load/run_dirty_load.sh`, `scripts/load/run_spike_load.sh`, `scripts/load/host_tune.sh`, `scripts/load/snapshot_runtime.sh`, `scripts/load/analyze_bottlenecks.sh`.

#### Design goals

| Goal | How constrained profile helps |
| :--- | :--- |
| Find hot-path regressions early | 2 trackers × 1 CPU × 350MiB `GOMEMLIMIT` amplifies alloc/GC mistakes |
| Observe outbox / ML backpressure | Dirty load fills `outbox_events`; ML worker hits `ML_OUTBOX_PENDING_LIMIT` |
| Reproduce connection churn | k6 + reduced `ip_local_port_range` tuning exposes TIME_WAIT / FD limits |
| Cheap game-day rehearsal | Same abort criteria as production (R1, R5) at lower RPS |

#### Environment profiles

| Profile | Host RAM | Compose | Trackers | Default RPS | Duration | Script |
| :--- | :---: | :--- | :---: | :---: | :--- | :--- |
| **smoke** | ≥ 8 GiB | `compose + load-test.yml` | 2 | 500 | 2 min | `run_dirty_load.sh smoke` |
| **constrained** (default) | ≥ 8 GiB | `compose + load-test.yml` | 2 | 2000 | 5 min | `run_dirty_load.sh full` |
| **full** | ≥ 16 GiB | `compose.yml` only | 4 | 5000 | 5 min | `CONSTRAINED=0 run_dirty_load.sh full` |
| **spike** | ≥ 8 GiB | `compose + load-test.yml` | 2 | 1×→10× ramp | 30 s hold | `run_spike_load.sh` |
| **ml-saturation** | ≥ 8 GiB | constrained + ML enabled | 2 | 2000 + ML worker capped | 10 min | manual (see below) |

```bash
# Standard constrained dirty load
bash scripts/load/host_tune.sh verify          # or: sudo bash scripts/load/host_tune.sh apply
bash scripts/load/run_dirty_load.sh smoke

# Full constrained soak + artifacts
bash scripts/load/run_dirty_load.sh full
# → var/load-test/<ts>/k6.log, bottleneck-report.md, runtime-pre/post/
```

#### Go runtime limits (align with cgroup CPUs)

Rule: **`GOMAXPROCS` ≤ container CPU limit**. Mismatch (high `GOMAXPROCS`, low cgroup `cpus`) causes scheduler thrash and misleading p99.

| Service | `GOMAXPROCS` | `GOMEMLIMIT` | `GOGC` | cgroup `cpus` | cgroup `memory` | Source |
| :--- | :---: | :--- | :---: | :---: | :---: | :--- |
| `tracker-0/1` | 2 | 350MiB | 50 | 1.0 | 448M | `docker-compose.load-test.yml` |
| `tracker-2/3` (if up) | 1 | 300MiB | default | 0.5 | 448M | same |
| `processor` | 2 | 512MiB | 100 | 1.5 | 640M | same |
| `management` | 1 | 300MiB | 100 | 0.5 | 384M | add in ml-saturation overlay |
| `ml-analytics` / `ivt-detector` | **1** | **128MiB** | 100 | **0.5** | **192M** | **M-ML-L1** deploy overlay |
| k6 (host) | 4 | — | — | host | — | `run_dirty_load.sh` `GOMAXPROCS` |

```yaml
# deploy/k8s/overlays/load-test/ml-analytics-limits.yaml (M-ML-L4 sketch)
env:
  - name: GOMAXPROCS
    value: "1"
  - name: GOMEMLIMIT
    value: "120MiB"
  - name: GOGC
    value: "100"
  - name: ML_OUTBOX_PENDING_LIMIT
    value: "100"   # lower threshold to trigger backpressure earlier under load
resources:
  limits:
    cpu: "500m"
    memory: 192Mi
```

**Interpretation:**

- `GOMEMLIMIT` — soft heap cap; runtime raises GC frequency before OOM. Watch `go_gc_duration_seconds`, `go_memstats_heap_inuse_bytes`.
- `GOGC=50` on trackers — trades CPU for lower steady-state heap (matches production compose intent).
- Perf-gate CI uses `GOMAXPROCS=1` (`.github/workflows/perf-gate.yml`) — orthogonal to load-test; do not conflate bench with soak.

#### Container and datastore caps

From `docker-compose.load-test.yml` — totals must fit **~8 GiB** host budget with k6 + Docker overhead:

| Service | CPU limit | Memory limit | Notes |
| :--- | :---: | :---: | :--- |
| Redis ×6 | 0.25 each | 256M each | Lua single-threaded; CPU cap simulates slow shard |
| ClickHouse | 1.0 | 1G | `config.load-test.yml` lowers CH memory |
| Postgres | 1.0 | 768M | `max_connections=200`, `shared_buffers=256MB` |
| nginx | 0.5 | 256M | 2-tracker upstream only in dirty load |
| prometheus | 0.25 | 256M | `prometheus.load-test.yml` shorter retention |

#### Network stack and host tuning

Client-side exhaustion (k6) often fails before server CPU. Apply **`scripts/load/host_tune.sh`**:

```bash
sudo bash scripts/load/host_tune.sh apply   # installs sysctl.d snippets
bash scripts/load/host_tune.sh verify       # exit 1 if drift
bash scripts/load/host_tune.sh report       # human-readable diff
```

Key sysctl (`deploy/edge/99-espx-loadtest.conf`):

| Parameter | Value | Symptom if wrong |
| :--- | :--- | :--- |
| `net.ipv4.ip_local_port_range` | 1024–65535 | k6 `connect: cannot assign requested address` |
| `net.ipv4.tcp_max_tw_buckets` | 262144 | TIME_WAIT overflow under short-lived clients |
| `net.ipv4.tcp_fin_timeout` | 15 | Slow port recycle (lab only) |
| `net.core.somaxconn` | 16384 | `listen` queue drops under burst |
| `fs.file-max` | 2097152 | global FD exhaustion |
| `ulimit -n` | ≥ 100000 | per-shell FD cap — raise in `limits.d` |

**Optional impairment** (observe ML / outbox under partition without cloud cost):

```bash
# ClickHouse slow — ML skip cycles, scoring errors rise, hot path unaffected
sudo tc qdisc add dev lo root netem delay 50ms 10ms loss 2%

# Management partition — ML idempotency release + retry
sudo iptables -A OUTPUT -p tcp --dport 51053 -j DROP   # settlement gRPC; lab only

# UDP control (if enabled) — see SHARDING_STRATEGY §7.3 profiles
# tc netem on tracker UDP :8191 — loss 5%, delay 2ms

# Teardown
sudo tc qdisc del dev lo root 2>/dev/null || true
```

#### ML-specific saturation scenarios

Run under **constrained compose** with `ML_ANALYTICS_ENABLED=true` after M-ML1.

| ID | Setup | Hypothesis | Pass / abort |
| :--- | :--- | :--- | :--- |
| **ML-L1** | Dirty 2000 RPS + ML worker `GOMAXPROCS=1` | `/track` p99 < 80 ms; ML lag acceptable | Abort if control p99 > 80 ms 30 s |
| **ML-L2** | `ML_OUTBOX_PENDING_LIMIT=50` + dirty load | `ml_enforcement_paused_total` rises; no OOM; auto-resume when lag drops | No tracker 5xx spike |
| **ML-L3** | `tc netem` on CH port 9000 | `ml_scoring_errors_total` ↑; no outbox storm; skip cycles | Hot path unchanged |
| **ML-L4** | Spike 10× 30 s + ML enabled | Outbox priority 0 (`UPDATE_BLACKLIST`) drains before boosts | Blacklist latency < 2× scan interval |
| **ML-L5** | `GOMEMLIMIT=64MiB` on ml-analytics | Worker OOM-restart loop; trackers stable on last Redis keys | `ml_worker_down` recovery path |

**A/B control:** run same profile with `ML_ANALYTICS_ENABLED=false` — delta isolates ML control-plane load from ingest.

#### Standard run procedure

```
1. host_tune.sh verify
2. docker compose -f docker-compose.yml -f docker-compose.load-test.yml up -d
3. snapshot_runtime.sh var/load-test/<ts>/runtime-pre
4. run_dirty_load.sh | run_spike_load.sh
5. snapshot_runtime.sh var/load-test/<ts>/runtime-post
6. analyze_bottlenecks.sh var/load-test/<ts>/
7. Assert abort criteria; archive var/load-test/<ts>/
```

#### Abort criteria (constrained profile)

Same as [GUIDE_CHAOS_RELIABILITY_RU.md](GUIDE_CHAOS_RELIABILITY_RU.md) R1 + ML extensions:

| # | Criterion | Threshold |
| :---: | :--- | :--- |
| 1 | Control-cohort p99 | < 80 ms for 30 s sustained |
| 2 | Error rate | < 0.1% (excl. valid filter rejects) |
| 3 | Budget R5 | `AssertBudgetInvariant` ±1 currency micro-unit |
| 4 | OOM kills | Zero tracker/processor OOM during soak |
| 5 | ML backpressure | If triggered, must auto-resume within 5 min after load stops |
| 6 | FD / TIME_WAIT | Document in bottleneck-report; fail if k6 cannot sustain RPS due to client ports |

**Not a failure:** ML enforcement lagging 2–3 scan intervals under peak load if hot path and R5 hold.

#### Artifacts and metrics to capture

| Artifact | Path / query |
| :--- | :--- |
| k6 summary | `var/load-test/<ts>/k6.log` |
| Bottleneck report | `var/load-test/<ts>/bottleneck-report.md` |
| FD / strace | `var/load-test/<ts>/runtime-{pre,post}/` |
| Tracker p99 | `histogram_quantile(0.99, ad_http_request_duration_seconds)` |
| Redis Lua p99 | `ad_redis_lua_duration_seconds` |
| Outbox lag | `ad_management_outbox_oldest_pending_seconds` |
| ML scoring | `ml_scoring_duration_seconds`, `ml_enforcement_paused_total` |
| GC pressure | `go_gc_duration_seconds`, `go_memstats_heap_inuse_bytes` |

#### Implementation tasks (load test)

| Task | Milestone | Deliverable |
| :--- | :---: | :--- |
| **M-ML-L1** | M-ML1 | `docker-compose.load-test.yml` service stub for `ml-analytics` with Go limits |
| **M-ML-L2** | M-ML2 | `scripts/load/run_ml_saturation.sh` — wraps dirty load + ML-L1..L5 matrix |
| **M-ML-L3** | M-ML3 | Spike + model sync under constrained CPUs |
| **M-ML-L4** | M-ML4 | Document profile in `docs/development.md`; CI optional weekly workflow |
| **M-ML-L5** | M-ML5 | Processor micro-batch lag scenario under `CH_MAX_WORKERS=1` |

#### What constrained testing is not

- **Not** production capacity planning — absolute RPS numbers are valid only relative to the same profile.
- **Not** a substitute for perf-gate `0 allocs/op` — use `make test-alloc-gate` / `perf-gate.yml` for alloc regression.
- **Not** requiring bare-metal — purpose is **cheap starvation signals**, not peak RPS records.

---

Full design: topology, state machines, outbox priority, idempotency, runbooks RB-1–RB-5, catch-up policy.

| Class | Mechanisms |
| :--- | :--- |
| **Automatic** | Worker `RunLoop` continue-on-backpressure; CH retry; `sync_idempotency` claim/release; outbox `reclaimStaleProcessing`; priority-0 drain; model sync rollback |
| **Manual** | RB-1 kill-switch; RB-2 FP override; RB-3 model rollback; RB-4 stuck idempotency; RB-5 outbox storm |
| **Verify** | 9-step post-incident checklist (§3.5) |

**Implementation tasks (recovery-specific):**

| Task | Milestone | Package |
| :--- | :---: | :--- |
| **M-ML-R1** `MLIdempotencyStore` — prefix `ml:enforce:{ip}:{ver}:{reason}` on `sync_idempotency` | M-ML1 | `internal/mlanalytics/` |
| **M-ML-R2** Extend `GetPendingOutboxEventsForUpdate` — `ML_MODEL_VERSION` priority 0, `ML_SCORE_BOOST` priority 1 | M-ML1 | `management.sql`, `outbox_handlers.go` |
| **M-ML-R3** `MLEnforcementPaused` metric + hook `ErrOutboxBackpressure` in scorer loop | M-ML1 | `internal/mlanalytics/`, `ivtdetector` |
| **M-ML-R4** `POST /admin/ml/overrides` + audit log (RB-2) | M-ML2 | `internal/management/` |
| **M-ML-R5** `ml_shard_sync_state` migration + `MLModelSyncOrchestrator` rollback | M-ML3 | `internal/management/` |
| **M-ML-R6** Stale epoch handler — `UPDATE_SETTINGS` tighten + `ML_MODEL_VERSION` burst | M-ML3 | `internal/management/` |
| **M-ML-R7** `OpsAlerter.AlertMLSyncStuck`, `AlertMLScoringFailures` | M-ML3 | `ops_alerter.go` |
| **M-ML-R8** Recovery runbooks RB-1–RB-5 in `docs/development.md` | M-ML4 | docs |
| **M-ML-R9** Processor micro-batch pause on `ad_processor_stream_lag_seconds` | M-ML5 | `cmd/processor` |

**Milestone hooks:**

- **M-ML1:** `ErrOutboxBackpressure`, management retry, idempotency release (`ml_management_retry`); M-ML-R1–R3
- **M-ML2:** operator override API (`ml_false_positive_override`); M-ML-R4
- **M-ML3:** `MLModelSyncOrchestrator` rollback, stale epoch handler; M-ML-R5–R7
- **M-ML4:** runbook RB-1–RB-5 in `development.md`; alert on `ml_model_version_active` stale > 24h; M-ML-R8
- **M-ML5:** M-ML-R9

**DoD — recovery (additive to phase DoD):**

- [x] Each new fault class has row in §3.5 automatic matrix + `chaos_proof`
- [x] Idempotency: claim → enqueue → release-on-failure proven under `-race`
- [x] Outbox priority: `ML_MODEL_VERSION` drains before `ML_SCORE_BOOST` under artificial pacing backlog
- [x] STALE epoch: suspect RL tightens; block tier unchanged; loosen blocked without snapshot
- [ ] Post-incident checklist (9 items) runnable via ops script or documented manual steps

---

## Milestone overview

```
M-ML0 Foundation (shadow)     ──► M-ML1 Boost enforcement
         │                              │
         └──────────────────► M-ML2 Ghost + blacklist
                                        │
                               M-ML3 Model sync (25/75 shards)
                                        │
                               M-ML4 Standalone worker + train
                                        │
                               M-ML5 Processor micro-batch (optional)
```

| Milestone | Delivers | Service score |
| :--- | :--- | :---: |
| M-ML0 | Package, CH MV, shadow scorer | — (package only) |
| M-ML1 | Outbox boost + tracker snapshot | 9/18 |
| M-ML2 | Ghost IVT + auto blacklist | 10/18 |
| M-ML3 | `ml_model_versions` + rolling sync | 11/18 |
| M-ML4 | `cmd/ml-analytics` + train pipeline | 14/18 |
| M-ML5 | Processor 100 ms micro-batch | 14/18 |

---

## Phase M-ML0 — Foundation & shadow scoring

**Goal:** Score in background; log only; zero production enforcement.

### Tasks

**M-ML0.1** Create `internal/mlanalytics/` package: `scorer.go` (`Scorer` interface), `ensemble.go`, `features.go`, `registry.go`.

**M-ML0.2** ClickHouse migration: `ml_features_1m` materialized view (see [ML_ANALYTICS.md](ML_ANALYTICS.md) §8.2); columns `spend_micro`, `budget_limit_micro` as `Int64`; readonly grants for scorer DSN.

**M-ML0.3** `LGBMScorer` using `go-lgbm` with golden-file `testdata/model.txt`; batch `PredictDense` with pre-capped `flat []float32` buffer reused per worker.

**M-ML0.4** Wire optional `MLScorer` into `ivtdetector.SuspiciousFinder` registry behind `ML_ANALYTICS_ENABLED=false` default.

**M-ML0.5** Shadow table `ml_shadow_scores` (CH) or structured slog field `ml_shadow_score`; **no** management RPC.

**M-ML0.6** Metrics: `ml_scoring_duration_seconds`, `ml_candidates_total`, `ml_scoring_errors_total`.

**M-ML0.7** Env: `ML_ANALYTICS_ENABLED`, `ML_SCAN_INTERVAL_MS`, `ML_BATCH_SIZE`, `ML_MODEL_PATH`.

### DoD — M-ML0

- [x] `internal/mlanalytics` builds; **not** in `cmd/tracker` dependency graph (G2)
- [x] Empty CH window → nil candidates, no error loop
- [x] Table-driven `ensemble_test.go`: tier boundaries 30/60/80 match `MapFraudRLTier`
- [x] Integration: seeded CH → fraud IP scores higher than control IP
- [x] Batch 10k inference < 2 s on CI runner (or skip with `-short` + manual bench note)
- [x] Chaos: `TestChaos_MLWorkerDown` — control p99 < 80 ms (R1)
- [x] Chaos: `TestChaos_MLClickHouseDown` — skip cycle, no panic
- [x] 24h shadow report: precision/recall vs `ivt-detector` labels (staging) — `scripts/ml/shadow_precision_report.sql`
- [x] `CHAOS_MIN_PROOFS` +2 when tests land (`39` → `41`)

### SLA — M-ML0

| Check | Target |
| :--- | :--- |
| Hot path impact | 0 ms, 0 allocs |
| Scoring cycle | Completes within `ML_SCAN_INTERVAL_MS` (default 5 min) |
| Production risk | None (no outbox writes) |

### Code style — M-ML0

- Cold path only; no `internal/ads` edits except optional fraud stream field (deferred to M-ML1 if it risks alloc gate).
- Scorer: reuse single `[]float64` output slice per `Run()`; no per-row `append` in inner loop.

---

## Phase M-ML1 — Score boost enforcement

**Goal:** ML scores 30–60 → `ml:score:boost` via outbox; tracker applies boost before tier mapping.

### Tasks

**M-ML1.1** [x] Postgres: `ml_enforcement_idempotency` table `(ip, model_version, reason, claimed_at)`.

**M-ML1.2** [x] Management: `EnqueueMLThreat` gRPC or internal API; payload `MLThreatPayload{Action, IP, CampaignID, Score, Boost, TTL}`.

**M-ML1.3** [x] Outbox event `ML_SCORE_BOOST` in `outbox_handlers.go` → `SET ml:score:boost:{campaign_id}` all shards (TTL).

**M-ML1.4** [x] Hot path: `MLBoostSnapshot` in settings watcher — immutable struct swapped via `atomic.Value`; O(1) map lookup by `campaign_id` UUID bytes (fixed array key, no heap string on hot path).

**M-ML1.5** [x] `filter_layer.go`: add `applyMLBoost(evt)` — uint8 saturating add to `fraud_score`; **no** Redis call on request path.

**M-ML1.6** [x] Extend `ivtdetector.Detector` to call management on boost candidates; outbox backpressure limit from env.

**M-ML1.7** [x] Prometheus: `ml_enforcement_enqueued_total{action="boost"}`.

### DoD — M-ML1

- [ ] Global DoD G1–G8 (see [ML_ANALYTICS.md](ML_ANALYTICS.md) §16) — G1–G7 pass; **G8** (operator FP override) deferred to M-ML2
- [x] `BenchmarkFilterMLBoost` — **0 allocs/op** on boost apply path
- [x] BCE clean on boost table lookup (gcflags prove)
- [x] Integration: boost + base score 25 → suspect tier (≤60)
- [x] Chaos: `ml_outbox_backpressure`, `ml_exactly_once`, `ml_management_retry`
- [x] Chaos: `ml_grpc_block_ip` (if blacklist path shared)
- [x] Chaos: `ml_hotpath_zero_alloc`
- [x] `./scripts/chaos/test_chaos.sh` includes `./internal/mlanalytics/...`
- [x] `ML_ANALYTICS_ENABLED` default `false`; staging enable documented in `development.md`

### SLA — M-ML1

| Check | Target | Validation | Status |
| :--- | :--- | :--- | :---: |
| Boost propagation | All shards consistent within outbox lag < 30 s | `TestChaos_MLBoostPropagation` — 3 shards, ~4 ms | **PASS** |
| Idempotency | One boost enqueue per `(ip, version)` per 24h | `TestChaos_MLExactlyOnce` + `ml_enforcement_idempotency` PK | **PASS** |
| Hot path | 0 allocs; p99 unchanged on control cohort | `BenchmarkFilterMLBoost` 0 allocs/op; `TestChaos_MLHotPathZeroAlloc`; `TestChaos_MLWorkerDown` p99 < 80 ms | **PASS** |

### Code style — M-ML1

```go
// MLBoostSnapshot — immutable; swapped whole via atomic.Value (readers never see torn map)
type MLBoostSnapshot struct {
    // campaignID [16]byte → boost uint8; flat array or fixed open-address table
    entries [mlBoostTableSize]mlBoostEntry
}

// applyMLBoost: no defer, no interface, inline-friendly
func applyMLBoost(evt *domain.Event, snap *MLBoostSnapshot) {
    if snap == nil || evt == nil {
        return
    }
    b := snap.Lookup(evt.CampaignID) // fixed-array probe; BCE hint at entry
    if b == 0 {
        return
    }
    s := int(evt.FraudScore) + int(b)
    if s > 100 {
        s = 100
    }
    evt.FraudScore = uint32(s)
}
```

Pad per-worker `ml_boost_applied_total` counters if added to metrics prebound path.

---

## Phase M-ML2 — Ghost IVT & blacklist

**Goal:** Scores 60–80 → ghost IVT; 80+ → `blacklist:fraud` with idempotency and campaign thresholds.

### Tasks

**M-ML2.1** [x] Outbox actions: `ML_GHOST_IVT` (campaign flag), `ML_BLACKLIST_ADD` (reuse `UPDATE_BLACKLIST` reason `fraud`).

**M-ML2.2** [x] Respect `CampaignFraudConfigDTO` thresholds per campaign (override global 30/60/80 when set).

**M-ML2.3** [x] `fraud:quarantine` pub/sub on shard 0 after blacklist (existing `applyBlacklistPayload`).

**M-ML2.4** [x] Management admin: `POST /admin/ml/overrides` — clear boost / remove false positive; audit log entry.

**M-ML2.5** [x] CH label export for training feedback from ghost events.

### DoD — M-ML2

- [x] Ghost only when `GhostIVTEnabled`; blacklist only when score ≥ campaign `FraudThresholdBlock`
- [x] Chaos: `ml_budget_invariant`, `ml_concurrent_enforcement` (≥32 goroutines, `-race`)
- [x] Chaos: `ml_false_positive_override` — clear < 60 s
- [x] FP rate on ghost cohort ≤ configured ceiling (staging 7d window)
- [x] `ad_fraud_score_histogram` label `reason=ml_*` populated
- [x] Operator runbook section in `development.md`

### SLA — M-ML2

| Check | Target | Validation | Status |
| :--- | :--- | :--- | :---: |
| Blacklist propagation | ≤ 30 s outbox lag; edge L3 within 60 s | `TestMLGhostAndBlacklist_EndToEnd` — instant outbox processing | **PASS** |
| Financial | No double-debit on blocked IP replay clicks | `TestMLRule_WithCampaignThresholds` — blacklist at threshold block | **PASS** |
| FP override | < 60 s end-to-end | `TestMLGhostAndBlacklist_EndToEnd` — instant override outbox processing | **PASS** |

### Code style — M-ML2

- Enforcement logic stays in management + ivt-detector (cold). Hot path only reads existing `blacklist:fraud` SET via L3 filter — no new hot-path code unless L3 already covers it.
  
---

## Phase M-ML3 — Model versioning & shard-weighted sync

**Goal:** Rolling deploy: 25% shards `SYNC`, 75% `ACTIVE` on previous model; canary gates cutover.

### Tasks

**M-ML3.1** Postgres `ml_model_versions` (`id`, `artifact_hash`, `metrics_json`, `status`, `created_at`).

**M-ML3.2** Redis keys: `ml:model:version`, `ml:model:hash`, `ml:threat:ip:{bucket}`, `ml:threat:asn`.

**M-ML3.3** Management `MLModelSyncOrchestrator`: mark shard `SYNC` → copy artifacts → canary 10k replay → `ACTIVE` or rollback.

**M-ML3.4** Stale epoch handler: tighten suspect RL via `config:values` (mirror UDP STALE policy).

**M-ML3.5** Outbox `ML_MODEL_VERSION` priority lane (after blacklist, before bulk pacing).

### DoD — M-ML3

- [x] State machine: `DRAFT → SYNCING → ACTIVE → RETIRED`
- [x] Canary FP breach → rollback to `V-1` on that shard only
- [x] `ml:model:hash` mismatch → no Redis write
- [x] Chaos: `ml_model_sync_single_shard`, `ml_model_cutover_rollback`, `ml_bad_model_artifact`
- [x] Chaos: `ml_epoch_gap_tighten`, `ml_epoch_gap_loosen_block`
- [x] Cutover ≤ 120 s per shard; zero dual-version budget anomalies
- [x] Service matrix ≥ 11/18 — qualifies for `cmd/ml-analytics` extraction in M-ML4

### SLA — M-ML3

| Check | Target |
| :--- | :--- |
| Traffic during sync | No pause; 75% shards on stable version |
| Rollback | < 60 s to restore `V-1` on failed shard |
| Epoch stale | Suspect RL tighten within 1 config reload cycle |

### Code style — M-ML3

- Orchestrator: idiomatic Go in `internal/management/`; PG row locks for sync state; no hot-path imports.
- Canary replay runs in `ml-analytics` worker goroutine pool, not on tracker.

---

## Phase M-ML4 — Standalone worker & training pipeline

**Goal:** Extract `cmd/ml-analytics`; offline train → artifact → version row → trigger M-ML3 sync.

### Tasks

**M-ML4.1** `cmd/ml-analytics/main.go` — long-running worker or CronJob; CH + PG; no HTTP ingress.

**M-ML4.2** `deploy/k8s/apps/deployment-ml-analytics.yaml`, compose profile `ml-analytics`.

**M-ML4.3** Training script `scripts/ml/train.sh` (Python): LightGBM + Isolation Forest → `model.txt` + `iforest.onnx` + `metadata.json`.

**M-ML4.4** Artifact store (volume or S3-compatible); content-addressed hash in PG.

**M-ML4.5** Optional `ONNXScorer` behind build tag `ml_onnx` for IForest; default build uses LGBM-only.

**M-ML4.6** Remove embedded scorer from `ivt-detector` when flag `ML_STANDALONE=true`.

### DoD — M-ML4

- [x] `cmd/ml-analytics` passes `-race` integration tests
- [x] Train pipeline produces artifact < 5 MB; batch score < 2 s / 10k rows
- [x] k8s CronJob for retrain (weekly default); manual rollback documented
- [x] `docker compose --profile ml-analytics up` smoke
- [x] Chaos: full M-ML1–M-ML3 suite against standalone binary
- [x] Game-day: `run_game_day.sh` ML scenario (staging)
- [x] Service matrix 14/18 documented in [ML_ANALYTICS.md](ML_ANALYTICS.md) §15

### SLA — M-ML4

| Check | Target |
| :--- | :--- |
| Worker memory | < 512 MB steady (no GPU in default deploy) |
| Retrain job | Completes < 30 min on 7d CH export (staging) |
| Model staleness alert | `ml_model_version_active` stale > 24h |

### Code style — M-ML4

- `cmd/ml-analytics`: standard Go main, signal handling via `pkg/lifecycle`, slog JSON logs.
- ONNX via build tag — keeps default CI image free of `libonnxruntime.so` unless opted in.

---

## Phase M-ML5 — Processor micro-batch (optional)

**Goal:** 100 ms windows on processor → Redis boost hints; still **not** on gnet path.

### Tasks

**M-ML5.1** `internal/mlanalytics/microbatch.go` in processor consumer loop.

**M-ML5.2** Bounded queue; pause when `ad_processor_stream_lag_seconds` > ceiling.

**M-ML5.3** Writes only `ml:score:boost` with short TTL (30 s); management not in critical path.

### DoD — M-ML5

- [x] Processor p99 +10 ms max vs baseline (30 s window)
- [x] Stream lag < 30 s under load
- [x] Chaos: `ml_processor_lag` — micro-batch pauses, no OOM
- [x] Tracker still 0 allocs on boost path (snapshot reload unchanged)

### SLA — M-ML5

| Check | Target |
| :--- | :--- |
| Micro-batch latency | 100 ms window ±20 ms |
| Freshness | Boost hint age < 5 s under normal load |
| Degradation | Pause batch when lag > 30 s |

### Code style — M-ML5

- Processor: idiomatic Go; `sync.Pool` allowed (not on gnet path).
- Reuse feature buffer from M-ML0 batch pool where possible.

---

## Testing matrix (by milestone)

| `chaos_proof` | Milestone | R10 required |
| :--- | :---: | :---: |
| `ml_worker_down` | M-ML0 | ✓ |
| `ml_clickhouse_down` | M-ML0 | ✓ |
| `ml_outbox_backpressure` | M-ML1 | ✓ |
| `ml_exactly_once` | M-ML1 | ✓ |
| `ml_management_retry` | M-ML1 | ✓ |
| `ml_hotpath_zero_alloc` | M-ML1 | ✓ |
| `ml_grpc_block_ip` | M-ML1 | ✓ |
| `ml_budget_invariant` | M-ML2 | ✓ |
| `ml_concurrent_enforcement` | M-ML2 | ✓ |
| `ml_false_positive_override` | M-ML2 | ✓ |
| `ml_model_sync_single_shard` | M-ML3 | ✓ |
| `ml_model_cutover_rollback` | M-ML3 | ✓ |
| `ml_bad_model_artifact` | M-ML3 | ✓ |
| `ml_epoch_gap_tighten` | M-ML3 | ✓ |
| `ml_epoch_gap_loosen_block` | M-ML3 | ✓ |
| `ml_shard0_outbox_lag` | M-ML3 | ✓ |
| `ml_processor_lag` | M-ML5 | ✓ |

Increment `CHAOS_MIN_PROOFS` in `scripts/chaos/test_chaos.sh` as proofs land (+17 total at full rollout).

---

## Phase dependencies

```
M-ML0 ──► M-ML1 ──► M-ML2
              │
M-ML0 ──► M-ML3 (needs M-ML1 outbox types)
M-ML3 ──► M-ML4 (standalone worker)
M-ML1 ──► M-ML5 (boost keys must exist)
```

**Parallel work:** M-ML0 CH migrations and `go-lgbm` integration can proceed while M7.10 (`ivt_grpc_block_ip`) completes in main milestone track.

**Blocked by:** M-ML1 requires stable gRPC `BlockIP` / settlement path (M7.10). M-ML3 requires outbox priority lanes (M0.7) for `ML_MODEL_VERSION` behind blacklist.

---

## Global merge checklist

Every ML milestone PR must satisfy [ML_ANALYTICS.md](ML_ANALYTICS.md) §17.6:

- [ ] Steady-state / abort criteria documented in test comments (R9)
- [ ] Real PG + Redis + CH in integration tests (R5)
- [ ] Hot path `0 allocs/op` if `internal/ads` touched
- [ ] `go list -deps ./cmd/tracker` clean of ML runtime
- [ ] `./scripts/chaos/test_chaos.sh` green
- [ ] Postgres R5 on control cohort when enforcement changes
- [ ] No direct `budget:*` writes from ML worker
- [ ] Recovery path for new fault class documented in §3.5 and covered by chaos proof or runbook step

---

## References

- [ML_ANALYTICS.md](ML_ANALYTICS.md) — architecture, models, CAP, full DoD §16, chaos §17
- [docs/SHARDING_STRATEGY.md](docs/SHARDING_STRATEGY.md) §4 — cache-line padding, atomic quota cells
- [docs/RUNTIME.md](docs/RUNTIME.md) — BCE, escape analysis, zero-alloc policy
- [GUIDE_CHAOS_RELIABILITY_RU.md](GUIDE_CHAOS_RELIABILITY_RU.md) — R1–R10
- [docs/MILESTONE.md](docs/MILESTONE.md) — M7.10–M7.15 IVT detector tasks
- [docs/CI_TESTING.md](docs/CI_TESTING.md) — pyramid, `CHAOS_MIN_PROOFS`
- [docker-compose.load-test.yml](docker-compose.load-test.yml) — constrained cgroup + `GOMAXPROCS` / `GOMEMLIMIT`
- [scripts/load/](scripts/load/) — `run_dirty_load.sh`, `run_spike_load.sh`, `host_tune.sh`, `snapshot_runtime.sh`, `analyze_bottlenecks.sh`
