# Capabilities

Shipped milestones (M1–M14). Open gaps: [BACKLOG.md](./BACKLOG.md).

---

## M1 — Slot Migration and Redis Key Catalog

**Status:** complete (M1-01..M1-09)

| Deliverable | Detail |
| :--- | :--- |
| `CampaignRedisKeyCatalog` | Single key list for COPY/DRAIN/warm-up — `redis_key_catalog.go` |
| Hash-tagged COPY | `{uuid}budget:campaign:{uuid}`, dedup, idempotency, rl, imp_ts, quota, fcap |
| PG re-warm cutover | `RewarmCampaignBudgetKeys` at activation; `EXISTS` gate (default path) |
| Migration fence | `MIGRATION_FENCE_ENABLED=true` in production; Lua code `11` on fenced debit |
| Rollback playbook | [DEVELOPMENT.md](./DEVELOPMENT.md) §Slot migration |
| Dual-write cutover (M1-08) | Opt-in `SLOT_MIGRATION_DUAL_WRITE_ENABLED` — Redis stream `slot_migration:delta`, lag catch-up (`CatchUpSlotMigrationDeltas`), cutover gated on `SLOT_MIGRATION_LAG_EPSILON` + `VerifyBudgetInvariant`; fence fallback when lag > `SLOT_MIGRATION_LAG_THRESHOLD` |
| Lag alert (M1-09) | `SlotMigrationLagActive` — `ad_slot_migration_lag_messages > 0` for 30s (`prometheus.rules.yaml`) |
| Metrics | `ad_slot_migration_lag_messages`, `ad_slot_migration_dual_write_total`, `ad_slot_migration_cutover_blocked_total` |

**Chaos:** `TestChaos_LUA10_DebitFencedDuringSlotCopy`, `TestChaos_SO02_SlotMigrationPGRewarmCutover`, `TestChaos_SO02_SlotMigrationDualWriteCutover`.

---

## M2 — Elastic Triplets (Dynamic Sharding)

**Status:** complete (2026-07-24) · Detail: [DATA.md](./DATA.md) Part I §7

| Deliverable | Detail |
| :--- | :--- |
| `campaign_routing` | Per-campaign triplet home + `routing_epoch` — migration `00052_campaign_routing.sql` |
| Global `routing_epoch` | `redis_slot_map_meta.routing_epoch`; loaded into `StaticSlotSharder.MigrationGen` |
| `ShardOrchestrator` | Capacity EWMA → hottest campaign micro-migration; `SHARD_ORCHESTRATOR_ENABLED` |
| TCP cutover | HMAC-SHA256 snapshot + tracker ACK — `TCP_CONTROL_ENABLED`, `:8192` |
| Broker / edge | `SlotMapReloadMessage.routing_epoch`; `edge-slot-map.lua` epoch sync |
| Lua fence | `LuaRoutingEpoch()` wired to budget-fast / unified-filter ARGV |
| Metrics | `ad_elastic_*`, `ad_tcp_control_*` |

**Chaos:** `TestChaos_SO_NoFalseMigrate`, `TestChaos_SO_CampaignRoutingMigration`, `TestChaos_SO_RoutingEpochRace`, `TestChaos_SO_TripletFailover`, `TestChaos_TCP_SnapshotHMACACK`.

---

## M3 — Budget Integrity and Reconciliation

**Status:** complete · Detail: [DATA.md](./DATA.md) Part II §Reconciliation authority

| Deliverable | Detail |
| :--- | :--- |
| `ReconcileBudgetSnapshot` | Single `ReconWorker`; no parallel reconciler |
| Atomic Lua snapshot | `FetchBudgetReconSnapshot` — campaign, sync, inflight, quota in one `EVALSHA` |
| Grace window | Skip when `inflight > 0` within flush interval |
| Corrections | `RECONCILIATION_ADJUST` outbox only; chunk cap from `ReconService` |
| Contention | `FOR UPDATE SKIP LOCKED` on campaign flush outside strict band |
| Metrics | `ad_reconciliation_drift_micro`, `ad_reconciliation_corrections_total`, `ad_sync_lag_seconds` |

**Chaos:** `TestChaos_ReconUnderLoad` → `chaos_proof fault=recon_under_load`.

---

## M4 — Multi-Region Dedup Key Adapter (D3 v2)

**Status:** complete · Detail: [DATA.md](./DATA.md) Part VII, §Idempotency

| Deliverable | Detail |
| :--- | :--- |
| `pkg/dedupkey` | Stable scope (SSID) + `factor_u` payload hash + `factor_d` PG receipt |
| `dedup_claim_confirm` | Single PG round-trip before ledger/Redis/outbox apply |
| Integrations | `SyncWorker`, `RegionOutboxRelay`, broker PG consumer |
| Metrics | `ad_dedup_proposal_total`, `ad_dedup_mismatch_total`, `ad_dedup_confirm_latency_seconds` |

**Forbidden on `/track`:** adapter not imported from hot path.

**Chaos:** `TestChaos_DedupCrashRecovery`, `TestChaos_DedupResumeApply`, `TestChaos_DedupMultiRegionDuplicate`.

---

## M5 — HTTP/1–3 Ingress

**Status:** complete · Detail: [ARCHITECTURE.md](./ARCHITECTURE.md) §Edge ingress, [GO.md](./GO.md) §10

| Phase | Deliverable |
| :--- | :--- |
| **A (edge)** | nginx terminates H2/H3 on `:443`; upstream H1.1 to tracker |
| **B (tracker)** | `http1_fsm.go` table-FSM; chunked TE (`http1_chunked.go`); pipelining |
| **C (opt-in)** | h2c on gnet: `http2_frame.go`, `http2_hpack.go`, `handler_http2.go` |
| **D (eval)** | `cmd/tracker-quic` sidecar; `http3_frame.go`, `http3_qpack.go` |

Production default: edge terminate → H1.1 tracker. h2c and tracker-quic are evaluation paths.

---

## M6 — Hot Path and Broker Test Coverage

**Status:** complete (2026-07-24) · Detail: [DEVELOPMENT.md](./DEVELOPMENT.md) §Testing

| Area | Coverage |
| :--- | :--- |
| HTTP/1 FSM | 26+ table cases, split-TCP wire, hostile corpus (`TestChaos_HTTP1_*`) |
| Filter matrix | 17/17 `filterRejectKind` via handler + chaos |
| Broker | Live consumer flush, corrupt vtproto skip, reconcile worker, e2e `broker_ingest_test.go` |
| CI | alloc-gate (`BenchmarkHTTP1Parse`, `ParseJSONOpt`, auction); nightly broker throughput |
| OpenRTB edges | `rtb_ingest_edge_test.go`, `openrtb26_ingest_chaos_test.go` |

**Supplemental chaos (2026-07-24):** `go test ./internal/ingestion/ -run 'TestChaos_' -timeout 15m`

---

## M7 — RTB Exchange Surface (P0)

**Status:** R1–R9 complete · Detail: [RTB.md](./RTB.md) §2.6

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| R1 | `rtb_deal_outcomes` ClickHouse writer | `rtb_deal_outcomes_writer.go` |
| R2 | PMP in `rankCandidates` | `auction_rank.go`, `rtb_catalog.go` |
| R3 | `ReserveMicro` sync | `00050_campaign_reserve_micro.sql`, `rtb_sync.go` |
| R4 | `RTB_TARGETING_INDEX` default `true` | `internal/config/env.go` |
| R5 | Admin live gate | `rtb_live_gate.go`, `handler_rtb.go` |
| R6 | OpenRTB 2.6 parser (0 allocs) | `openrtb26_parse.go` |
| R7 | `POST /openrtb/bid` | `openrtb26_bid_handler.go` |
| R8 | Stack-buffer bid response | `openrtb26_bid_response.go` |
| R9 | Monotonic `tmax` deadline | `auction_rank.go`, `no_bid.go` |

**Benchmarks (linux/amd64, 3× median):**

| Benchmark | ns/op | allocs/op |
| :--- | ---: | ---: |
| `BenchmarkAuction` | ~28 | 0 |
| `BenchmarkParseOpenRTB26` | ~404 | 0 |
| `BenchmarkWriteOpenRTB26BidHTTP` | ~91 | 0 |

---

## M7 P1 — RTB Yield, Trust, and Consistency

**Status:** R10–R20 complete (2026-07-24) · Detail: [RTB.md](./RTB.md) §2.8

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| R10 | Multi-country catalog fan-out | `rtb_geo_fanout.go` |
| R11 | Hybrid ranking weights | `hybrid_balancer.go` |
| R12 | ML fraud boost in score | `auction_ranking.go`, `rtb_boost.go` |
| R13 | Pre-auction prefilter | `rtb_prefilter.go` |
| R14 | Scan limit 500 + metric | `auction_rank.go`, `metrics.go` |
| R15 | `ClearingPriceMicro` on events | `event.go`, `rtb_catalog.go` |
| R16 | Bid-shading admin API | `service_rtb_bid_shade.go` |
| R17 | Pre-bid IVT (`RTB_PREBID_IVT`) | `rtb_prefilter.go` |
| R18 | `schain` hot validation | `rtb_schain.go`, `openrtb26_parse.go` |
| R19 | Supply audit worker | `supply_audit_worker.go` |
| R20 | Redis budget mirror | `rtb_budget_mirror.go` |

**Live auction path:**

```text
prefilter (breaker, geo shard) → prebid IVT (opt) → schain (opt)
  → PMP deal gate → RunAuction → ClearingPriceMicro → budget mirror (authority=rtb)
```

**Metrics:** `ad_rtb_auction_scan_limit_total`; `ad_rtb_auction_no_bid_total{reason=prebid_ivt|schain_invalid|breaker_open|scan_limit}`.

**Verification (linux/amd64, Go 1.25.12, 2026-07-24):**

| Command | Result |
| :--- | :--- |
| `go test ./internal/ingestion/... -run Rtb -short` | PASS |
| `go test ./internal/rtb/... -short` | PASS |
| `make test-alloc-gate` | PASS |

| Benchmark | ns/op | allocs/op |
| :--- | ---: | ---: |
| `BenchmarkAuction` | ~37 | 0 |
| `BenchmarkParseOpenRTB26` | ~618 | 0 |

**Backlog:** R21–R31 — [BACKLOG.md](./BACKLOG.md) §1 (GAP-RTB-10..12).

---

## M8 — Local Budget Quanta

**Status:** complete (2026-07-24) · Detail: [DATA.md](./DATA.md) Part II §Reconciliation authority, [GO.md](./GO.md) §1

Amortizes Redis Lua budget RTT for high-RPS quota-mode campaigns. Local RAM debit is **never** unaccounted: every spend publishes a vtproto `BudgetDelta` to broker topic `budget-deltas`; processor and management recon include pending broker deltas in the M3 snapshot formula.

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M8-01 | Campaign-global `LocalQuantaLedger` — SoA + cache-line pad, `TrySpendLocal` 0 allocs/op | `local_quanta.go` |
| M8-02 | Async refill at 80% (`QuotaRefillWorker`, `local-quota-refill.lua`) | `local_quanta_refill.go` |
| M8-03 | Strict mode with hysteresis (`QUOTA_STRICT_THRESHOLD_MICRO` / `QUOTA_STRICT_EXIT_MICRO`) | `local_quanta_strict.go` |
| M8-04 | Broker `budget-deltas` topic; `BudgetDeltaAggregator` for recon | `local_quanta_broker.go`, `api/events.proto` |
| M8-05 | Dedup/fcap/idempotency stay in Redis (`budget-fast.lua` with `skip_budget=1`) | `local_quanta_filter.go` |
| M8-06 | `LOCAL_QUOTA_MODE=off\|shadow\|live` canary gate | `internal/config/env.go` |
| M8-07 | RPS-adaptive chunk size (`AdaptiveChunkSize`, EMA per cell) | `local_quanta.go`, refill worker |
| M8-08 | One logical pool per `campaign_id` across pinned workers | `LocalQuantaLedger` (process-global slots) |
| M8-09 | Tracker boot: `FetchRecoveryDeltas` → `RecoverFromDeltas` | `cmd/tracker/main.go` |
| M8-10 | Refill herd control: jitter + per-shard cap + `budget:refill_lock` | `local_quanta_refill.go` |

**Live path (impression, fast-path eligible, not strict):**

```text
TrySpendLocal (RAM) → BudgetDeltaPublisher → budget-fast.lua (skip_budget=1: idempotency + XADD)
```

At 80% depletion → background `QuotaRefillWorker` → Redis `DECRBY budget:quota` → `Credit` local ledger.

**Enablement:** requires `QUOTA_MODE=live` (or `shadow` for distributed quota). Run `LOCAL_QUOTA_MODE=shadow` ≥ 24 h (`ad_local_quota_shadow_diff_total` < 0.1%) before `LOCAL_QUOTA_MODE=live`.

**Metrics:** `ad_local_quota_spend_total`, `ad_local_quota_refill_total{status}`, `ad_local_quota_refill_herd_total`, `ad_local_quota_shadow_diff_total`, `ad_local_quota_flush_total{reason}` (M14-13/15).

**Benchmarks (linux/amd64, Go 1.25, 2026-07-24):**

| Benchmark | ns/op | allocs/op |
| :--- | ---: | ---: |
| `BenchmarkLocalQuantaSpend` | ~13 | 0 |
| `BenchmarkLocalQuantaSpend_parallel` (8 workers) | ~90 | 0 |
| 1M `TrySpendLocal` ops | ~20 ms | — |

**Verification:**

```bash
go test ./internal/ingestion/... -run 'Quota|LocalQuota|LocalQuanta' -short
go test ./internal/ingestion/... -bench=BenchmarkLocalQuanta -benchmem
make test-alloc-gate
```

**Chaos / load-test gates (ops):** `scripts/perf-gate/perf_gate_run.sh`, `./scripts/chaos-drills/test_chaos.sh` — budget invariant under quanta + strict.

---

## M9 — Edge Lua and Redis RTT Consolidation

**Status:** complete (2026-07-24) · Detail: [DATA.md](./DATA.md) Part I §3–4

Single `EVALSHA` per filter check on the impression fast path; IP rate limits at edge only; nginx shard balancer parity with Go `StaticSlotSharder`; zero heap allocations on Lua eval wire path.

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M9-01 | Tier B impressions default (`LUA_FAST_PATH_ENABLED=true`) | `lua_router.go`, `internal/config/env.go` |
| M9-02 | Consolidated Lua pre-checks (fraud signal, placement, ingress quota) | `budget-fast.lua`, `unified-filter.lua`, `lua_precheck.go` |
| M9-03 | IP rate limit at XDP/nginx only; `rateLimit=0` on tracker | `cmd/tracker/main.go`, `unified-filter.lua` |
| M9-04 | In-Lua tier degradation (&lt; 2 ms deadline); `filter_tier_degraded_total` | `unified-filter.lua`, `metrics/collectors.go` |
| M9-05 | Nginx `get_shard()` upstream balancer | `deploy/nginx/lua/edge-shard-balancer.lua`, `lua_consolidation_test.go` |
| M9-06 | 40/40/20 triplet documented as canary-only | [DATA.md](./DATA.md) Part I §3 |
| M9-07 | `SelectAndShard` behind `jumphash` build tag; CI guard | `hybrid_balancer_jumphash.go`, `scripts/ci/check_compliance.sh` |
| M9-08 | Sticky eval pins — 0 allocs/op on `EVALSHA`; conn reopen; pool reserve | `redis_eval_pin.go`, `redis_eval_pooled.go`, `database/redis_shards.go` |

**Impression fast path:**

```text
FilterEngine → Tier B budget-fast.lua (1× EVALSHA: budget + pre-checks + XADD)
```

**Sticky pin rules:** one `redis.Conn` per `FilterWorkerIdx` × shard; index set only when `PinnedWorkerPool` offloads the request; `FilterWorkerIdx < 0` or ≥ pin table → pooled client (safe for tests); reopen on dead TCP before retry.

**Benchmarks (linux/amd64, real Redis testcontainer, 3× median, 2026-07-24):**

| Benchmark | Tier | ns/op | allocs/op |
| :--- | :--- | ---: | ---: |
| `BenchmarkLuaScript_Happy` | B (impression) | ~81k | **0** |
| `BenchmarkLuaScript_Worst` | C (click + fcap/pacing/TTC) | ~92k | **0** |
| `BenchmarkUnifiedFilter_Check_FastPath_RealRedis` | B end-to-end | ~77k | **0** |
| `BenchmarkUnifiedFilter_Check_RealRedis` | C end-to-end | ~100k | **0** |

Before sticky pins (2026-07-24): Tier B/C benches reported **3–5 allocs/op** (go-redis `connCheck` + `int64` boxing on deadline args). After: pooled wire slices, pre-boxed Lua args, `StringVal` deadline encoding, sticky `Conn.Process`.

**Verification:**

```bash
go test ./internal/ingestion/... -run 'Filter|Lua|Unified|EdgePin' -short
go test ./internal/ingestion/... -bench='BenchmarkLuaScript|BenchmarkUnifiedFilter_Check' -benchmem
make test-alloc-gate
```

**Chaos:** `TestChaos_EdgeSlotMapParity`, `TestChaos_LuaFastPathP99`, `TestEdgePin_*` (sticky conn regression).

**Closed gaps:** GAP-SHARD-06, GAP-HOT-03.

---

## M10 — XDP Edge L4 (Tiers A–C)

**Status:** complete (B2 spoof-block deferred) · Detail: [EBPF.md](./EBPF.md) §3–6

### Tier A — protocol hygiene

| ID | Deliverable |
| :--- | :--- |
| M10-A1..A3 | TCP anomaly drop, invalid SYN, non-TCP on `:8180` |
| M10-A4 | Runtime `config` ARRAY map (no 8-CPU hardcode) |
| M10-A5 | RST rate limit per IP |
| M10-A6 | Debian BPF build path |
| M10-obs | `ad_xdp_pass_total`, `ad_xdp_drop_total` via `edge-bpf-sync` |

### Tier B — SYN-flood resilience

| ID | Deliverable |
| :--- | :--- |
| M10-B1 | Stateless SYN cookies (`XDP_SYN_COOKIE=1`, tail-call `xdp_syn_cookie`) |
| M10-B3 | Violation ringbuf → `edge-bpf-sync` autoban → Redis LPM (no outbound to offender) |
| M10-B4 | `/24` SYN cap (`syn_subnet_ratelimit_v4`, default 256/s) |
| M10-B2 | *Deferred* — `tcp_retransmit_synack` trace → spoof block (legal review) |

### Tier C — passive IVT signals

| ID | Deliverable |
| :--- | :--- |
| M10-C1 | SYN TCP fingerprint (window, MSS, doff, TTL) → ringbuf |
| M10-C2 | `ivt-detector` correlation TCP hash × UA × JA3 → `ML_GHOST_IVT` outbox |
| M10-C3 | Fingerprint never sole `XDP_DROP` cause (`check_compliance.sh` + tests) |
| M10-C4 | Stats snapshot → Prometheus + Redis dashboard (`xdpstats`) |

### Verification

```bash
go test ./internal/edge/bpf/... -count=1          # privileged: CAP_BPF + memlock
bash scripts/ci/check_compliance.sh
```

**linux/amd64 bench (privileged Docker, 2026-07-24):** all `BenchmarkXDP_*` at **0 allocs/op**, ~837–1013 ns/op (`Program.Run` + reused `DataOut`).

---

## M11 — Adaptive Fraud Telemetry Aggregation

**Status:** complete (M11-01..M11-05) · Code: `fraud_stream_queue.go`, `fraud_stream_aggregate.go`

| ID | Deliverable |
| :--- | :--- |
| M11-01 | **80% ring threshold** — `alloc−read ≥ 0.8 × fraudRingUsable` (3276/4095) switches to fixed **4096-slot** hash table `[IPv4 /24 + FraudReasonID] → atomic.Uint64`; `BenchmarkFraudAggregate` **0 allocs/op** (~23 ns/op) |
| M11-02 | **75 ms flush worker** — cold goroutine pipelines `XADD` with `type=fraud_aggregate`, `subnet`, `fraud_reason`, `count`, `window_ms` to `FRAUD_STREAM_NAME` |
| M11-03 | **L3/L1 priority** — `l3_blocklist` and dual L1-high (`FraudLayerL1Reject`) always full ring enqueue; never aggregated |
| M11-04 | **Metrics** — `ad_fraud_stream_mode{aggregating}`, `ad_fraud_stream_aggregated_total`, `ad_fraud_stream_dropped_total` (aggregate table overflow only), `ad_fraud_stream_agg_table_fill_ratio`; alert `FraudStreamAggTablePressure` at >90% |
| M11-05 | **ClickHouse sink** — `fraud_aggregate_spikes` (`SummingMergeTree`); processor fraud consumer routes `type=fraud_aggregate` via `clickhouse_store.go` |

**Ring vs aggregate drops:** `ad_fraud_stream_drop_total` — ring full; `ad_fraud_stream_dropped_total` — aggregate hash probe exhaustion.

**Spike query (1 h):**

```sql
SELECT subnet, sum(event_count) AS total_events
FROM fraud_aggregate_spikes
WHERE created_at >= now() - INTERVAL 1 HOUR
GROUP BY subnet
HAVING total_events > 1000
ORDER BY total_events DESC;
```

`SummingMergeTree` pre-sums `event_count` per `(subnet, fraud_reason, created_at)` at merge; the query aggregates across reasons in the hour window.

### Verification

```bash
go test ./internal/ingestion/... -run 'FraudStream' -short
go test ./internal/ingestion/... -bench=BenchmarkFraudAggregate -benchmem
make test-alloc-gate
```

**Chaos:** `TestChaos_FraudStreamL3NeverAggregated` → `chaos_proof fault=fraud_stream_l3_never_aggregated`.

**Bench (linux/amd64, GOMAXPROCS=12, 3× median, 2026-07-24):**

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkFraudAggregate` | **~23** | **0** | **0** |

Synthetic **51.2k** fraud events/s (`TestFraudStreamWriter_spike50kZeroRingDrops`): **0** ring drops; events folded into aggregates.

---

## M12 — Parsing Consolidation and OpenRTB Ingress

**Status:** complete (M12-01..M12-08, M12-06, M12-07) · Detail: [EDGE.md](./EDGE.md), [GO.md](./GO.md) §Wire DFA benchmarks

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M12-01 | `ParseTrackRequestJSONOpt` wired on stdlib + gnet `/track` | `handler.go`, `track_ingest_gnet.go`, `requests_parse_opt.go` |
| M12-02 | OpenRTB extract → incremental JSON FSM (no `bytes.Index`) | `openrtb_ingress_parse.go`, `openrtb_parse.go` |
| M12-08 | **Default** ingress schema `openrtb_3`; opt-in `espx_native` | `internal/config/ingress.go`, `edge-parse-dfa.lua`, `ingress_metrics.go` |
| M12-03 | Processor stream decode: vtproto field `d` + `fraud_aggregate` only | `processor.go`, `fraud_stream_queue.go` |
| M12-04 | `ParseUUID([]byte, &id)` on warm paths | `registry.go`, `settings.go` |
| M12-06 | GO.md ↔ table-FSM (M5-B) synced | `docs/GO.md` |
| M12-07 | Alloc-gate: `BenchmarkHTTP1Parse`, `BenchmarkTrackRequest_ParseJSONOpt` | `Makefile` `test-alloc-gate` |

**Ingress schema matrix:**

| Mode | `TRACKER_INGRESS_SCHEMA` | `/track` body | Edge DFA | Hot parser |
| :--- | :--- | :--- | :--- | :--- |
| **Default** | `openrtb_3` (`config.Load`) | OpenRTB 3.0 / AdCOM JSON | `item[0].id` extract | `ParseOpenRTB3Ingress` FSM |
| **Optional** | `espx_native` | `TrackRequest` JSON or `AdEvent` vtproto | `campaign_id` scan | `ParseTrackRequestJSON*` / `UnmarshalVT` |
| **Bid** | always OpenRTB | OpenRTB 2.6/3.0 BidRequest | N/A | `openrtb26_parse.go` (shared FSM core) |

**Post-ship optimizations:** OpenRTB parse cached on `Event.Scratch` at ingress (`openrtb_scratch.go`); `buildRtbTargeting` reuses cache instead of re-parsing. Legacy flat `bid_micro` / `category_mask` JSON counted via `espx_ingress_legacy_json_total` (one-release sunset).

**vtproto vs DFA (guide §9):** OpenRTB / fixed JSON subset → DFA; eSPX protobuf → vtproto only in `espx_native`; admin JSON → `encoding/json`.

### Verification

```bash
go test ./internal/ingestion/... -run 'ParseTrack|ParseUUID|OpenRTB|BuildRtb' -short
go test ./internal/ingestion/... -bench='BenchmarkTrackRequest_ParseJSON|BenchmarkBuildRtbTargeting|BenchmarkParseOpenRTB3FSM' -benchmem
make test-alloc-gate   # or direct alloc-gate commands if gen/buf remote unavailable
```

**Bench (linux/amd64, GOMAXPROCS=12, 2026-07-24):**

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkHTTP1Parse` | ~505 | 0 | 0 |
| `BenchmarkParseOpenRTB3FSM` | ~285 | 0 | 0 |
| `BenchmarkTrackRequest_ParseJSONOpt` | ~169 | 0 | 0 |
| `BenchmarkTrackRequest_ParseJSON_Legacy` | ~175 | 0 | 0 |
| `BenchmarkBuildRtbTargeting_OpenRTB3` | ~62 | 0 | 0 |
| `BenchmarkBuildRtbTargeting_Legacy` | ~202 | 0 | 0 |

**Backlog (optional):** M12-05 binary campaign replica (`registry.saveReplica/loadReplica`) — not required for closure.

---

## M13 — Runtime Tuning and Installer Safety

**Status:** complete (M13-01..M13-05) · Installer: [deploy/installer/README.md](../deploy/installer/README.md), `internal/installer/`

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M13-01 | Tracker **`GOGC=300`**, `GOMEMLIMIT=700MiB`; processor `GOGC=100` unchanged | `docker-compose.yaml`, `deployment-trackers.yaml.tpl`, `README.md` |
| M13-02 | Slot hash eval — **CRC32 Castagnoli retained** (faster, equal entropy) | `slot_hash_eval_test.go`, `sharding.go`, `edge-slot-map.lua` |
| M13-03 | Binary rollback: backup → replace → 1 s `--health-probe`; systemd `OnFailure` rollback | `apply_binary.go`, `rollback_cli.go`, `render_systemd.go` |
| M13-04 | Compose/k8s inline GOGC/GOMEMLIMIT documentation | `docker-compose.yaml`, `deployment-trackers.yaml.tpl` |
| M13-05 | Installer `ingress_schema` (`openrtb_3` default, `espx_native` opt-in) rendered to secrets/systemd/compose | `profile.go`, `render_env.go`, `configure.go` |

**GC policy (M13-01):** Hot path is 0 allocs/op (`make test-alloc-gate`). With stable heap under `GOMEMLIMIT`, tracker `GOGC` raised from 50 → 300 to reduce GC CPU; soft limit unchanged at 700 MiB. Validate CPU/STW under dirty load before production: `scripts/load-test/run_dirty_load.sh` + `go_gc_duration_seconds` p99.

**Slot hash (M13-02):** amd64 bench — CRC32 **2.2 ns/op**, xxhash64 3.4 ns/op, murmur3 7.5 ns/op; shard entropy ~0.9999 (4-way, 100k UUIDs). No `slot_map_version` bump.

**Installer rollback (M13-03):** `apply` backs up to `~/.espx/backup/<service>-<version>`, probes with `<binary> --health-probe <url>` (1 s timeout), restores on failure. `espx-install rollback <tracker|processor>` for crash-loop recovery via `espx-rollback@.service`.

### Verification

```bash
make test-alloc-gate
go test ./internal/installer/... -short
go test ./internal/ingestion/... -run TestSlotHashEntropy -bench='BenchmarkSlotHash' -benchmem -short
```

**Bench (linux/amd64, GOMAXPROCS=12, 2026-07-24):**

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkSlotHash_CRC32` | **~2.2** | 0 | 0 |
| `BenchmarkSlotHash_xxhash64` | ~3.4 | 0 | 0 |
| `BenchmarkSlotHash_murmur3` | ~7.5 | 0 | 0 |
| `BenchmarkHTTP1Parse` (post-GOGC retune) | ~519 | 0 | 0 |
| `BenchmarkAuction` | ~30 | 0 | 0 |

**Operator validation (optional):** load-test CPU delta at `GOGC=300` vs 50 — target ~3% CPU reduction without `go_gc_duration_seconds` p99 regression > 20%.

---

## M14 — Shard-0 Survival, Ingress Hardening, Quanta Lifecycle

**Status:** complete (2026-07-24) · Shard-0 report: [M14_SHARD0_TECHNICAL_REPORT.md](./M14_SHARD0_TECHNICAL_REPORT.md)

Closes GAP-SHARD-04, GAP-WIRE-03, GAP-WIRE-04; partial GAP-OPS-04 (fraud backlog), GAP-CMP-01 (edge tarpit).

### Shard-0 outage survival (M14-01..05)

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M14-01 | Global key fan-out (`blacklist:*`, `ml:score:boost:*`, placement pause) + local-shard reads | `redis_global.go`, outbox, `settings.go` |
| M14-02 | Registry stale-serve (`REGISTRY_STALE_TTL`, `503 registry_stale`, `ad_registry_stale_mode`) | `registry_stale.go`, `registry.go` |
| M14-03 | Campaign-update broker fallback (`CAMPAIGN_UPDATE_BROKER_FALLBACK`) | `campaign_update_watcher.go` |
| M14-04 | Shard-0 ingest reroute / `503 shard_unavailable` | `shard_resolve.go`, UnifiedFilter breakers |
| M14-05 | Alert `Shard0PubSubUnreachable`, runbook, chaos drill | `prometheus.rules.yaml`, `DEVELOPMENT.md`, `m14_shard0_failure.sh` |

### Wire security (M14-06..09)

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M14-06 | `MaxJSONDepth=16` / `OrtbMaxJSONDepth=32`; `ErrMalformed` | `json_depth.go`, `requests_parse.go` |
| M14-07 | H2 incomplete spin → Close; `H2_INCOMPLETE_MAX` | `handler_http2.go`, `http2_conn.go` |
| M14-08 | Edge tarpit opt-in (`EDGE_TARPIT_ENABLED`) | `edge-tarpit.lua`, `access-check.lua` |
| M14-09 | Hostile corpus CI proofs | `wire_m14_chaos_test.go`, `wire_security_gap_chaos_test.go` |

### Fraud storm telemetry (M14-10..12)

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M14-10 | Critical 512 + analytical 3584 fraud rings | `fraud_stream_queue.go` |
| M14-11 | Ring fill / pending / PEL age + Grafana | `fraud_backpressure.go`, `main.json` |
| M14-12 | `FRAUD_CONSUMER_LAG_SEC` → `aggregating=force` | `fraud_backpressure.go`, tracker/processor mains |

### Local quanta lifecycle + Lua observability (M14-13..17)

| ID | Deliverable | Key files |
| :--- | :--- | :--- |
| M14-13 | Pause/eviction flush → Redis `INCRBY` + broker return delta | `local_quanta_flush.go`, `local-quota-return.lua`, `registry.go` |
| M14-14 | SIGTERM `FlushAll` before refill/publisher/pin close | `cmd/tracker/main.go` |
| M14-15 | `AdaptiveChunkSizeStrict` + strict-enter flush | `local_quanta_refill.go` |
| M14-16 | `filter_lua_branch_total{branch}` from Lua return codes | `lua_precheck.go`, `handleLuaResult` |
| M14-17 | `FILTER_SLOW_MS` slog `campaign_id`+`tier` | `filter_metrics.go`, `config/env.go` |

**Verification:**

```bash
go test ./internal/ingestion/ -run 'RegistryStale|ResolveDebitShard|DepthCap|WireJSON|H2Hostile|FraudStream|CriticalLane|LocalQuota|Quanta|AdaptiveChunk|LuaBranch' -short -count=1
go test ./tests/chaos/ -run TestChaos_Shard0Outage -timeout 15m   # Docker
bash scripts/chaos-drills/m14_shard0_failure.sh
```

**Chaos proofs:** `shard0_survival_shards_1_3`, `wire_json_depth_reject`, `h2_hostile_disconnect`, `fraud_critical_lane_no_agg`, `quanta_graceful_shutdown` — see [CHAOS.md](./CHAOS.md).

---

## Wire DFA benchmarks (HTTP/1–3)

Registered in `internal/ingestion/http_dfa_bench_test.go`. Run:

```bash
go test -run='^$' -bench='BenchmarkHTTP[123]DFA_' -benchmem -count=3 ./internal/ingestion/...
```

**linux/amd64, GOMAXPROCS=12, 3× median (2026-07-24):**

| Benchmark | Corpus | ns/op | MB/s | allocs/op |
| :--- | :--- | ---: | ---: | ---: |
| `BenchmarkHTTP1DFA_Happy` | POST `/track` minimal headers | **~70** | ~1625 | **0** |
| `BenchmarkHTTP1DFA_Worst` | Full nginx edge header set | **~498** | ~753 | **0** |
| `BenchmarkHTTP2DFA_Happy` | 9-byte DATA frame header | **~8.5** | ~1645 | **0** |
| `BenchmarkHTTP2DFA_Worst` | Full h2c preface + SETTINGS + HEADERS + DATA `/track` | **~112** | ~1155 | **0** |
| `BenchmarkHTTP3DFA_Happy` | Single QUIC varint decode | **~2.2** | ~450 | **0** |
| `BenchmarkHTTP3DFA_Worst` | H3 HEADERS + DATA frames → `parsedHTTPRequest` | **~50** | ~1660 | **0** |

Legacy aliases: `BenchmarkHTTP1Parse`, `BenchmarkHTTP2DecodeFrame`, `BenchmarkHTTP3VarintDecode` — same hot paths, perf-gate registered.

**Track / OpenRTB body parse (M12):** see [CAPABILITIES.md](./CAPABILITIES.md) §M12 for `BenchmarkParseOpenRTB3FSM`, `BenchmarkTrackRequest_ParseJSONOpt`, `BenchmarkBuildRtbTargeting_*`.

**SLA context:** wire parse is not the tracker bottleneck (Redis Lua p99 < 10 ms dominates). DFA benches guard regressions on 0 allocs/op.

---
