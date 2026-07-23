# RTB Module: Architecture, Hot-Path Engineering, and Roadmap

In-process Real-Time Bidding (RTB) on the tracker `/track` path.

**Related:** [GO.md](./GO.md) (compiler/runtime policy), [ARCHITECTURE.md](./ARCHITECTURE.md) (platform context), [REDIS.md](./REDIS.md) (Lua budget authority).

### SLA (from `.cursorrules` / M18)

| Metric | Target |
|--------|--------|
| `RunAuction` p99 | < 15 µs |
| Candidates scanned p99 | < 500 |
| Heap allocations | **0** per auction |

This document covers shipped capabilities, low-level performance techniques (per [GO.md](./GO.md)), and the improvement roadmap with implementation notes.

### Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Shipped Features](#2-shipped-features-2026-07)
3. [Environment Knobs](#3-environment-knobs)
4. [Low-Level Hot-Path Engineering](#4-low-level-hot-path-engineering)
5. [Rollout and Budget Authority](#5-rollout-and-budget-authority)
6. [Roadmap](#6-roadmap)
7. [Implementation Checklist](#7-implementation-checklist-hot-path-changes)
8. [File Reference](#8-file-reference)
9. [Suggested Sequencing](#9-suggested-sequencing)

---

## 1. Architecture Overview

RTB is **not** a standalone OpenRTB exchange endpoint today. It runs inside `processTrack()` before `FilterEngine.Check`:

```
Ingest (/track, gnet)
  → ensureIngestGeo (MaxMind → evt.GeoHash)
  → applyRtbAuction (mode-dependent)
       off    → skip
       shadow → RunAuctionEval + metrics (client campaign_id unchanged)
       live   → RunAuction + budget debit → replace evt.CampaignID
  → FilterEngine (geo, fraud, Lua unified filter, …)
  → settlement stream
```

### Package layout

| Component | Path | Role |
|-----------|------|------|
| Auction core | `internal/rtb/` | Registry, ranking, clearing, `BudgetStore`, snapshot persistence |
| Tracker glue | `internal/ingestion/rtb_*.go` | Catalog sync, `/track` integration, OpenRTB parse, shadow metrics |
| Control plane | `internal/management/handler_rtb.go`, `service_rtb_deals.go`, `service_bid_floor.go` | Deal CRUD, bid-request lint, floor optimization |
| Configuration | `internal/config/rtb.go`, `env.go` | `RTB_MODE`, budget authority, targeting index |

### Data flow (cold vs hot)

| Path | Entry points | Operations |
|------|--------------|------------|
| **Cold** | `SyncRtbCatalog`, `ReloadRtbDeals`, `UpdateCampaigns` | Rebuild geo shards, SoA buckets, inverted targeting index, presort by score |
| **Hot** | `RunAuction` → `rankCandidates` | Scan materialized SoA slices in O(window), debit budget via CAS |

Catalog readers never take writer locks: `Registry.catalog` is an `atomic.Pointer[catalogSnapshot]` swapped on rebuild.

---

## 2. Shipped Features (2026-07)

### 2.1 Auction engine (`internal/rtb/`)

| Feature | Description |
|---------|-------------|
| Winner selection | Ranks by `bid × CTR` effective score (`auction_ranking.go`). CTR in PPM fixed-point (`CTRPPMUnit = 1_000_000`); zero CTR → 1.0 |
| Tie-break | Higher `Weight`, then higher raw `Bid`. `catalog_bucket_sort.go` presorts each bucket descending |
| Early exit | `if score < maxScore { break }` after presort — valid only when bucket is fully sorted (see §4.4) |
| Clearing | First-price or second-price plus reserve. `RTB_CLEARING_MODE=first` → `ClearingFirstPrice` |
| Geo sharding | 64 shards (`geoShardCount`, mask `& 63`) via `LoadShard(req.GeoHash & mask)` |
| Geo index | `buildGeoIndex` → `GeoBucketSoA`, lookup via `sort.Search` on `GeoBucketHash` |
| Targeting index | `buildTargetingIndex` → `TargetBucketSoA`; enabled with `RTB_TARGETING_INDEX=1`. Key = geo ‖ deviceBit ‖ categoryBit |
| Budget debit | CAS loop in `CheckAndSpendAll`: campaign budget, customer pool, daily cap with rollback |
| No-bid taxonomy | `NoBidReason` (uint8) → `filterRejectKind` on live reject |
| Shadow eval | `RunAuctionEval` computes winner without spend (`RTB_MODE=shadow`) |
| Snapshot persistence | `Registry.SaveSnapshot` / `LoadSnapshot` → `RTB_SNAPSHOT_PATH`, wire format v4 |
| Deal index | `DealIndex` atomic swap; hot path enforces floors only today |

### 2.2 Tracker integration (`internal/ingestion/`)

| Feature | Description |
|---------|-------------|
| Rollout modes (`rtb_track.go`) | `off`, `shadow`, or `live` via `RTB_MODE` |
| OpenRTB 3.0 hot parse (`openrtb_parse.go`) | Substring scan (`flr`, `device.type`, `category_mask`, `deal_id`); 0 allocations |
| Legacy JSON fallback | `parseBidMicro`, `parseCategoryMask` when OpenRTB 3.0 marker absent |
| Deal floors (`EffectiveDealFloor`) | Max of publisher floor, Postgres deal floor, Redis `rtb:floor:{id}` |
| Catalog sync (`rtb_sync.go`) | Hybrid metadata → bid/CTR; periodic registry rebuild |
| Budget authority (`rtb_authority.go`) | `redis` debits in Lua; `rtb` uses in-process budget, skips Lua debit |
| Budget mirror (`rtb_budget_sync.go`) | Copies Redis balances into `BudgetStore` when authority is `rtb` |
| Reconcile sampler (`rtb_budget_reconcile.go`) | Periodic Redis vs RTB divergence metrics |
| Shadow parity (`rtb_shadow_metrics.go`, `rtb_shadow_diff.go`) | Winner mismatch, no-bid counters, hourly ring buffer |
| Pubsub reload (`rtb_deals.go`) | Channel `rtb:catalog:reload` triggers deals reload and catalog rebuild |

### 2.3 Control plane (`internal/management/`)

| Feature | Description |
|---------|-------------|
| PMP deal CRUD | `/admin/rtb/deals` — `rtb_deals`: floor, geo_mask, cat_mask, seats, pacing |
| Bid request lint | `POST /admin/rtb/validate-bid-request` — OpenRTB 2.6/3.0 JSON validation (cold path) |
| Shadow diff API | `GET /admin/rtb/shadow-diff` — in-memory hourly buckets |
| Floor optimizer | `OptimizeBidFloors` reads `rtb_deal_outcomes` from ClickHouse, writes `rtb:floor:*` to Redis |
| Outbox reload | `RELOAD_RTB_CATALOG` publishes to Redis pubsub for tracker refresh |

### 2.4 Observability

| Metric | Purpose |
|--------|---------|
| `ad_rtb_auction_duration_seconds` | Sampled (1/128) via `runtime.nanotime` |
| `ad_rtb_auction_candidates_scanned` | Scan cost per auction |
| `ad_rtb_auction_no_bid_total{reason}` | Pre-bound label counters |
| `ad_rtb_shadow_winner_mismatch_total` | Shadow vs client campaign divergence |
| `ad_rtb_budget_reconcile_high` | Redis/RTB budget divergence gate |

Grafana dashboard: `deploy/monitoring/grafana/.../rtb.json`.

### 2.5 Licensing

JWT entitlements `openrtb_engine` / `rtb_live` gate RTB. When off, `filter_entitlements.go` rejects `bid`/`rtb` events.

### 2.6 Known gaps (shipped schema, incomplete hot path)

| Area | Status |
|------|--------|
| `rtb_deals.geo_mask`, `cat_mask`, `seats`, `pacing` | Stored but **not enforced** in `rankCandidates` |
| `ReserveMicro` in catalog sync | Exists but **always 0** in `SyncRtbCatalog` |
| `rtb_deal_outcomes` in ClickHouse | Read by floor optimizer; **no hot/cold writer** |
| OpenRTB 2.6 hot parse | Cold-path validation only |
| Standalone `/openrtb/bid` | Not implemented |
| `HybridBalancer.SelectAndShard` | Built but **not called** on hot path (catalog weights only) |
| Multi-country targeting | `firstTargetCountryGeo` — first sorted country only |

---

## 3. Environment Knobs

| Variable | Default | Description |
|----------|---------|-------------|
| `RTB_MODE` | `off` | `shadow` = eval + metrics; `live` replaces `campaign_id` with winner |
| `RTB_BUDGET_AUTHORITY` | `redis` | `rtb` spends in `CheckAndSpend`; Lua skips budget debit |
| `RTB_CLEARING_MODE` | second-price | Set `first` for first-price clearing |
| `RTB_TARGETING_INDEX` | `false` | Geo + device + category inverted index |
| `RTB_SNAPSHOT_PATH` | — | Budget/catalog snapshot file path |
| `RTB_CATALOG_RELOAD_CHANNEL` | `rtb:catalog:reload` | Pubsub channel for deal/catalog reload |
| `RTB_RECONCILE_INTERVAL_MS` | `30000` | Budget divergence sampler interval |
| `RTB_BUDGET_DIVERGENCE_THRESHOLD_MICRO` | `1000` | Reconcile alert threshold |
| `RTB_RECONCILE_SAMPLE_SIZE` | `32` | Campaigns sampled per reconcile tick |
| `RTB_HYBRID_MAX_RPS_PER_NODE` | `5000` | Hybrid balancer metadata |
| `BID_FLOOR_*`, `DEAL_FLOOR_REFRESH_INTERVAL_MS` | — | See `env.go` for floor optimizer and Redis cache refresh |

System setting `rtb_budget_authority` in `system_settings` can override authority via `RtbAuthorityController`.

---

## 4. Low-Level Hot-Path Engineering

Policy source: [GO.md](./GO.md) §§4–7. RTB is the reference implementation for Structure-of-Arrays auction loops.

### 4.1 Structure of Arrays (SoA) and cache locality

**Problem (AoS):** `[]Campaign{bid, ctr, mask, …}` forces each iteration to load ~80+ bytes per candidate when only `DeviceMask` and `Bid` are needed first.

**Solution:** Two-level SoA:

1. **Shard registry** — `CampaignAuctionRegistry` holds parallel slices (`Bids []int64`, `DeviceMasks []uint8`, …).
2. **Bucket SoA** — `candidateBucketSoA` duplicates hot fields in **iteration order** for one geo/targeting bucket.

Cold path (`appendBucketCandidate`) materializes bucket rows once. Hot path never indexes `reg.Bids[i]` inside the scan loop — only `bids[pos]`, `deviceMasks[pos]`, etc.

**Cache effect:** `uint8` device/pacing masks pack ~64 values per 64-byte cache line. Sequential `pos++` access is hardware prefetcher-friendly. Eliminating `catalogIdx[pos]` → `reg.*[i]` indirection removes dependent-load stalls (pointer chasing).

### 4.2 Bounds-Check Elimination (BCE)

Go emits `runtime.panicIndex` on every `slice[i]` unless the compiler proves `i < len(slice)`.

**Pattern in `rankCandidates`:**

```go
// 1. Validate entire bucket window once (slicesValid + range check)
if bucketStart < 0 || bucketEnd < bucketStart || bucketEnd > soa.len() {
    return ..., NoBidCorruptCatalog
}
// 2. Create fixed-length sub-slices — bounds checks hoisted here
catalogIdx := soa.CatalogIdx[bucketStart:bucketEnd]
bids := soa.Bids[bucketStart:bucketEnd]
// ...
// 3. Loop uses len(catalogIdx) and pos — no panicIndex on bucket access
for pos := 0; pos < len(catalogIdx); pos++ {
    bid := bids[pos]
}
```

**Verification:** `go test -gcflags=-d=ssa/check_bce/debug=1` or `go tool objdump -S` on `rankCandidates` — no `CALL runtime.panicIndex` in the loop body (documented in M18 §7).

**Rule for new RTB loops:** One early abort per buffer at loop entry (`if len(buf) <= i { return ErrMalformed }`), not a single check at loop head only.

### 4.3 False-sharing padding (`AlignedBudget`)

Concurrent auctions on different campaigns debit different budget slots. Naive `[]int64` packs 8 slots per cache line — CAS on slot A invalidates slot B on another core (MESI **false sharing**).

```go
type AlignedBudget struct {
    Value int64
    _     [7]int64  // pad to 64 bytes (one cache line per slot)
}
```

Each `CompareAndSwapInt64` touches an isolated line. Trade-off: 8× memory for budget arrays; acceptable at campaign counts < 100k.

**Catalog config:** `clearingMode`, `targetingIndexEnabled` use `atomic.Uint32` / `atomic.Bool` — read once per auction, not per candidate.

### 4.4 Branch prediction and pipeline density

| Branch | Predictability | Notes |
|--------|----------------|-------|
| Device/category filter `(deviceMasks[pos] & deviceType) == 0` | High | Bitwise AND, no map; device type fixed per request |
| Pacing gate `pacingOpen[pos] == PacingClosed` | Medium | Stable per campaign |
| Score early break (presorted descending) | High | Mispredict once per auction after winner found |
| Budget exhausted `LoadBudget < bid` | Medium | Rises under spend pressure |
| Corrupt catalog | Rare | Cold branches hoisted by compiler |

**Monotonicity invariant:** `sortBucketSoA` sorts by `effectiveScore` desc, then `Weight`, then `Bid`. The early `break` is correct only if the bucket is fully sorted — do not skip sort on catalog rebuild.

**Avoid on hot path:** `map` lookup, `interface{}`, closures, `defer`, string `+` concat (use fixed `[N]byte` or `append` into reused buffer on cold path only).

### 4.5 Atomic snapshots (thundering herd avoidance)

| Structure | Mechanism |
|-----------|-----------|
| `Registry.catalog` | `atomic.Pointer[catalogSnapshot]` — readers load once; writers rebuild off-path and swap |
| `DealIndex.snap` | Same pattern; concurrent reload vs lookup is safe |
| `DealFloorCache.snap` | `atomic.Pointer[map[string]int64]` |
| `BudgetStore.budgets` | `atomic.Pointer[budgetSlice]` — slice growth on cold allocate path only |

**Hot-loop rule:** `store := registry.store` and `regCount := reg.Count` captured once; never `atomic.Load` inside the candidate `for` loop.

**Budget CAS:** `checkAndSpendOn` spin-CAS on `&slice.data[idx].Value` — contention per campaign, not global.

### 4.6 Zero-allocation OpenRTB parse

`ParseOpenRTB3Payload` uses `bytes.Index` on `evt.Payload` (gnet frame buffer):

- No `json.Unmarshal`, no `string` allocations for numeric fields.
- `parseDecimalMicro` manual digit walk for `flr`.
- `ParseDealID` returns `string(...)` — **one allocation** when deal_id present; acceptable on deal traffic only.

`buildRtbTargeting` runs before auction; keep parse 0-allocs for non-deal requests (benchmark: `BenchmarkBuildRtbTargeting_OpenRTB3`).

### 4.7 Monotonic time and sampled metrics

```go
//go:linkname monotonicNano runtime.nanotime
func monotonicNano() int64
```

| Optimization | Description |
|--------------|-------------|
| Monotonic clock | Immune to NTP jumps |
| `metricsEnabled` atomic | Off in benchmarks |
| `rtbAuctionMetricsSampleMask = 127` | Observe 1/128 auctions |
| `bindAuctionMetrics()` | Pre-bound Prometheus handles — no `WithLabelValues` on hot path |

### 4.8 Escape analysis and inlining

| Type / function | Behavior |
|-----------------|----------|
| `BidRequest`, `AuctionResult` | Stack-only value types |
| `RunAuction` | Inlined at `applyRtbAuction` call site |
| `rankCandidates` | `req`, `soa` do not escape; `registry` param may leak metadata but not heap boxes |

**Forbidden:** passing concrete structs as `interface{}` on the auction path; closures in `for` loops.

### 4.9 Plan 9 / assembly analysis workflow

When changing `rankCandidates` or `CheckAndSpendAll`:

```bash
go test -run='^$' -bench=BenchmarkAuction -benchmem ./internal/rtb/
go test -gcflags="-m -m" ./internal/rtb/ 2>&1 | rg 'rankCandidates|RunAuction'
go test -c -o /tmp/rtb.test ./internal/rtb/
go tool objdump -S -l /tmp/rtb.test | rg -A30 'rankCandidates'
```

| Assembly symptom | Action |
|------------------|--------|
| `CALL runtime.panicIndex` inside loop | BCE failure — add window guard |
| `CALL runtime.morestack` inside loop | Stack growth or non-inlined call |
| `LOCK` on global address in inner loop | False sharing or atomic in loop — hoist load |
| Register spill of `maxScore`/`winnerIdx` every iteration | Register pressure — simplify loop body |

### 4.10 SLA budget arithmetic

| Component | Target (p99) |
|-----------|--------------|
| Total `/track` handler | < 80 ms |
| `RunAuction` | < 15 µs |
| Candidates scanned | < 500 |
| Redis unified-filter Lua (per shard) | < 10 ms |

At ~25–112 ns/op auction (benchmark), CPU is not the bottleneck — **candidate count** and **post-auction Redis** dominate. `RTB_TARGETING_INDEX` reduces scan width; presort reduces wasted iterations after winner.

---

## 5. Rollout and Budget Authority

### 5.1 Mode matrix

| `RTB_MODE` | Auction | Budget debit | `campaign_id` |
|------------|---------|--------------|---------------|
| `off` | Skipped | — | Unchanged |
| `shadow` | `RunAuctionEval` | No | Unchanged |
| `live` | `RunAuction` | Per authority | Winner UUID replaces client ID |

### 5.2 Budget authority

| Authority | Lua `skip_budget` | Source of truth | Spend owner |
|-----------|-------------------|-----------------|-------------|
| `redis` (default) | `false` | Redis | `unified-filter.lua` |
| `rtb` | `true` | `BudgetStore.CheckAndSpendAll` | Redis mirrored + reconciled |

**Cutover sequence:**

1. `shadow` until parity rate stable
2. `live` + `redis`
3. `live` + `rtb` with reconcile gate green

---

## 6. Roadmap

Phases ordered by market requirements (OpenRTB 2.6, floor loop, supply chain, exchange surface) and eSPX gap analysis.

| Phase | Priority | Items | Focus |
|-------|----------|-------|-------|
| 1 | P0 | R1–R5 | Close measurement and PMP loop |
| 2 | P0 | R6–R9 | OpenRTB 2.6 exchange surface |
| 3 | P1 | R10–R16 | Yield and auction quality |
| 4 | P1 | R17–R20 | Trust, supply chain, IVT |
| 5 | P2 | R21–R26 | Targeting and inventory expansion |
| 6 | P2–P3 | R27–R31 | Platform and ops |

### Phase 1 — Close the measurement and PMP loop (P0)

| ID | Task |
|----|------|
| **R1** | Write `rtb_deal_outcomes` to ClickHouse. Cold path: lossy fixed ring → batch insert (`FraudStreamWriter` pattern). Fields: `deal_id`, `outcome`, `floor_micro`, `created_at`. Hook: `recordRtbShadowAuction` + live `applyRtbAuction`. 0 allocs on hot path |
| **R2** | Enforce PMP deal metadata. Extend `BidRequest` with `DealID` + `SeatCount` (stack). In `rankCandidates`: `DealIndex` lookup; reject if `geo_mask & reqGeo == 0`, `cat_mask & category == 0`, deal `PacingClosed`. Bitwise, branchless where possible |
| **R3** | Populate `ReserveMicro`. `BuildRtbInputsFromRegistry` ← campaign column. No hot-path change; reserve in `applyReserve` |
| **R4** | Graduate `RTB_TARGETING_INDEX` default-on. Load test p99 scan < 500; flip after chaos tests |
| **R5** | Automated live cutover gate. Management endpoint: `RtbShadowDiffForWindow` + `ad_rtb_budget_reconcile_high` → `ready: bool` + reasons |

### Phase 2 — OpenRTB 2.6 exchange surface (P0)

| ID | Task |
|----|------|
| **R6** | OpenRTB 2.6 hot parser (`openrtb26_parse.go`): `imp[0].bidfloor`, `imp[0].pmp.deals[]`, `device.devicetype`, `site`/`app`, `source.ext.schain`. Bytes only, BCE, no `encoding/json` on hot path |
| **R7** | `/openrtb/bid` gnet route. Parse → `buildRtbTargeting` → `RunAuction` → minimal `BidResponse`. Propagate `tmax` → monotonic deadline |
| **R8** | Bid response generation. Stack-fixed `[]byte` buffer, no `fmt.Sprintf`. Price micro-units → decimal via fixed digit buffer |
| **R9** | `tmax` deadline propagation. `BidRequest.DeadlineMono int64`; check `monotonicNano() > deadline` every N candidates (e.g. 32) |

### Phase 3 — Yield and auction quality (P1)

| ID | Task |
|----|------|
| **R10** | Multi-country geo fan-out. Cold path: one catalog row per target country |
| **R11** | Hybrid weights in ranking. Wire `HybridBalancer` → `Weight` in `RtbCampaignInput` (today hardcoded `1`) |
| **R12** | ML fraud boost: `effectiveScore = bid * ctr * boostPPM / 1e6`. `FraudScoreBoostSnapshot` once per auction; no `fraudscoring` import |
| **R13** | Pre-auction lightweight filters: geo bitmask, breaker open — before `RunAuction` in live mode |
| **R14** | Scan budget cap: `maxScan := 500` → `NoBidScanLimit` + metric |
| **R15** | Persist clearing price: `evt.ClearingPriceMicro` (vtproto) for billing audit |
| **R16** | Bid shading API (cold). Management simulate: `RunAuctionEval` + shade from second-price delta histogram |

### Phase 4 — Trust, supply chain, IVT (P1)

| ID | Task |
|----|------|
| **R17** | Pre-bid IVT gate when `RTB_PREBID_IVT=1`. Bot/datacenter check before `RunAuction` |
| **R18** | `schain` validation. Cold: `validateBidRequest`. Hot: stack array `[8]node`, allowlist `asi`/`sid` |
| **R19** | ads.txt / sellers.json audit worker. Management cron; no hot path |
| **R20** | Bidirectional budget sync when authority=rtb. Outbox → Redis after `CheckAndSpendAll` |

### Phase 5 — Targeting and inventory expansion (P2)

| ID | Task |
|----|------|
| **R21** | Placement / domain targeting. Extend targeting index key (≤ 64 bits or two-level index) |
| **R22** | Creative-level auction. SoA add `CreativeID uint32` |
| **R23** | Video / VAST awareness. Parse `imp.video` on cold path |
| **R24** | Daypart pre-filter. Bitmap in catalog row; bitwise `&` with request hour |
| **R25** | Frequency cap pre-check. Optional Redis MGET batch before auction |
| **R26** | CTV `gtax` / ECIDs. OpenRTB 2.6 `content` object; cold catalog tagging |

### Phase 6 — Platform and ops (P2–P3)

| ID | Task |
|----|------|
| **R27** | Admin simulate auction: `POST /admin/rtb/simulate` → `RunAuctionEval` |
| **R28** | A/B cohort rollout. Hash `user_id` → shadow/live bucket; metric label `cohort` |
| **R29** | ARTF local enrichment hooks. Function pointers on cold reload only — no `interface{}` in loop |
| **R30** | Multi-node budget consistency. See [MULTI_REGION.md](./MULTI_REGION.md) |
| **R31** | Wire or remove `HybridBalancer.SelectAndShard` |

---

## 7. Implementation Checklist (Hot-Path Changes)

Any RTB hot-path PR must satisfy:

```bash
go test ./internal/rtb/... -short
go test -run='^$' -bench=BenchmarkAuction -benchmem ./internal/rtb/
make test-alloc-gate    # when touching ingestion/rtb track path
go test -gcflags="-m" ./internal/rtb/ 2>&1 | rg 'does not escape'
```

| Criterion | Requirement |
|-----------|-------------|
| Allocations | 0 B/op, 0 allocs/op on `BenchmarkAuction` |
| BCE | No `panicIndex` in `rankCandidates` loop (objdump) |
| Atomics | No `atomic.Load` inside candidate loop |
| Interfaces | No `interface{}` / closures in auction path |
| Strings | No `+` concat per request in parse/rank |
| Metrics | Pre-bound counters; sampled histograms only |
| Chaos | `go test -run TestChaos ./internal/rtb/...` when changing catalog or budget |

---

## 8. File Reference

| File | Role |
|------|------|
| `internal/rtb/auction.go` | `RunAuction`, `RunAuctionEval`, clearing |
| `internal/rtb/auction_rank.go` | `rankCandidates`, BCE window, early break |
| `internal/rtb/auction_ranking.go` | `effectiveScore`, CTR PPM |
| `internal/rtb/auction_clearing.go` | First/second price, reserve |
| `internal/rtb/catalog_registry.go` | Shard rebuild, atomic publish |
| `internal/rtb/catalog_bucket_soa.go` | Bucket materialization |
| `internal/rtb/catalog_bucket_sort.go` | Presort for branch prediction |
| `internal/rtb/catalog_geo_index.go` | Geo bucket build |
| `internal/rtb/catalog_targeting_index.go` | Inverted index build |
| `internal/rtb/budget_store.go` | Aligned slots, slot allocation |
| `internal/rtb/budget_spend.go` | CAS debit, rollback |
| `internal/rtb/metrics.go` | Monotonic sampled metrics |
| `internal/rtb/persistence.go` | Snapshot v4 |
| `internal/rtb/deal_index.go` | PMP deal snapshot |
| `internal/ingestion/rtb_track.go` | Track integration, targeting build |
| `internal/ingestion/rtb_catalog.go` | `RtbCatalog` facade |
| `internal/ingestion/rtb_sync.go` | Catalog sync, hybrid bridge |
| `internal/ingestion/openrtb_parse.go` | Hot OpenRTB 3.0 substring parse |
| `internal/ingestion/openrtb_validate.go` | Cold 2.6/3.0 lint |

---

## 9. Suggested Sequencing

```
Phase 1 (R1–R5)  → measurement + PMP + cutover safety
Phase 2 (R6–R9)  → OpenRTB 2.6 surface (market blocker)
Phase 3 (R10–R16) → yield / ranking quality
Phase 4 (R17–R20) → trust + IVT + budget consistency
Phase 5–6          → inventory dimensions + ops tooling
```

| Category | Items |
|----------|-------|
| **Quick wins** | R1, R3, R5, R14 (low hot-path risk) |
| **Strategic bets** | R6–R8 (exchange endpoint), R17 (pre-bid IVT), R29 (ARTF hooks) |

---

*Last updated: 2026-07-21.*
