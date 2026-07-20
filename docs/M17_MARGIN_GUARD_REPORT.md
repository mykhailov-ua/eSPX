# M17 Margin Guard & Placement Auto-Pauser Technical Report

## Features Implemented

### 1. ClickHouse Placement-Level Analytics
- Added `placement_id` to `impressions`, `clicks`, and `conversions` tables (`00005_placement_counts.sql`).
- Created `placement_stats_hourly` SummingMergeTree fed by three MVs (money, clicks, conversions).
- Updated `ClickHouseStore` batch append to include `placement_id`.

### 2. Margin Guard Engine (`internal/marginguard`)
- **ROI floor**: pause when `ROI < policy.RoiFloorPct` (default −30%).
- **Zero-conv streak**: pause when `conversions == 0` and `clicks >= zero_conv_streak` (default 100).
- **Sample gate**: no action until `clicks >= min_clicks` (default 50).

### 3. Margin Guard Worker (`cmd/margin-guard`)
- 60 s evaluation loop over `placement_stats_hourly`.
- **Stale guard**: skip when CH lag > 300 s.
- **Pro tier gate**: checks `ent.Features.MarginGuard` via campaign registry.
- **Outbox**: `PAUSE_PLACEMENT` → Redis `HSET blacklist:placement:{campaign_id}` on all shards.
- **Notifier**: Telegram alert with metric snapshot on pause.

### 4. Hot-Path Placement Blacklist
- `PlacementBlacklistFilter` in `FilterEngine` chain (before L3 fraud blacklist).
- Redis key: `blacklist:placement:{campaign_id}` field = `placement_id`.
- Returns `ErrPlacementBlocked` → HTTP 403 / gnet `respPlacementBlocked`.

### 5. Admin API
- `GET/POST /api/v1/margin-guard/policies`
- `GET /api/v1/margin-guard/activity`
- `POST /api/v1/margin-guard/overrides` (resume placement)

---

## Testing Results

| Suite | Command | Result |
| :--- | :--- | :--- |
| Rule engine | `go test ./internal/marginguard/... -short` | PASS (9 cases) |
| Zero-alloc hot path | `TestPlacementBlacklistFilter_zeroAlloc` | PASS (0 allocs/op) |
| Escape guard | `TestPlacementBlacklistFilter_escapeClean` | PASS |
| Postgres EXPLAIN | `TestMarginGuardExplainQueryPlans` | PASS (see below) |

---

## Hot-Path Performance

Environment: `linux/amd64`, Go 1.25, Intel i5-11400H @ 2.70 GHz.

### Benchmarks (`-benchmem`, mock Redis, 5 runs)

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkPlacementBlacklistFilter_miss` | **38.7** (p50 of 5) | 0 | 0 |
| `BenchmarkPlacementBlacklistFilter_hit` | **39.1** (p50 of 5) | 0 | 0 |

Hit vs miss latency is identical in-process because the mock returns synchronously; production adds one Redis `HEXISTS` RTT (~0.3–1 ms LAN, budget < 5 s absorb SLA is dominated by outbox fan-out).

### Escape Analysis — `PlacementBlacklistFilter.Check`

```
cannot inline (*PlacementBlacklistFilter).Check: cost 354 exceeds budget 80
```

Compile-time `-m=2` notes `append(..., "blacklist:placement:"...)` *may* escape on first grow; **runtime `testing.AllocsPerRun` confirms 0 allocs/op** after pool warm-up because `bufPool` returns `cap=128` slices (prefix 20 + UUID 36 = 56 bytes).

Key construction uses `bufPool` + `appendUUID` + `unsafeString` (same pattern as `DuplicateEventFilter`).

### ASM Output (`go tool objdump -S -s 'PlacementBlacklistFilter.*Check'`)

Critical hot-path sequence (miss path, abbreviated):

```asm
; early guard: placement_id present
CMPQ 0x48(DI), $0          ; len(PlacementID)
JE   return_nil            ; skip when empty (fast path for no-subid traffic)

; pool acquire + key build (no heap on steady state)
CALL sync.(*Pool).Get
CMPQ DX, $0x14             ; cap >= 20?
JAE  skip_grow             ; taken: pooled buffer already sized
CALL runtime.growslice     ; cold: first pool fill only
CALL runtime.memmove       ; copy "blacklist:placement:" prefix
CALL appendUUID            ; 36-byte canonical UUID in-place

; redis interface dispatch (production: HEXISTS)
CALL rdb.HExists(...).Result

CALL sync.(*Pool).Put
TESTL CL, CL               ; isBlacklisted?
JE   return_nil            ; miss: allowed (predictable not-taken on hit bench)
MOVQ ErrPlacementBlocked   ; hit: reject
RET
```

### Branch Prediction Analysis

`perf stat` unavailable on this host (`kernel.perf_event_paranoid=4`). Static analysis from disassembly:

| Branch | Offset | Steady-state bias | Notes |
| :--- | :--- | :--- | :--- |
| `evt == nil` | `0xde6b8a` | Not taken | Filter always receives pooled event |
| `PlacementID == ""` | `0xde6b95` | **Campaign-dependent** | Bimodal: skip filter entirely when empty |
| `len(rdbs)==0` / shard nil | `0xde6ba0` | Not taken | Production always has shard 0 |
| `cap < 20` (growslice) | `0xde6c11` | Not taken | After warm-up, pool buf cap 128 |
| Redis `err != nil` | `0xde6d56` | Not taken | Fail-open; rare infra faults |
| `isBlacklisted` | `0xde6d62` | **Not taken** on miss | Paused placements are tail traffic (<1%) |

Predictor impact: the dominant production path (allowed placement with `placement_id` set) executes a linear chain with 5 highly stable forward branches before Redis I/O. Mispredict cost is ~1–2 cycles per branch on Skylake-class CPUs; filter overhead excluding Redis is **~40 ns** (≈100–120 cycles at 2.7 GHz), well inside the 50 ms tracker SLA.

---

## Cold-Path Performance (Margin Guard Worker)

### `Evaluate` Benchmark

| Metric | Value |
| :--- | ---: |
| ns/op | **714** (median of 5 runs) |
| B/op | 504 |
| allocs/op | 9 |

Allocations come from `map[string]any` metrics + `fmt.Sprintf` reason strings — acceptable on 60 s worker cadence, not on gnet loop.

### pprof — `BenchmarkEvaluate` (1.55M iter, 2.1 s CPU)

**CPU top:**
```
77.9%  marginguard.Evaluate
33.5%  fmt.(*pp).doPrintf        ← Reason string formatting
18.5%  runtime.mapassign_faststr ← metrics map
14.6%  runtime.scanobject/GC
```

**Heap (alloc_space):**
```
98.4%  marginguard.Evaluate
 6.5%  fmt.Sprintf
```

**Recommendation (future):** replace `map[string]any` + `Sprintf` with stack-fixed `[N]byte` reason buffer and typed `DecisionMetrics` struct to cut worker allocations; not required for M17 SLA.

### Escape Analysis — Cold Path

```
Evaluate: cannot inline (cost 269)
  metrics map[string]any{...} escapes to heap
  &Decision{...} escapes to heap
  fmt.Sprintf args escape to heap

fetchActivePolicies: rows.Scan → []*Policy heap growth (expected)
applyDecision: json.Marshal, pool.Exec → heap (expected async path)
```

---

## Postgres Query Plans

Captured via `TestMarginGuardExplainQueryPlans` (Postgres 16, testcontainers, `EXPLAIN ANALYZE, BUFFERS`).

### `fetch_active_policies`

```sql
SELECT id, campaign_id, name, min_clicks, roi_floor_pct, zero_conv_streak, is_active
FROM margin_guard_policies WHERE is_active = true
```

```
Seq Scan on margin_guard_policies  (cost=0.00..16.50 rows=325 width=81)
  Filter: is_active
Execution Time: 0.011 ms
```

At low policy cardinality a seq scan is optimal. Add partial index `(campaign_id) WHERE is_active` if policy table grows past ~10k rows.

### `dedupe_pause_exists` (per placement decision)

```sql
SELECT EXISTS(
  SELECT 1 FROM margin_guard_activity
  WHERE campaign_id = $1 AND placement_id = $2 AND action = 'pause'
    AND created_at > now() - INTERVAL '1 day'
)
```

```
Result
  InitPlan 1
    ->  Bitmap Heap Scan on margin_guard_activity
          Recheck Cond: (campaign_id = $1)
          Filter: (placement_id = $2 AND action = 'pause' AND created_at > ...)
          ->  Bitmap Index Scan on idx_margin_guard_activity_campaign_id
                Index Cond: (campaign_id = $1)
Execution Time: 0.023 ms
```

Uses `idx_margin_guard_activity_campaign_id`; recheck filters placement/action/time on small row sets.

### `list_activity_by_campaign` (Admin API)

```
Limit
  ->  Sort (created_at DESC)
        ->  Bitmap Heap Scan on margin_guard_activity
              ->  Bitmap Index Scan on idx_margin_guard_activity_campaign_id
Execution Time: 0.044 ms
```

### Suggested index (optional hardening)

```sql
CREATE INDEX CONCURRENTLY idx_margin_guard_activity_dedupe
  ON margin_guard_activity (campaign_id, placement_id, action, created_at DESC)
  WHERE action = 'pause';
```

Would turn dedupe `EXISTS` into index-only lookup under high activity volume.

---

## DoD Checklist

- [x] `cmd/margin-guard` worker
- [x] Policy CRUD + activity log
- [x] Notifier integration
- [x] Pro tier gate
- [x] Stale-data guard
- [x] Hot-path zero-alloc filter (verified)
- [x] Postgres EXPLAIN captured
- [x] ASM + branch analysis documented
