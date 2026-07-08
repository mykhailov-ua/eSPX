# Hot-path baseline: error handling (R8.3) and benchmarks

Date: 2026-07-07 (updated after alloc fixes)  
Machine: linux/amd64, Intel i5-11400H @ 2.70GHz

Raw benchmark output (5 runs each):

- [hot_path_baseline_ads_raw.txt](./hot_path_baseline_ads_raw.txt)
- [hot_path_baseline_rtb_raw.txt](./hot_path_baseline_rtb_raw.txt)

---

## Alloc fixes (2026-07-07)

| # | Issue | Fix | Before | After |
|---|-------|-----|--------|-------|
| 1 | `IPRateLimiter_Check` | Lua `INCR+PEXPIRE` via pooled `Process`, fixed `wire [5]any` | 205 ns, 368 B, **5 allocs** | 67 ns, 16 B, **1 alloc** |
| 2 | `infra503` E2E | `*net.OpError` sentinel in bench; `*net.OpError` fast path in `IsNetworkOrSystemError` | 534 ns, 32 B, **2 allocs** | 698 ns, 8 B, **1 alloc** |
| 3 | `ExtraRepeated` | `appendReuseBytes` + `scripts/codegen/patch_vtproto_hotpath` (runs in `make proto`) | 387 ns, 16 B, **4 allocs** | 339 ns, 0 B, **0 allocs** |
| 4 | `GeoFilter_lookupError` | Package sentinel `errGeoLookupFailed`; `ErrInvalidIP` in geo | 58 ns, 16 B, **1 alloc** | 36 ns, 0 B, **0 allocs** |
| 5 | `Auction_highDensity` | Cold-path bucket sort by score; early exit in `rankCandidates` | 6,666 ns, 0 allocs | 126 ns, 0 allocs (~53x) |

Remaining 1 alloc on infra503 bench and IPRateLimiter mock: likely `evalCmdPool` / test harness; not on happy accept path.

### Files touched

- `internal/ads/ip-rate-limit.lua`, `ip_rate_limit.go` - Lua rate limiter
- `internal/ads/redis_eval_pooled.go` - single-key eval helpers
- `internal/ads/pb/unmarshal_helpers.go` - `appendReuseBytes`
- `scripts/codegen/patch_vtproto_hotpath/` - post-buf patch wired in `gen.sh`
- `internal/ads/geo.go` - `ErrInvalidIP`, `ErrGeoProviderClosed`
- `internal/ads/unified_filter.go` - reuse `evt.GeoCountry` in bid floor
- `internal/database/redis_breaker.go` - `*net.OpError` before string fallback
- `internal/rtb/catalog_bucket_sort.go`, `auction_rank.go`, `catalog_registry.go`, `persistence.go`

---

## Benchmark summary (median of 5 runs, post-fix)

```bash
go test ./internal/ads/... \
  -bench='Benchmark(HotPath_|GeoFilter|IPRateLimiter|UnifiedFilter|RedisBudget|AdsPacketHandlerProto|FraudFilter|DuplicateEvent|FilterEngine_Check)' \
  -benchmem -count=5 -run='^$'

go test ./internal/rtb/... \
  -bench='BenchmarkAuction' \
  -benchmem -count=5 -run='^$'
```

### E2E `/track` (gnet proto handler)

| Scenario | Benchmark | ns/op | B/op | allocs/op |
|----------|-----------|------:|-----:|----------:|
| **Happy** accept | `HotPath_AdsPacketHandlerProto_accept` | 160 | 0 | 0 |
| **Happy** no extra | `AdsPacketHandlerProto_NoExtra` | 239 | 0 | 0 |
| **Happy** extra bytes | `AdsPacketHandlerProto_ExtraBytes` | 269 | 0 | 0 |
| **Worst** reject 404 | `HotPath_AdsPacketHandlerProto_reject404` | 323 | 0 | 0 |
| **Worst** infra 503 | `HotPath_AdsPacketHandlerProto_infra503` | 698 | 8 | 1 |
| **Worst** repeated extra | `AdsPacketHandlerProto_ExtraRepeated` | 339 | 0 | 0 |

### Filter components

| Scenario | Benchmark | ns/op | B/op | allocs/op |
|----------|-----------|------:|-----:|----------:|
| **Happy** engine no timeout | `FilterEngine_Check_noTimeout` | 24 | 0 | 0 |
| **Happy** unified Lua (mock) | `UnifiedFilter_Check` | 399 | 0 | 0 |
| **Worst** geo lookup error | `GeoFilter_lookupError` | 36 | 0 | 0 |
| **Worst** IP rate limiter | `IPRateLimiter_Check` | 67 | 16 | 1 |

### RTB auction

| Scenario | Benchmark | ns/op | allocs/op | vs happy |
|----------|-----------|------:|----------:|---------:|
| **Happy** sparse | `BenchmarkAuction` | 15 | 0 | 1x |
| **Worst** high density | `BenchmarkAuction_highDensity` | 126 | 0 | ~8x |

---

## Hot-path error handling (R8.3)

See prior sections in git history. Core flow: `FilterEngine.Check` -> `classifyFilterErr` -> `trackOutcome` / pre-built `filterRejectSpecs`.
