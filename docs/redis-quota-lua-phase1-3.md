# Phase 1.3 — unified-filter.lua quota keys

Distributed Quotas Redis layer (`REDIS.md` §1.3). Complements `docs/redis-quota-lifecycle.md` (PG reserve path).

## New keys (campaign shard-local)

| Key | Type | Purpose |
| :--- | :--- | :--- |
| `budget:quota:{campaign_id}` | string (int64) | Local spendable quota (micro-units) |
| `budget:refill_lock:{campaign_id}` | string | NX lock, TTL 10 s — thundering herd guard |
| `budget:refill_needed` | SET | Campaign IDs awaiting QuotaManager refill (Phase 1.4) |

Legacy `budget:campaign:{id}` remains for dual-read transition and SyncWorker.

## Lua behaviour

| `QUOTA_MODE` | Spend source | Refill trigger |
| :--- | :--- | :--- |
| `off` | `budget:campaign` only (unchanged) | disabled |
| `shadow` / `live` | `budget:quota` if present, else `budget:campaign` | when quota debit leaves `remaining < chunk × threshold%` |

Return codes unchanged: `3` = budget/quota exhausted, `-1` = both keys missing.

ARGV additions: `quota_enabled`, `chunk_size`, `refill_threshold_pct` (defaults 0 / 0 / 20).

---

## Network overhead

Still **one EvalSha round trip** per `/track` filter — no extra RTT vs legacy script.

| Component | Legacy (12 KEYS) | Quota mode (15 KEYS) | Delta |
| :--- | :--- | :--- | :--- |
| Redis command | `EVALSHA` × 1 | `EVALSHA` × 1 | 0 RTT |
| KEY slots | 12 | 15 | +3 names (~90 B wire) |
| ARGV slots | 24 | 27 | +3 scalars (~24 B) |
| Reply | int64 status | int64 status | 0 |
| Refill path (cold) | — | +`SET NX` +`SADD` inside same script | 0 RTT (same EvalSha) |

**Tracker ↔ Redis payload estimate:** +~120 bytes/request on quota mode (~2–4% vs typical 3–4 KB EvalSha with stream fields). Dominated by `XADD` stream payload, not key names.

**QuotaManager ↔ Redis / PG:** not on hot path (Phase 1.4).

---

## Blocking operations in Lua

Redis runs the script **atomically** on the shard master — while executing, other commands to that shard queue behind it. There is **no** cooperative yield inside the script.

### Operations used (all O(1) per key, no scans)

| Call | Blocking? | Notes |
| :--- | :--- | :--- |
| `MGET` | Holds engine | 4–5 keys, single round inside script |
| `GET` (imp_ts) | Same | Only on click + TTC enabled |
| `INCR` / `INCRBY` | Same | Rate, fcap, spend, sync |
| `SET NX EX` | Same | Dedup, refill lock |
| `SADD` | Same | Dirty sets, refill queue |
| `XADD` | Same | Stream enqueue (~largest CPU slice) |
| `EXPIRE` | Same | TTL on counters |

**Not used (would block hot path badly):** `KEYS`, `SCAN`, `BLPOP`, `WAIT`, `RANDOMKEY`, cross-shard calls.

**Quota mode delta vs legacy:** +1 key in `MGET` (no extra internal RTT). Refill branch adds at most `SET NX` + `SADD` when quota low — still same single script execution, no second client round trip.

**Hot path Go side:** `Check` → `evalScript` is one async Redis call; gnet worker waits on filter deadline, not on PG or QuotaManager.

---

## Performance measurements

Run locally (testcontainers Redis):

```bash
go test -count=1 -run TestUnifiedFilter_QuotaMode_LatencyProfile ./internal/ads/
go test -bench=BenchmarkUnifiedFilter_Check_QuotaMode -benchmem -run='^$' ./internal/ads/
go test -bench=BenchmarkUnifiedFilter_Check_RealRedis -benchmem -run='^$' ./internal/ads/  # legacy compare
```

Reference (linux amd64, local Redis container):

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkUnifiedFilter_Check_RealRedis` (legacy) | ~77k | 1283 | 27 |
| `BenchmarkUnifiedFilter_Check_QuotaMode` | ~82k | 1311 | 29 |
| Delta | ~+6% | +28 B | +2 |

Quota mode adds **negligible Go-side overhead** (2 extra key buffers from pool, 3 ARGV slots). Latency delta vs legacy is within local Redis noise; production SLA still bound by 1× EvalSha RTT.

---

## Config

```env
QUOTA_MODE=off          # off | shadow | live
QUOTA_CHUNK_SIZE=5000000
QUOTA_REFILL_THRESHOLD_PCT=20
```

`cmd/tracker` → `UnifiedFilter.SetQuotaConfig`.

## Script preload (1.3.5)

`PreloadScripts` sets `ad_redis_lua_script_loaded{shard}=1` per shard at startup. Alert if 0 after deploy (see `docs/development.md`).

## Tests

| Test | Covers |
| :--- | :--- |
| `TestUnifiedFilter_quotaDebit` | spend from `budget:quota` |
| `TestUnifiedFilter_quotaDualRead_legacyFallback` | no quota → legacy key |
| `TestUnifiedFilter_quotaExhausted_returns3` | return code 3 |
| `TestUnifiedFilter_quotaRefill_thunderingHerd` | 64 parallel → ≤1 `SADD` |
| `TestUnifiedFilter_quotaOff_legacyPathUnchanged` | `QUOTA_MODE=off` |
| `TestUnifiedFilter_QuotaMode_LatencyProfile` | p50/p99 sanity |

## Code map

| File | Role |
| :--- | :--- |
| `internal/ads/unified-filter.lua` | Quota debit + refill trigger |
| `internal/ads/unified_filter.go` | KEYS[13–15], ARGV[25–27] |
| `internal/config/env.go` | `QUOTA_*` env |
| `internal/ads/unified_quota_test.go` | Integration + bench |
