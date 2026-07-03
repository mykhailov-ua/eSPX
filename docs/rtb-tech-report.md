# internal/rtb — техотчёт по оптимизации

**Дата:** 2026-06-27  
**Платформа:** linux/amd64, Go 1.25+, Intel i5-11400H (12 threads)

## Изменения

### 1. Убран избыточный `LoadBudget` перед `CheckAndSpend`

**Было:** двойная проверка бюджета победителя — `LoadBudget(winner) < price`, затем `CheckAndSpend`.

**Стало:** только `CheckAndSpend`, который внутри делает `curr < limit` перед CAS.

**Почему безопасно:** `CheckAndSpend` атомарно отклоняет списание при недостатке средств. Поведение идентично, минус один `atomic.LoadInt64` на каждый успешный аукцион.

`LoadBudget` в цикле фильтрации **оставлен** — нужен для корректного `secondBid` (кампании без бюджета не должны влиять на clearing price).

### 2. Переименование `BidFloor` → `Bid`

| Было | Стало |
|------|-------|
| `CampaignData.BidFloor` | `CampaignData.Bid` |
| `CampaignAuctionRegistry.BidFloors` | `CampaignAuctionRegistry.Bids` |

Поле — **max bid кампании** для ранжирования и second-price, не publisher floor. `req.MinBid` — floor площадки.

Wire-формат snapshot не изменился (бинарный layout `[]int64` тот же).

---

## Тесты

```text
go test -race -count=1 ./internal/rtb/...
ok  espx/internal/rtb  2.560s

go test -count=1 ./internal/rtb/...
ok  espx/internal/rtb  1.522s
```

Все unit-тесты и race-detector — **PASS**, включая `TestRegistry_runAuction_concurrentSpendBoundedByCAS`.

---

## Бенчмарки (`-benchmem`, 10 прогонов, benchstat)

| Benchmark | До (median) | После | B/op | allocs/op |
|-----------|-------------|-------|------|-----------|
| `BenchmarkAuction` (1000 campaigns, spread geo) | ~40.5 ns | **40.24 ns ± 1%** | 0 | 0 |
| `BenchmarkAuction_highDensity` (1000 in one shard) | ~2650 ns | **2658 ns ± 2%** | 0 | 0 |

Вывод: изменение в пределах шума. Hot path остаётся **0 allocs/op**.

---

## Escape analysis (`-gcflags='-m=2'`)

Ключевые факты для `RunAuction`:

| Символ | Результат |
|--------|-----------|
| `req *BidRequest` | **does not escape** |
| `AuctionResult` | возврат по значению, на heap не уходит |
| `LoadBudget` | inlined (cost 32) |
| `CheckAndSpend` | inlined (cost 57) |
| `RunAuction` | **не inline** (cost 387 > budget 80) — ожидаемо для цикла |
| `registry *Registry` | leaking param (receiver) — норма для method value |

Аллокации на cold path: `NewBudgetStore`, `UpdateCampaigns`, `SaveSnapshot` — вне hot loop.

---

## pprof

### CPU — `BenchmarkAuction` (spread, ~29M iter)

| flat% | cum% | Функция |
|-------|------|---------|
| 49% | 51% | `(*Registry).RunAuction` |
| — | 53% | `BenchmarkAuction` |

### CPU — `BenchmarkAuction_highDensity` (1000/shard)

| flat% | cum% | Функция |
|-------|------|---------|
| 62% | 69% | `(*Registry).RunAuction` |
| 8% | 8% | `(*BudgetStore).LoadBudget` |

`LoadBudget` в high-density — ожидаемый вклад (~8% flat): до ~1000 atomic read на аукцион при плотном шарде. Удаление одного read у победителя на этом фоне незаметно.

### Mem profile (setup phase)

Основные аллокации — `UpdateCampaigns` / `appendSlotLocked` при прогреве бенча, не внутри `RunAuction` loop.

---

## Что не меняли (осознанно)

| Замечание | Решение |
|-----------|---------|
| `LoadBudget` в цикле | Нужен для корректного second-price |
| `geoHashes[i]` в шарде | Шард = `geo & 0xF`, полный geo различается |
| Multi-pass фильтрация | Преждевременно при ~2.7 µs worst case |
| Double budget source (Redis vs rtb) | Задача интеграции, не этого PR |

---

## Итог

Два точечных фикса без изменения семантики:

1. **−1 atomic load** на успешный аукцион.
2. **Ясный нейминг** `Bid` вместо путающего `BidFloor`.

Hot path: **0 B/op, 0 allocs/op**, race-clean, ~40 ns (spread) / ~2.7 µs (1000/shard).

### Package layout (`internal/rtb`)

| File | Concern |
|------|---------|
| `rtb.go` | package doc |
| `catalog_types.go` | `CampaignID`, `CustomerID`, `BidRequest`, `AuctionResult` |
| `catalog_shard.go` | `CampaignAuctionRegistry`, `CampaignData` |
| `catalog_registry.go` | `Registry`, `UpdateCampaigns` |
| `catalog_geo_index.go` | per-geo inverted index (cold path) |
| `catalog_pacing.go` | pacing gate constants |
| `auction.go` | `RunAuction` orchestration |
| `auction_rank.go` | candidate ranking |
| `auction_ranking.go` | eCTR score |
| `auction_clearing.go` | clearing mode and price |
| `budget_store.go` | slot allocation, admin API |
| `budget_spend.go` | CAS spend chain |
| `no_bid.go`, `metrics.go`, `errors.go` | outcomes, telemetry, errors |
| `persistence.go` | snapshot wire format |

---

## P0 (2026-06-27)

### Реализовано

1. **Weight tie-break** — при равном `Bid` побеждает больший `Weight`; при равном весе — первый в shard-order.
2. **`NoBidReason`** — `RunAuction` / `RunAuctionEval` возвращают `(AuctionResult, NoBidReason)`.
3. **Метрики** — `ad_rtb_auction_*`, `ad_rtb_budget_spend_rejected_total`; `SetMetricsEnabled(false)` для bench.
4. **Интеграция ads** — `rtb_bridge.go`, `rtb_catalog.go`, `BudgetAuthority` (Redis / RTB / Shadow).

### Файлы

| Пакет | Файлы |
|-------|-------|
| `internal/rtb` | `no_bid.go`, `metrics.go`, `auction.go` |
| `internal/metrics` | RTB collectors |
| `internal/ads` | `rtb_bridge.go`, `rtb_catalog.go`, `rtb_bridge_test.go` |

---

## P1 (2026-06-27)

### Реализовано

| # | Фича | Детали |
|---|------|--------|
| 1 | **eCTR ranking** | `CampaignData.CTRPPM` (fixed-point, `1_000_000` = CTR 1.0). Ранжирование по `bid * CTR / 1e6`. Clearing price — по bid раннер-апа. |
| 2 | **Clearing mode** | `Registry.SetClearingMode`: `ClearingSecondPrice` (default) / `ClearingFirstPrice`. Проброс через `RtbCatalog.SetClearingMode`. |
| 3 | **Reserve price** | `CampaignData.Reserve` — кампания не участвует при `bid < reserve`; clearing поднимается до reserve (cap по bid победителя). |
| 4 | **Geo sharding 64** | `geoShardCount` 16 → **64** (`geo & 0x3F`). Меньше коллизий в шарде. |
| 5 | **Geo inverted index** | Cold path: `buildGeoIndex` → `GeoBucketHash/Start/Idx`. Hot path: binary search + итерация только кандидатов нужного geo. |

### Snapshot wire format v3

- Версия **3**: после `Bids[]` пишутся `CTRPPM[]` и `Reserves[]`.
- **v2** загружается: 16 шардов, CTR=1.0, Reserve=0, индекс строится при load.
- **v3** пишется всегда: 64 шарда.

### Новые файлы

| Файл | Назначение |
|------|------------|
| `auction_clearing.go` | `ClearingMode`, clearing price |
| `auction_ranking.go` | `effectiveScore`, `CTRPPMUnit` |
| `catalog_geo_index.go` | `buildGeoIndex`, `geoRange` |
| `auction_clearing_test.go`, `catalog_geo_index_test.go` | clearing, geo index |

### Бенчмарки P1 (`SetMetricsEnabled(false)`)

| Benchmark | P0 | P1 | allocs |
|-----------|-----|-----|--------|
| `BenchmarkAuction` (spread) | ~100 ns | **~11 ns** | 0 |
| `BenchmarkAuction_highDensity` | ~3.0 µs | **~4.2 µs** | 0 |

Geo-index сильно ускоряет spread-case (итерация по bucket, не весь шард). High-density чуть медленнее из-за eCTR/reserve/index overhead — ожидаемо.

### Тесты

```text
go test -race -count=1 ./internal/rtb/...
ok  espx/internal/rtb  2.786s
```

Новые: `TestAuction_eCTR_ranking`, `TestAuction_reserve_floor`, `TestAuction_firstPrice`, `TestAuction_geoIndex_skipsOtherGeoInShard`, `TestBuildGeoIndex_groupsByGeo`.

### ads bridge

`RtbCampaignInput` расширен: `CTRPPM`, `ReserveMicro`.

### Не вошло в P1 (P2+)

- Device/category inverted index (только geo)
- Консолидация с `HybridBalancer` (отдельный PR)
- Multi-pass SoA фильтрация

---

## P2 (2026-06-27)

### Реализовано

| # | Фича | Детали |
|---|------|--------|
| 1 | **Daily budget snapshot** | `CampaignData.DailyBudget` в каталоге; hot path — `dailySpent[]` (инкремент), rollover по UTC-дню (`maybeRollDaily`). `DailyBudget=0` — без лимита. |
| 2 | **Pacing gate** | `PacingOpen` / `PacingClosed` в каталоге; фильтр до ранжирования. Значение приходит из management, **не вычисляется** на bid path. |
| 3 | **Customer budget** | `CustomerID` → общий пул `customerBudgets[]`; несколько кампаний одного рекламодателя делят один CAS-слот. |
| 4 | **Атомарный spend** | `CheckAndSpendAll`: campaign → customer → daily с rollback при отказе. |
| 5 | **Snapshot v4** | После `Reserves[]`: `DailyBudgets[]`, `PacingOpen[]`, `CustomerBudgetIndices[]`. v2/v3 грузятся: pacing=open, customer=disabled. |
| 6 | **ads bridge** | `RtbCampaignInput`: daily/pacing/customer; `rtb_hybrid_bridge.go` для HybridBalancer metadata. |

### Новые `NoBidReason`

- `NoBidPacingClosed` — все кандидаты в geo-bucket с закрытым pacing.
- `NoBidDailyCapExceeded` — daily headroom < bid у всех оставшихся кандидатов.

### Новые файлы

| Файл | Назначение |
|------|------------|
| `pacing.go` | Константы pacing gate |
| `spend.go` | `CheckAndSpendAll`, daily rollover, customer rollback |
| `customer_id.go` | `CustomerID uint64` |
| `auction_p2_test.go` | pacing, daily cap, customer pool, rollback |
| `internal/ads/rtb_hybrid_bridge.go` | Hybrid → RTB catalog |

---

## Аргументация решений (P2)

### 1. Pacing — snapshot, не hot-path вычисление

**Проблема:** в production pacing живёт в Redis Lua (`unified-filter.lua`) и management. Дублировать формулу (even delivery, burst allowance) внутри `RunAuction` — два источника правды и drift.

**Решение:** `PacingOpen uint8` в каталоге — management публикует бинарный gate при sync. Hot path — одна проверка `== PacingClosed`, 0 alloc.

**Почему не `PacingClosed = 0`:** zero-value в Go = unset. Legacy-кампании без поля должны быть **open**. Поэтому `PacingOpen=1`, `PacingClosed=2`; `normalizePacingOpen(0)` → open.

**Trade-off:** между sync-интервалами pacing может отставать от Redis на shadow path. Для cutover приемлемо: management — authority, RTB — зеркало с тем же gate.

### 2. Daily cap — отдельный `dailySpent[]`, не «остаток как budget»

**Проблема:** campaign budget — **декремент** остатка (`curr >= price` → `curr -= price`). Daily cap — **инкремент** потраченного за день (`spent + price <= limit`).

**Решение:** параллельный `dailySpent` с тем же slot index, что и campaign budget. Prefilter: `dailyLimit - spent >= bid`. Spend: `checkAndAddDailySpend` (CAS add). Rollover: обнуление slice при смене UTC-дня под mutex (cold, раз в сутки).

**Почему не Redis на RTB path:** RTB ещё не в production hot path; in-memory daily — быстрый shadow/RTB authority без RTT. При cutover management пересинхронизирует daily snapshot; drift за день bounded sync interval.

**Не персистим dailySpent в snapshot v4:** после рестарта daily сбрасывается — как при rollover. Management при boot отдаёт актуальный spent из Postgres/Redis. Campaign budget персистится (как раньше).

### 3. Customer budget — shared pool, rollback order

**Проблема:** несколько кампаний одного customer могут суммарно превысить лимит, если списывать только per-campaign budget.

**Решение:** `CustomerID` → `customerBudgets[idx]`; `GetOrAllocateCustomerSlot` на cold path при `UpdateCampaigns`. Prefilter: `LoadCustomerBudget >= bid`. Spend: после campaign CAS — customer CAS; при fail — rollback campaign.

**Порядок spend:** campaign → customer → daily. При fail на daily — rollback customer + campaign. Это минимизирует окно, где customer списан без campaign (невозможно — customer после campaign).

**`CustomerID == 0`:** invalid, `invalidCustomerBudgetIdx = ^uint32(0)` — кампания без customer pool.

### 4. No-bid reason приоритет pacing > daily

Если winner не найден и были pacing-blocked кандидаты → `NoBidPacingClosed`, иначе если daily-blocked → `NoBidDailyCapExceeded`, иначе `NoBidNoCandidates`. Pacing — жёсткий gate от management; daily — мягче для ops (видно в метриках отдельно).

### 5. Что сознательно не вошло в P2

| Идея | Почему отложено |
|------|-----------------|
| Device/category inverted index | Geo-index дал основной выигрыш; device/category — bitmask, дешёвый filter внутри bucket |
| Персист daily/customer в snapshot | Ephemeral; authority — management/Redis; меньше wire complexity |
| Вычисление pacing на bid path | Дублирование Lua, drift, alloc/time на hot path |
| Multi-pass SoA | Worst case ~6 µs при 1000/shard — ещё в бюджете SLA |

### Бенчмарки P2 (`SetMetricsEnabled(false)`)

| Benchmark | P1 | P2 | allocs |
|-----------|-----|-----|--------|
| `BenchmarkAuction` (spread) | ~11 ns | **~14 ns** | 0 |
| `BenchmarkAuction_highDensity` | ~4.2 µs | **~6.1 µs** | 0 |

Overhead: +2–3 проверки на кандидата (pacing, daily headroom, customer budget) + CAS-цепочка на spend. Spread-case +3 ns — в пределах шума. High-density +~2 µs — ожидаемо при 1000 итераций bucket.

### Тесты

```text
go test -race -count=1 ./internal/rtb/...
ok  espx/internal/rtb  2.633s

go test -count=1 ./internal/ads/... -run 'Rtb|Pacing|BuildRtb'
ok  espx/internal/ads
```

Новые: `TestAuction_pacingClosed`, `TestAuction_dailyCap_*`, `TestAuction_customerBudget_sharedPool`, `TestCheckAndSpendAll_rollsBackCustomerOnDailyFail`, hybrid bridge tests.

### Итог P2

Три слоя бюджетного контроля (campaign / customer / daily) + pacing gate из management, без аллокаций на hot path. Snapshot v4 расширяет каталог; ephemeral spend state остаётся in-memory до полного cutover.

---

## Tracker integration (2026-06-27)

### Hot path

`cmd/tracker/main.go` при `RTB_MODE≠off`:
1. Cold: `StartRtbCatalogSync` — rebuild каталога на `REGISTRY_SYNC_INTERVAL_MS`
2. Hot: `processTrack` → `applyRtbAuction` **до** `FilterEngine.Check`
3. `RTB_MODE=live` — `evt.CampaignID` = winner из `RunAuction`
4. `RTB_MODE=shadow` — `RunAuctionEval`, client `campaign_id` не меняется

### Budget authority

| RTB_MODE | RTB_BUDGET_AUTHORITY | Spend |
|----------|----------------------|-------|
| off | — | Lua only |
| shadow | redis | Lua; RTB eval без debit |
| live | redis | Lua budget; RTB eval выбирает winner |
| live | rtb | `CheckAndSpendAll`; Lua `skip_budget` (dedup/stream/fcap сохранены) |

### Env

`RTB_MODE`, `RTB_BUDGET_AUTHORITY`, `RTB_CLEARING_MODE`, `RTB_SNAPSHOT_PATH` — см. `.env.example`.

### SLA

Правила latency (100 ms ceiling, p95/p99) — `.cursorrules` секция **Tracker latency SLA**.

---

## Phase 2 — Budget authority (2026-06-27)

### Budget mirror + reconcile

- `SyncRTBBudgetState` on each catalog sync: campaign/customer/daily from registry; Redis keys when `RTB_BUDGET_AUTHORITY=rtb`
- `RtbBudgetReconcileWorker` — cold-path sample Redis `budget:campaign:*` vs `rtb.BudgetStore`
- Metrics: `ad_rtb_budget_reconcile_divergence_micro`, `ad_rtb_budget_reconcile_high`, `ad_rtb_budget_reconcile_samples_total`
- Alert: `RtbBudgetReconcileHigh`

### No-bid → filter reject mapping

| NoBidReason | filterRejectKind |
|-------------|------------------|
| PacingClosed | pacing |
| DailyCap / SpendFailed | budget |
| NoCandidates / EmptyShard | bid_floor |
| CorruptCatalog / InvalidRequest | infra |

### Lua skip_budget

`RTB_BUDGET_AUTHORITY=rtb` + `SetSkipBudgetDebit(true)` — dedup/fcap/stream unchanged; Redis budget not debited.

### Env

`RTB_RECONCILE_INTERVAL_MS`, `RTB_BUDGET_DIVERGENCE_THRESHOLD_MICRO`, `RTB_RECONCILE_SAMPLE_SIZE`

---

## Phase 1 — Catalog quality + geo dedup (2026-06-27)

### Hybrid catalog sync

- `HybridBalancer` wired in `cmd/tracker` when `RTB_MODE≠off`
- `SyncRtbCatalog` → `BuildCampaignMetaList` + `BuildRtbCatalogRowsFromHybrid`
- Per-customer pool: sum of `RemainingBudgetMicro` across active campaigns
- Env: `RTB_HYBRID_MAX_RPS_PER_NODE` (default 5000)

### Geo dedup

- `ensureIngestGeo` at `processTrack` entry; cached on `domain.Event` (`GeoCountry`, `GeoHash`)
- `GeoFilter` reuses cache — one MaxMind lookup per request when RTB+geo both enabled
- `category_mask` parsed from payload in `buildRtbTargeting`

### Tests

- `track_rtb_test.go` — `processTrack` + RTB live/shadow
- `rtb_sync_test.go` — customer pools + hybrid sync
- `event_geo_test.go` — geo cache + GeoFilter dedup

---

## Phase 5 — Targeting inverted index (staging only, 2026-06-27)

### Device + category index

Cold path `buildTargetingIndex` → sorted `(geoHash, deviceBit, categoryBit)` buckets → `TargetBucketIdx`.

Hot path when `RTB_TARGETING_INDEX=true`:

1. `candidateRange` → `targetingRange` (narrow bucket)
2. Fallback to `geoRange` on miss
3. `rankCandidates` iterates bucket slice (0 alloc)

Prod default: `RTB_TARGETING_INDEX=false` (`.env.rtb-prod` stub). Staging: `true` in `.env.rtb-staging`.

### Files

| File | Role |
|------|------|
| `internal/rtb/catalog_targeting_index.go` | build + lookup |
| `catalog_targeting_index_test.go` | index + auction tests |
| `.env.rtb-staging` | `RTB_TARGETING_INDEX=true` |
| `.env.rtb-prod` | stub `RTB_MODE=off`, index false |
| `deploy/rtb-prod/` | prod no-op docs |

### Wire

`cmd/tracker`: `SetTargetingIndexEnabled` **before** `StartRtbCatalogSync`.

---

## Phase 4 — Live cutover prep (2026-06-27)

### E2E: live + RTB budget authority

`tests/e2e_rtb_test.go` — `TestE2E_RtbLiveBudgetAuthority`:

- Postgres + Redis (testcontainers)
- `RTB_MODE=live`, `RTB_BUDGET_AUTHORITY=rtb`, `skip_budget` Lua path
- Client `campaign_id` ≠ winner; RTB replaces before filters
- Redis `budget:campaign:*` unchanged; `rtb.BudgetStore` debits clearing price
- Stream → Postgres `campaign_stats` for RTB-selected campaign

Requires `bid_micro` in payload (second-price clearing with single bidder otherwise charges floor 0).

### Staging template

| Artifact | Purpose |
|----------|---------|
| `.env.rtb-staging` | Env fragment for live + rtb authority soak |
| `deploy/rtb-staging/docker-compose.override.yml` | Tracker overlay + `rtb_snapshot` volume |
| `deploy/monitoring/grafana/.../rtb.json` | Grafana dashboard `uid=rtb-cutover` |

### Grafana panels

- `ad_rtb_budget_reconcile_high` (stat gate)
- Shadow mismatch rate vs `ad_filter_throughput_total`
- Wins vs `ad_rtb_budget_spend_rejected_total`
- Auction p50/p99, no-bid by reason
- Reconcile divergence histogram, shadow no-bid / price delta

---

## Phase 3 — Hot path refactor (2026-06-27)

### UnifiedFilter.Check

- Убран `defer` на hot path: `unifiedCheckScratch.acquire/release` + `runUnifiedLua`
- `evalScript` → прямой `rdb.EvalSha` с кэшированным `scriptHash` (без mutex `Script` на hot path)
- Pre-bound `luaNoScriptCounters` — без `strconv.Itoa` на NOSCRIPT path
- **Остаётся 1 alloc/op** от go-redis variadic `args...` в `EvalSha` (~350 ns/op mock bench)

### RTB auction metrics

- `auctionStartMono()` вместо `time.Now()` при `metricsEnabled`
- Duration histogram: downsample 1/128 (`rtbAuctionMetricsSampleMask`), как Lua latency
- Counters (win/no-bid) — полная частота; только histogram sampled

### processTrack / applyRtbAuction

- `applyRtbAuction(proc trackProcessor, ...)` by-value — без escape `&p` на stack processor

### React split

- `parseTrackIngest`, `fillTrackEvent`, `deliverGnetTrack` в `track_ingest_gnet.go`
- `React` — routing + вызов helpers (лучше inlining на accept path)

### FilterEngine deadline

- Документировано: `timeout > 0` → 2 heap allocs (`context.WithValue` + fraud acc); tracker использует `FilterEngine(0)`

---

## Phase 0 — Observability (2026-06-27)

| Metric | Назначение |
|--------|------------|
| `ad_rtb_shadow_winner_mismatch_total` | RTB shadow winner ≠ client `campaign_id` |
| `ad_rtb_shadow_no_bid_total{reason}` | no-bid при client campaign (по `NoBidReason`) |
| `ad_rtb_shadow_price_delta_micro` | \|clearing − payload bid\| на shadow win (sample 1/128) |

Запись в `applyRtbAuction` при `RTB_MODE=shadow`; 0 allocs на match-path (без geo lookup).

### RTB metrics default

`metricsEnabled` в `internal/rtb` — **false** по умолчанию (бенчи без `time.Now`). Tracker включает `rtb.SetMetricsEnabled(true)` при `RTB_MODE≠off`.

### Cutover runbook

`docs/rtb-cutover.md` — phased rollout shadow → live+redis → live+rtb.

### CI

`make test-alloc-gate`: shadow zero-alloc + `BenchmarkAuction` 0 allocs. `perf-gate-bench.sh` включает `BenchmarkAuction`.
