# RTB chaos test plan

**Цель:** зафиксировать ожидаемое поведение при некорректных данных, нарушении порядка операций и краевых входах. Тесты **не чинят** прод-код — только документируют pass/fail.

**Область:** `internal/rtb` (auction, budget store, spend chain, catalog integrity, snapshot wire).

---

## A. Мусор и невалидный вход (`BidRequest`)

| ID | Сценарий | Ожидание |
|----|----------|----------|
| A1 | `req == nil` | `NoBidInvalidRequest`, бюджеты не меняются |
| A2 | `MinBid < 0` | `NoBidInvalidRequest` |
| A3 | `DeviceType == 0` при mask-only кампаниях | `NoBidNoCandidates` / empty shard |
| A4 | `CategoryMask == 0` | `NoBidNoCandidates` |
| A5 | `MinBid == MaxInt64` | no-bid или spend fail, **без panic** |
| A6 | `GeoHash` без кампаний в шарде | `NoBidEmptyShard` или `NoBidNoCandidates` |

---

## B. Битый / несогласованный каталог

| ID | Сценарий | Ожидание |
|----|----------|----------|
| B1 | `Count > len(CampaignIDs)` | `NoBidCorruptCatalog`, **без panic** |
| B2 | `Count > 0`, пустой geo-index | `NoBidNoCandidates`, **без panic** |
| B3 | `GeoBucketIdx` указывает за пределы slice | `NoBidCorruptCatalog`, **без panic** |
| B4 | `BudgetIndices[i]` за пределами store | `NoBidCorruptCatalog`, бюджеты не меняются |
| B5 | Отрицательный `Bid` в каталоге | не выигрывает / no-bid, **без panic** |
| B6 | `PacingOpen` = произвольный мусор (не 1/2) | трактуется как open (`normalize` только при sync) |

---

## C. Бюджет и порядок spend (`CheckAndSpendAll`)

| ID | Сценарий | Ожидание |
|----|----------|----------|
| C1 | Campaign budget = 0 | нет победителя |
| C2 | Customer pool < clearing price после прохождения rank | `NoBidSpendFailed`, **rollback campaign** |
| C3 | Daily cap < clearing price на spend | `NoBidSpendFailed`, rollback campaign + customer |
| C4 | `campaignIdx` out of range | `false`, **без panic** |
| C5 | Двойной spend: budget ровно на один clearing | первый win, второй `NoBidNoCandidates` (остаток < bid) |
| C6 | Prefilter по `bid`, spend по `price` (second-price) | price ≤ bid; overspend невозможен |
| C7 | `SetBudget(0)` между rank и spend (race) | `NoBidSpendFailed`, итог ≥ 0 |

---

## D. Приоритет NoBidReason

| ID | Сценарий | Ожидание |
|----|----------|----------|
| D1 | Все кандидаты pacing closed | `NoBidPacingClosed` |
| D2 | Pacing closed + daily blocked (разные кандпании) | `NoBidPacingClosed` (pacing приоритетнее) |
| D3 | Только daily blocked | `NoBidDailyCapExceeded` |

---

## E. Clearing / reserve края

| ID | Сценарий | Ожидание |
|----|----------|----------|
| E1 | `reserve > secondBid` | price = reserve, cap по winner bid |
| E2 | First-price + reserve | price = winner bid (capped reserve) |
| E3 | `CTRPPM = 0` в каталоге (legacy) | normalize → 1.0, аукцион работает |

---

## F. Customer pool

| ID | Сценарий | Ожидание |
|----|----------|----------|
| F1 | Две кампании, один customer, pool на один spend | второй win уменьшает pool |
| F2 | `CustomerID == 0` | customer slot disabled, только campaign budget |
| F3 | Customer exhausted, campaign ok | no-bid, campaign budget не трогается |

---

## G. Snapshot wire (persistence)

| ID | Сценарий | Ожидание |
|----|----------|----------|
| G1 | Пустой файл | error, registry пустой |
| G2 | Неверный magic | error |
| G3 | Version = 999 | `unsupported snapshot version` |
| G4 | Truncated после header | error, **без panic** |
| G5 | Valid v4 round-trip | budgets + P2 поля сохранены |

---

## H. Конкурентность (лёгкий chaos)

| ID | Сценарий | Ожидание |
|----|----------|----------|
| H1 | `UpdateCampaigns(nil)` во время `RunAuction` | **без panic**, budget ≥ 0 |
| H2 | Параллельный drain до 0 | wins bounded, budget не уходит в минус |

---

## Критерии pass/fail

- **Pass:** поведение совпадает с колонкой «Ожидание».
- **Fail:** panic, отрицательный budget, partial debit без rollback, silent overspend.
- Падения **не исправляются** в рамках chaos-прогона — фиксируются в отчёте.

## Файлы тестов

- `internal/rtb/chaos_test.go` — A–H (auction + budget)
- `internal/rtb/chaos_persistence_test.go` — G

---

## Результаты прогона (2026-06-27)

```text
go test -v -count=1 ./internal/rtb/... -run 'TestChaos_'
```

| ID | Тест | Результат | Комментарий |
|----|------|-----------|-------------|
| A1–A6 | Мусорный вход | **PASS** | nil/negative/min/max geo — без panic |
| B1 | count > slices | **PASS** | `NoBidCorruptCatalog` |
| B2 | пустой geo-index | **PASS** | `NoBidNoCandidates` |
| B3 | OOB `GeoBucketIdx` | **PASS** | bounds-check → `NoBidCorruptCatalog` |
| B4 | OOB `BudgetIndices` | **PASS** | `budgetSlotExists` → `NoBidCorruptCatalog` |
| B5 | negative bid | **PASS** | `NoBidNoCandidates` |
| C1 | budget = 0 | **PASS** | |
| C2–C4 | rollback / oob spend | **PASS** | rollback campaign+customer+daily подтверждён |
| C5 | exact budget | **PASS** | первый win price=50, второй `NoBidNoCandidates` |
| C6–C7 | clearing / race | **PASS** | budget ≥ 0 под гонкой |
| D1–D3 | NoBid priority | **PASS** | pacing > daily |
| E1–E3 | clearing edges | **PASS** | |
| F1–F3 | customer pool | **PASS** | |
| H1–H2 | concurrency | **PASS** | rebuild + parallel drain |
| G1–G5 | persistence | **PASS** | corrupt wire отклоняется, v4 round-trip OK |

**Итого: 32 теста — 32 PASS** (после фиксов B3/B4/C5, 2026-06-27).

### Исправления по backlog

1. **B3** — `rankCandidates`: `i < 0 || i >= reg.Count` → `NoBidCorruptCatalog`.
2. **B4** — `budgetSlotExists` перед `LoadBudget` → `NoBidCorruptCatalog` (не silent filter).
3. **C5** — фикстура `budget=100`, второй аукцион: `NoBidNoCandidates` (остаток 50 < bid 100).
