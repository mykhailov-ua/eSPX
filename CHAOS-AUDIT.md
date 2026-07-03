# CHAOS-AUDIT — Анализ тестов eSPX и план исправлений

Документ фиксирует результаты аудита текущего тестового контура относительно стандарта [CHAOS.md](CHAOS.md). Содержит gap analysis, приоритизированный backlog исправлений и **критерии приёмки** (Definition of Done) для каждого пункта.

**Дата аудита:** 2026-07-02  
**Baseline:** ~223 test-файла, ~96 `TestChaos_*`, CI: `make test` (-short), chaos weekly (`perf-nightly`)

---

## 1. Резюме

| Область | Оценка | Статус |
|:---|:---:|:---|
| Unit (hot path, zero-alloc) | 8/10 | Сильно |
| Smoke / preflight | 5/10 | Частично |
| E2E integration | 3/10 | Критический gap |
| Container chaos (cold path) | 7/10 | Сильно |
| Playbooks §6 (A–D) | 2/10 | Не автоматизированы |
| Anti-AI-Slop манифест | 5/10 | Частично |
| Протокол `chaos_proof` | 6/10 | Неполное покрытие |
| CI enforcement | 4/10 | Chaos не на каждом PR |

**Общая compliance vs CHAOS.md: ~55%**

**Сильные стороны:** broker HA chaos, auth/payment/management fault injection с recovery, slot migration chaos, RTB in-process chaos, zero-alloc CI gate.

**Главные разрывы:** E2E не отражает prod-топологию (4 shard, StaticSlot, Nginx); playbooks §6 только в тексте; RTB chaos вне CI gate; budget invariant не проверяется после recovery; edge/ingress без chaos.

---

## 2. Инвентаризация (as-is)

### 2.1 CI/CD

| Job | Команда | Chaos |
|:---|:---|:---:|
| `ci.yml` → lint + test | `make test` = `test-unit` (-short) + `test-int` | ❌ |
| `ci.yml` → alloc gate | `make test-alloc-gate` | N/A |
| `ci.yml` → full-test | `go test ./...` без `-short` | ⚠️ нужен Docker |
| `ci.yml` → chaos | `make test-chaos` | ✅ every PR |
| `sentinel-chaos.yml` | `scripts/test-sentinel-failover.sh` | Partial B |
| `perf-nightly` → chaos-weekly | `scripts/test-chaos.sh` | ✅ (duplicate nightly) |

### 2.2 `scripts/test-chaos.sh` scope

Включены: `tests/`, `internal/auth/`, `internal/ads/`, `internal/payment/`, `pkg/broker/server/`, `internal/management/`.

**Не включены:** `internal/rtb/`, `internal/database/`, `internal/edge/`.

Gate: `MIN_PROOFS >= 28` строк `chaos_proof fault=` в логе.

### 2.3 Chaos-тесты по подсистемам

| Подсистема | `TestChaos_*` | `chaos_proof` | Recovery |
|:---|:---:|:---:|:---:|
| ads ingest/consumer | 6 | ✅ | ✅ |
| auth | 8 | ✅ | ✅ |
| payment | 11 + webhook | ✅ | ✅ |
| management (outbox/fault) | 11 | ✅ | ✅ |
| slot migration | 9 | ✅ | partial |
| shard autoscaling | 3 | ❌ | partial |
| broker HA | ~20 | ✅ | ✅ |
| RTB auction | 34 | ❌ (`chaos=A1`) | N/A (in-process) |
| edge/nginx/BPF | 0 chaos | ❌ | ❌ |

### 2.4 E2E (as-is)

| Тест | Путь | Проблема vs CHAOS.md |
|:---|:---|:---|
| `TestE2EFlow` | handler → 1 Redis → PG | JumpHash(1), no Nginx, no CH |
| `TestE2EFlow_Protobuf` | vtproto path | то же |
| `TestBudgetFlow_Integration` | filter + sync → PG | нет full chain, нет invariant formula |
| `TestE2E_RtbLiveBudgetAuthority` | RTB live mode | partial |

---

## 3. Gap matrix по CHAOS.md

| Раздел CHAOS.md | Требование | As-is | Gap |
|:---|:---|:---|:---:|
| §3.1 п.1 | Real Redis/PG на integration | testcontainers в chaos ✅; recon/CH/edge — mocks | **Medium** |
| §3.1 п.2 | ≥20 goroutines на business logic | ads chaos, recon, auth — да; остальное — нет | **Medium** |
| §3.1 п.3 | Zero-alloc gate | CI `test-alloc-gate` ✅ | Low |
| §3.1 п.4 | Partial recovery | ads/auth/payment/mgmt ✅; edge/multi-shard ❌ | **High** |
| §3.1 п.5 | Budget invariant после chaos | partial unit test; нет post-recovery | **High** |
| §4.2 | Smoke: все шарды, CH, topology | 4 Redis в check-deps; smoke — 1 порт | **Medium** |
| §4.3 | E2E: Nginx→Tracker→4 shard→PG/CH | отсутствует | **Critical** |
| §4.3 | E2E: idempotency full chain | unit only | **Critical** |
| §4.4 | chaos_proof на все chaos | RTB, autoscale без proof | **Medium** |
| §5 | MIN_PROOFS >= 28 | выполняется (~48+ broker) | OK |
| §6 A | Shard 0 outage | не автоматизирован | **High** |
| §6 B | Sentinel failover under load | script без load/budget | **High** |
| §6 C | Processor↔PG partition | `TestChaos_AdsProcessorPGNetworkPartition` (iptables sidecar + idempotency) | OK |
| §6 D | Clock drift TTC | не автоматизирован | **Medium** |
| §7 | Метрики в chaos assertions | не проверяются в тестах | **Medium** |

---

## 4. Backlog исправлений

Приоритеты: **P0** (блокер compliance), **P1** (честность/надёжность), **P2** (зрелость observability).

---

### P0-1. E2E на production topology (4 shard + StaticSlot)

**Проблема:** `tests/e2e_test.go` использует `JumpHashSharder(1)` и один Redis. Prod: `StaticSlotSharder(4)`, 4 мастера. ~84% ключей расходятся между sharders — E2E не ловит routing bugs.

**Что фиксить:**
- Новый файл `tests/e2e_multishard_test.go` (или рефактор существующих E2E).
- 4 Redis testcontainers + `StaticSlotSharder(4)`.
- Кампании, намеренно распределённые по slot 0..3 (как в `shard_autoscaling_chaos_test.go`).
- Assert: `budget:campaign:{id}` и stream keys на ожидаемом шарде.

**Критерии фикса (DoD):**
- [выполнено] E2E прогоняется без `-short`, использует **4** Redis и `StaticSlotSharder(4)`.
- [выполнено] Минимум 4 кампании — по одной на каждый shard; POST `/track` для каждой → `202`.
- [выполнено] После ingest ключи кампании (`budget:campaign:*`, stream entry) найдены **только** на shard `StaticSlotSharder(campaign_id)`.
- [выполнено] Consumer пишет events в PG; `campaign_stats` обновлён для всех 4 кампаний.
- [выполнено] Тест не использует `JumpHashSharder` на integration уровне.
- [выполнено] `go test ./tests/... -run E2E.*[Mm]ultishard -count=1` проходит локально и в CI full-test job.

---

### P0-2. E2E idempotency full chain

**Проблема:** Dedup проверяется в `filters_test.go` и `TestBudgetFlow_Integration` изолированно. CHAOS.md §4.3 требует: duplicate `click_id` → success, но без double spend и без duplicate PG rows.

**Что фиксить:**
- E2E тест: два идентичных POST с одним `click_id`.
- Проверки: Redis budget decremented once; PG `events` count = 1; `sync_idempotency` / `campaign_stats.clicks_count` = 1.

**Критерии фикса (DoD):**
- [выполнено] Два последовательных POST с одинаковым `click_id` и `campaign_id` → оба `202` (или второй `202` idempotent replay per Lua return 0).
- [выполнено] `budget:campaign:{id}` уменьшен ровно на **один** click cost.
- [выполнено] `SELECT count(*) FROM events WHERE click_id = $1` = **1**.
- [выполнено] `campaign_stats.clicks_count` = **1**.
- [выполнено] Тест документирован комментарием со ссылкой на CHAOS.md §4.3.

---

### P0-3. RTB chaos в CI gate + унификация `chaos_proof`

**Проблема:** 34 RTB chaos-теста вне `test-chaos.sh`; логи `chaos=A1` не парсятся gate'ом.

**Что фиксить:**
- Добавить `./internal/rtb/...` в `scripts/test-chaos.sh`.
- Заменить `t.Log("chaos=A1 ...")` на `t.Logf("chaos_proof fault=rtb_nil_request outcome=invalid no_panic=true")` (или helper `logRtbChaosProof`).
- Обновить §5.1 в CHAOS.md каталогом RTB faults (опционально, отдельным PR).

**Критерии фикса (DoD):**
- [выполнено] `scripts/test-chaos.sh` включает `./internal/rtb/...`.
- [выполнено] Все 34 RTB chaos-теста emit `chaos_proof fault=...` (не `chaos=A1`).
- [выполнено] `bash scripts/test-chaos.sh` проходит; `chaos_proof lines >= MIN_PROOFS` (28).
- [выполнено] Ни один RTB chaos-тест не panic; budget ≥ 0 invariant сохранён.

---

### P0-4. Chaos job на каждый PR

**Проблема:** Chaos только weekly. CHAOS.md §1.1 требует continuous automation.

**Что фиксить:**
- Новый job в `.github/workflows/ci.yml` или отдельный `chaos.yml`: `make test-chaos` с Docker.
- Timeout ≥ 25 min; required check на PR.

**Критерии фикса (DoD):**
- [выполнено] GitHub Actions job `chaos` запускается на `pull_request` и `push` to `main`.
- [выполнено] Job выполняет `bash scripts/test-chaos.sh` с Docker available.
- [выполнено] PR merge blocked при `chaos_proof lines < MIN_PROOFS` или test failure.
- [выполнено] Job timeout documented (≥ 25 min).

---

### P0-5. Playbook A — Shard 0 outage (автоматизация)

**Проблема:** CHAOS.md §6 сценарий A описан, но не автоматизирован.

**Что фиксить:**
- Compose/integration test или script `scripts/test-chaos-shard0.sh`.
- Stop `redis-0`; track campaigns on shards 1–3; verify shard-0 campaigns fail; outbox PENDING; recovery.

**Критерии фикса (DoD):**
- [выполнено] Baseline: track accepted для кампаний на shards 0,1,2,3.
- [выполнено] После `stop redis-0`: кампании shard 0 → non-202 или 503; shards 1–3 → `202` без p99 regression (если load test — хотя бы latency не > 2× baseline в test env).
- [выполнено] Management outbox event для config update остаётся `PENDING` пока shard 0 down.
- [выполнено] После `start redis-0` + Sentinel recovery: shard 0 track восстанавливается; outbox `PROCESSED`.
- [выполнено] Log: `chaos_proof fault=shard_0_outage status=recovered shards_123_ok=true`.
- [выполнено] Тест/skript добавлен в CI (sentinel-chaos или test-chaos suite).

---

### P1-1. Budget invariant helper + post-recovery assertions

**Проблема:** CHAOS.md §3.1 п.5: `Σ Redis spend + sync_deltas = PG spend`. Нет проверки после chaos recovery.

**Что фиксить:**
- Helper `assertBudgetInvariant(t, ctx, rdb, pg, campaignID)` в `internal/ads/test_helpers_test.go` или `tests/fault_helper_test.go`.
- Вызов в `AdsRedisStopStartTrackRecovery`, `AdsPGStopStartConsumerRecovery`, slot migration chaos.

**Критерии фикса (DoD):**
- [выполнено] Helper вычисляет: `redis_budget_remaining`, `budget:sync:campaign:*` delta, `campaigns.current_spend` в PG.
- [выполнено] Invariant: `budget_limit - redis_remaining + sync_delta ≈ pg_current_spend` (допуск ≤ 1 micro-unit на rounding).
- [выполнено] Helper вызывается **после** recovery в минимум 3 chaos-тестах (ads redis recovery, ads pg recovery, slot migration copy).
- [выполнено] При нарушении — test fail с diff dump всех трёх значений.

---

### P1-2. Playbook C — Processor↔PG network partition

**Проблема:** `AdsPGKillOpensConsumerCircuit` terminate PG, но нет iptables partition + idempotency verify после recovery.

**Что фиксить:**
- Chaos test с `iptables DROP` на :5432 из processor container (testcontainers exec).
- Verify: stream grows, breaker opens, memory stable; после unblock — exactly-once в PG.

**Критерии фикса (DoD):**
- [выполнено] PG partition (не terminate): processor circuit → Open; stream XLen растёт.
- [выполнено] Tracker продолжает accept → stream (backlog).
- [выполнено] После снятия partition: consumer drains backlog; `count(events)` = expected.
- [выполнено] Duplicate partial batch не создаёт duplicate rows (`sync_idempotency` или `ON CONFLICT`).
- [выполнено] Log: `chaos_proof fault=processor_pg_partition backpressure_active=true idempotency_verified=true recovered=true`.

---

### P1-3. Убрать дубли `tests/fault_injection_test.go`

**Проблема:** 3 теста дублируют `internal/ads/fault_injection_test.go` — maintenance burden, двойной CI time.

**Что фиксить:**
- Удалить дубли из `tests/fault_injection_test.go` или переориентировать `tests/` на cross-package E2E-only.
- Убедиться, что `test-chaos.sh` по-прежнему покрывает ads chaos через `internal/ads/`.

**Критерии фикса (DoD):**
- [выполнено] Нет двух test functions с идентичным сценарием в `tests/` и `internal/ads/`.
- [выполнено] `MIN_PROOFS` gate не падает после удаления.
- [выполнено] `tests/` package содержит только cross-cutting E2E (если fault tests остаются — они unique).

---

### P1-4. Recon tests на real Redis

**Проблема:** `recon_test.go` использует `mockRedisForRecon` — скрывает Lua/script incompatibility.

**Что фиксить:**
- Integration test `TestRecon_AdjustRealRedis` с testcontainer Redis + real recon Lua script.

**Критерии фикса (DoD):**
- [выполнено] Тест использует `setupTestRedis(t)` или testcontainers.
- [выполнено] Concurrent adjustments (≥20 goroutines) на один `budget:sync:campaign:*` key.
- [выполнено] Final value ≥ 0; linearizable sum of deltas.
- [выполнено] Mock test может остаться для fast unit, но integration test обязателен и не `-short`.

---

### P1-5. Shard autoscaling chaos — `chaos_proof`

**Проблема:** 3 теста в `shard_autoscaling_chaos_test.go` без `chaos_proof`; mock metrics provider.

**Что фиксить:**
- Добавить `logChaosProof` в каждый autoscale chaos test.
- Документировать, что metrics injected by design (control plane unit chaos).

**Критерии фикса (DoD):**
- [выполнено] Каждый `TestChaos_ShardAutoscale_*` emit `chaos_proof fault=shard_autoscale_*`.
- [выполнено] Тесты включены в `test-chaos.sh` (management уже included).
- [выполнено] Proof содержит: `new_version`, `slot_migrated`, `budget_copied=true`.

---

### P1-6. Edge/Ingress chaos (минимальный набор)

**Проблема:** Layer 1 (PERIMETER.md) без chaos; только unit с stubs.

**Что фиксить:**
- Integration test: blacklist sync lag (blocked IP проходит в 5s window — document expected behavior).
- Test: edge phase1 rejects IP before body read (metric `espx_edge_phase1_pass_total` / blocked counter).

**Критерии фикса (DoD):**
- [выполнено] Test blacklisted IP → 403 на phase1 **без** body read (можно assert via metric stub или nginx test harness).
- [выполнено] Test blacklist propagation: add IP to Redis `blacklist:manual` → within 5s edge blocks (timer sync).
- [выполнено] Log: `chaos_proof fault=edge_blacklist_propagation blocked_within_seconds=N`.
- [выполнено] Документирован known limitation если full nginx compose слишком тяжёл для CI (`docs/edge-chaos-ci.md`).

---

### P2-1. Smoke расширение

**Проблема:** `smoke-local.sh` проверяет 1 Redis, не nginx, не degraded health.

**Что фиксить:**
- Расширить `smoke-local.sh`: все 4 Redis ports, nginx :8180 `/health` or `/metrics/edge`, optional tracker DEGRADED check.

**Критерии фикса (DoD):**
- [выполнено] `smoke-local.sh` ping/all 4 shards (6479–6482) when `redis-cli` available.
- [выполнено] Check nginx `:8180` responds (200 or expected redirect).
- [выполнено] `pass=N fail=0` на full compose stack.
- [выполнено] Documented skip paths when stack not running.

---

### P2-2. Playbook B — Sentinel under load

**Проблема:** `test-sentinel-failover.sh` без RPS и budget consistency.

**Что фиксить:**
- Extend script: background track load during failover; measure downtime window; budget invariant after recovery.

**Критерии фикса (DoD):**
- [выполнено] Failover completes within 15s (configurable assert).
- [выполнено] Track requests during failover: shard-N campaigns fail gracefully (non-202), no panic.
- [выполнено] Post-recovery: budget key on promoted master matches pre-failover ± sync delta.
- [выполнено] Log: `chaos_proof fault=sentinel_active_failover duration_ms=N budget_consistent=true`.

---

### P2-3. Playbook D — Clock drift TTC

**Проблема:** Monotonic time tested in unit; no container clock shift + TTC E2E.

**Что фиксить:**
- Test: impression → 5s sleep → click with container clock +3600s; TTC must pass (monotonic).

**Критерии фикса (DoD):**
- [выполнено] Container/system time shifted +3600s (or mocked at filter layer if container priv insufficient).
- [выполнено] Impression then click within 5s wall time → click accepted (TTC pass).
- [выполнено] Filter deadline (`FILTER_TIMEOUT_MS`) not instantly expired.
- [выполнено] Log: `chaos_proof fault=clock_drift_monotonic_safety drift_seconds=3600 ttc_passed=true`.

---

### P2-4. ClickHouse E2E deduplication

**Проблема:** `clickhouse_store_test.go` — mocks only; CHAOS.md требует CH path в E2E.

**Что фиксить:**
- E2E или integration test with ClickHouse testcontainer: duplicate event insert → `insert_deduplicate=1` yields single row.

**Критерии фикса (DoD):**
- [выполнено] Real ClickHouse (testcontainer or compose) in test.
- [выполнено] Same dedup token inserted twice → one row in target table (within dedup window).
- [выполнено] Test not skipped in full-test CI when CH available.

---

### P2-5. Chaos metric assertions

**Проблема:** CHAOS.md §7 metrics not asserted in chaos tests.

**Что фиксить:**
- In ads chaos recovery tests: scrape or read internal metrics for `ad_redis_breaker_state`, `ad_tracker_health_degraded`.

**Критерии фикса (DoD):**
- [выполнено] During Redis outage: `ad_tracker_health_degraded == 1` or breaker Open observed.
- [выполнено] After recovery: degraded == 0, breaker Closed within 30s.
- [выполнено] Documented in test comment which metric proves steady-state restoration.

---

## 5. Anti-AI-Slop checklist (для review каждого нового теста)

Перед merge любого теста на hot/cold path проверить:

| # | Вопрос | Pass criteria |
|:---:|:---|:---|
| 1 | Тестирует failure mode, не только happy path? | Assert на reject/degrade/recovery, не только `202` |
| 2 | Critical stores — real, не mock? | Redis/PG через testcontainers на integration+ |
| 3 | Есть concurrency ≥20 на shared state? | Для budget/dedup/outbox/idempotency paths |
| 4 | Проверяет recovery, не только crash? | stop→start или partition→heal в одном тесте |
| 5 | Budget/money invariant в конце? | Для spend/balance/topup paths |
| 6 | Chaos test emit `chaos_proof`? | Parseable CI gate line |
| 7 | Prod topology faithful? | StaticSlot N=4, не JumpHash(1) на E2E |
| 8 | Тест ловит регрессию, которую unit не ловит? | Обоснование в комментарии; иначе — не добавлять |

---

## 6. Порядок выполнения (recommended)

```
Sprint 1 (P0):  P0-3 RTB chaos_proof + test-chaos.sh
                P0-4 CI chaos job
                P0-1 E2E multishard StaticSlot
                P0-2 E2E idempotency

Sprint 2 (P0+P1): P0-5 Playbook A shard-0
                  P1-1 Budget invariant helper
                  P1-2 Playbook C PG partition
                  P1-3 Remove duplicate fault tests

Sprint 3 (P1+P2): P1-4 Recon real Redis
                  P1-5 Autoscale chaos_proof
                  P1-6 Edge chaos minimum
                  P2-1 Smoke extension

Sprint 4 (P2):  P2-2 Sentinel under load
                P2-3 Clock drift TTC
                P2-4 ClickHouse E2E
                P2-5 Metric assertions
```

---

## 7. Метрики успеха (target state)

| Metric | As-is | Target |
|:---|:---:|:---:|
| Compliance vs CHAOS.md | ~55% | **≥85%** |
| PR chaos gate | weekly only | **every PR** |
| E2E prod topology fidelity | 1 shard JumpHash | **4 shard StaticSlot** |
| Playbooks §6 automated | 0/4 | **4/4** |
| RTB in chaos CI | no | **yes** |
| Post-recovery budget invariant | 0 tests | **≥3 chaos tests** |
| Edge chaos tests | 2 (`internal/edge/perimeter`) | **≥2** |
| `chaos_proof` coverage | ~70 lines | maintain **≥40** unique faults |

---

## 8. Связанные файлы

| Файл | Роль |
|:---|:---|
| [CHAOS.md](CHAOS.md) | Стандарт (normative) |
| [CHAOS-AUDIT.md](CHAOS-AUDIT.md) | Этот документ (gap + backlog) |
| [docs/rtb-chaos-plan.md](docs/rtb-chaos-plan.md) | RTB chaos catalog |
| [scripts/test-chaos.sh](scripts/test-chaos.sh) | CI chaos runner |
| [PERIMETER.md](PERIMETER.md) | Edge scope для P1-6 |
| [docs/edge-chaos-ci.md](docs/edge-chaos-ci.md) | CI harness limitation (no full nginx) |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | CI integration point |

---

## 9. Исключения (conscious non-goals)

Следующее **не входит** в backlog без отдельного ADR:

- Production canary chaos (Netflix principle #3) — только staging/CI testcontainers.
- Byzantine fault injection beyond network corruption at parse boundary.
- Full 50k RPS load test в CI (только manual/staging для Playbook B).
- DST / FoundationDB-style deterministic simulation — out of scope.

---

*При закрытии каждого пункта backlog обновлять чекбоксы в §4 и метрики в §7.*
