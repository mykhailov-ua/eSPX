# Оптимизация рантайма Go в eSPX

Контур импорта eSPX (hot path) должен укладываться в жесткий SLA: время обработки запроса трекером не должно превышать 20 мс при миллионных объемах RPS. Для выполнения этих требований применены низкоуровневые оптимизации Go-компилятора, планировщика и памяти.

---

## 1. Архитектура обработки сети: gnet и PinnedWorkerPool

Стандартная библиотека Go (`net/http`) на каждый запрос порождает новую goroutine. Под высокой нагрузкой это приводит к интенсивному переключению контекста планировщика (G/M/P), фрагментации стеков и высокой нагрузке на сборщик мусора (GC).

### Использование gnet
eSPX использует событийно-ориентированную сетевую библиотеку `gnet`. Она работает поверх системного вызова `epoll` (Linux) в режиме мультиплексирования:
- Ограниченное число системных потоков (обычно 2 на поток процессора) вычитывают сетевые буферы.
- Данные парсятся без аллокаций с помощью конечного автомата (DFA HTTP/1.1 scanner) прямо из кольцевого буфера сетевого сокета.

### PinnedWorkerPool (Привязка воркеров)
После первичного разбора пакета в gnet управление передается воркер-пулу `PinnedWorkerPool`:
- Каждому соединению или ID кампании назначается конкретная goroutine воркера (pinning).
- Это повышает утилизацию процессора, так как данные кампании оседают в L1/L2 кэшах одного ядра CPU (locality of reference).
- Очереди задач между сетевыми потоками и воркерами реализованы на базе кольцевых буферов типа MPSC (Multi-Producer Single-Consumer).
- Структуры очередей имеют выравнивание по границе кэш-линии процессора (cache-line padding в 64 байта) для предотвращения эффекта ложного разделения кэша (false sharing):
  ```go
  type CacheLinePad struct {
      _ [64 - (unsafe.Sizeof(atomic.Int64{}) % 64)]byte
  }
  ```

---

## 2. Нулевые аллокации (Zero-Allocation Policy)

Для полного исключения пауз сборщика мусора на горячем пути запрещено динамическое выделение памяти в куче (heap escape).

### Локальные пулы контекстов
Переиспользование объектов сетевых запросов и ответов выполняется через локальные пулы контекстов соединений. В отличие от глобального `sync.Pool`, который разделяется между всеми ядрами и создает накладные расходы на синхронизацию, локальные пулы не используют атомарные операции и mutex-блокировки.

### Zero-Copy преобразования типов
При чтении данных из сетевых пакетов или при сериализации в Protobuf/JSON используется небезопасное преобразование слайсов байт в строки без копирования памяти через `unsafe.Pointer`:
```go
func unsafeString(b []byte) string {
    if len(b) == 0 {
        return ""
    }
    return unsafe.String(&b[0], len(b))
}
```
*Внимание: Применение этого метода требует строгого контроля времени жизни исходного буфера байт, чтобы строка не начала указывать на очищенную или перезаписанную память.*

---

## 3. Монотонное время против Clock Drift

В распределенных системах использование системного времени (wall-clock time) опасно из-за возможной синхронизации NTP или сдвигов времени (leap seconds). Для замеров интервалов времени (например, времени с момента показа до клика — Time-To-Click) eSPX использует монотонное время:
- Для этого считывается значение таймера процессора через вызовы `runtime.nanotime` (монотонный счетчик).
- Дедлайн обработки запроса вычисляется один раз при входе в фильтр-движок:
  ```go
  evt.FilterDeadlineMono = monotonicNano() + timeout.Nanoseconds()
  ```
- Все последующие сетевые клиенты (Redis, Postgres) при каждом вызове сравнивают монотонное время с дедлайном и автоматически занижают свои таймауты подключения.

---

## 4. Оптимизация компиляции и Анализ

Каждое изменение в контуре `internal/ads` проверяется с помощью анализа отчетов компилятора Go.

### Устранение проверок выхода за границы (BCE - Bounds Check Elimination)
При доступе к элементам слайсов компилятор Go по умолчанию встраивает инструкции проверки индекса. Чтобы помочь компилятору оптимизировать этот код в циклах, в начале функции выполняется превентивная проверка максимальной длины (Bounds Check Elimination hint):
```go
func processItems(items []Item) {
    if len(items) < 4 {
        return
    }
    // Компилятор видит эту проверку и отключает проверки индексов 0, 1, 2, 3 внутри тела функции
    _ = items[3] 
    ...
}
```
Верификация BCE выполняется командой:
```bash
go build -gcflags="-d=ssa/prove/debug=1" ./internal/ads/...
```

### Escape-анализ и Инлайнинг
Для контроля утечек в кучу и инлайнинга мелких функций используются флаги сборки:
- Проверка утечек в кучу:
  ```bash
  go build -gcflags="-m -m" ./internal/ads/... 2>&1 | grep "escapes to heap"
  ```
- Проверка инлайнинга (встраивания кода мелких функций непосредственно в точку вызова для устранения накладных расходов на вызов функции):
  ```bash
  go build -gcflags="-m" ./internal/ads/... 2>&1 | grep "can inline"
  ```
  Функции горячего пути с низкой цикломатической сложностью и отсутствием сложных ветвлений/циклов помечаются компилятором как пригодные для инлайнинга автоматически.

---

## 5. UDP-only recovery (control plane)

When `UDP_CONTROL_ENABLED=true`, ingress limits and epoch state arrive **only** over UDP (`management :8190` → `tracker :8191`). There is no HTTP/gRPC config pull on the hot path.

### Symptoms

| Signal | Meaning |
|--------|---------|
| `ad_udp_ingress_reject_total` rising | Epoch stale or quota map not refreshed |
| `ad_tracker_health_degraded` | Redis or filter path unhealthy; ingress may still gate |
| Ingress 429 on `/track` | Per-shard RPS cap from `IngressQuotaCell` |
| STALE channel | No valid packet within `2 × sync_interval` |

### Recovery procedure

1. **Confirm blast radius** — one shard / one tracker pod if possible. Compare control-cohort p99 on unaffected shards (ChAP).
2. **Check management UDP publisher** — `UDP_TRACKER_ADDRS` must list every tracker `host:8191`. Management sends each epoch 3× unicast + 1× broadcast per interval.
3. **Tracker CONFIG_REQUEST** — on STALE or epoch gap, tracker emits `CONFIG_REQUEST` with `tracker_id`, `last_epoch`, `config_hash`. Management responds with `CONFIG_SNAPSHOT` burst (5×) sourced from Postgres `control_plane_epochs`.
4. **Fail-closed policy** — with `UDP_FAIL_CLOSED=true`, STALE applies canary floor ingress (low RPS). **Do not** raise limits manually in Redis budget keys; UDP never writes `budget:*`.
5. **Epoch gap rules** — tightening (lower RPS / higher epoch) applies immediately; loosening without a signed snapshot is **rejected** (`udp_epoch_gap_loosen_block` proof).
6. **Clock resync** — coarse UDP time adjusts `cachedUnixMilli` by at most ±50 ms per packet and never decreases wall ms on the hot path; TTC deadlines use monotonic time only (`clock_drift_udp_time` chaos proof).
7. **Verify** — `ad_udp_ingress_acquire_total` recovers, control p99 < 80 ms for 30 s, `AssertBudgetInvariant` ±1 micro on sample campaigns.

### Network impairment reference (game day)

| Profile | loss | delay | jitter | Expected behaviour |
|---------|------|-------|--------|-------------------|
| `udp_light` | 1% | 0 | 0 | Epoch monotonic |
| `udp_moderate` | 5% | 2 ms | 1 ms | STALE → canary floor |
| `udp_severe` | 20% | 10 ms | 5 ms | CONFIG_REQUEST recovery ≤3 s; no overspend |

Abort load if control-cohort p99 > 80 ms for 30 s or R5 violation. See `scripts/load/run_game_day.sh` and `GUIDE_CHAOS_RELIABILITY_RU.md` scenarios A–H.
