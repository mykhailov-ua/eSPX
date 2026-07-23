# Guide: Hot Path — Zero Alloc, BCE, Branches, Padding

**Status:** Mandatory for `internal/ingestion`, hot paths in `pkg/broker`, edge Lua DFA  
**Audience:** Go developers touching tracker, processor ingest, broker wire codec  
**Related:** `.cursorrules`, [GO.md](./docs/GO.md), `make test-alloc-gate`, `scripts/perf-gate/`

## Goal

Единый чеклист и каталог приёмов для hot path: **0 heap allocs/op**, предсказуемые ветки, отсутствие false sharing, корректный BCE. Cold path (`management`, `adminapi`, webhooks) — idiomatic Go + `encoding/json`; этот гайд на него **не** распространяется.

## Scope

| In scope | Out of scope |
| :--- | :--- |
| `/track`, gnet, FilterEngine, RTB `RunAuction`, broker `ReadFrame` | Admin HTMX, cost-sync external APIs |
| Schema DFA (JSON/proto subset), table FSM (HTTP/H2) | Полный OpenRTB validate (admin lint) |
| vtproto pools, `unsafe.String` lifetime | CGO (кроме benchmark baseline) |
| Padding atomics, ingress quota cells | ORM, reflection mapping |

---

## 1. Политика zero-alloc

1. **Определение hot path:** код, вызываемый на каждом `/track` (или каждом broker frame) до ответа клиенту.
2. **Критерий:** `go test -benchmem` → `allocs/op == 0` на затронутых бенчмарках.
3. **CI:** `make test-alloc-gate` блокирует регрессии.
4. **Исключения** только с записью в PR + новый бенчмарк, доказывающий что alloc не на request path (например, cold pool `New`).

### Что запрещено на hot path

| Запрет | Почему |
| :--- | :--- |
| `interface{}` / `any` как аргумент | boxing → heap escape |
| Closures в request loop | capture context → heap, no inline |
| `sync.Map` | interface boxing + hash |
| `fmt.Sprintf`, `strings.Builder` per request | alloc per call |
| `+` на `string` в цикле | каждый `+` — новая строка в куче |
| `context.WithTimeout` per request | alloc context + timer |
| Динамические Prometheus labels | label set alloc + map |
| `encoding/json` на request body | reflection + alloc |
| `uuid.Parse(string)` | alloc промежуточной строки; использовать `ParseUUID([]byte, &dst)` |
| `defer` в tight loop | overhead; явный cleanup или pool `Put` в конце |

### Что разрешено

| Приём | Пример в репо |
| :--- | :--- |
| vtproto `UnmarshalVT` / `MarshalToSizedBufferVT` | `track_ingest_gnet.go`, `broker_payload.go` |
| Schema DFA для JSON | `ParseTrackRequestJSON`, `requests_parse_opt.go` |
| `sync.Pool` на границе request (не в inner loop) | `streamEventPool`, `connContext` |
| `atomic.Value` snapshot (cold write, hot read) | `SettingsWatcher`, `StaticSlotSharder` |
| Prebuilt `[]byte` ответов | `respBudget`, `respRateLimit` в `handler.go` |
| Ручная JSON-сборка фиксированной схемы | `writeGnetTrackAccepted` JSON branch |

---

## 2. BCE (Bounds-Check Elimination)

Go вставляет `runtime.panicIndex` при `buf[i]` без доказательства компилятору, что `i < len(buf)`.

### Паттерн: early abort + hint

```go
// requests_parse.go, openrtb будущий FSM
if len(data) == 0 {
    return errMalformedJSON
}
_ = data[len(data)-1] // BCE: весь цикл ниже без panicIndex на data[i]

n := len(data)
for i := 0; i < n; i++ {
    // ...
}
```

### Паттерн: window check перед циклом по срезу

```go
if end > len(buf) {
    return ErrMalformed
}
for i := start; i < end; i++ {
    c := buf[i] // без panicIndex в теле
}
```

### Верификация

```bash
go build -gcflags=-m=2 ./internal/ingestion/... 2>&1 | rg 'escapes|moved to heap'
go tool objdump -S -linenumbers ./tracker | rg 'panicIndex|morestack'
```

**Красные флаги в asm:** `CALL runtime.panicIndex`, частые `CALL runtime.morestack` в inner loop.

---

## 3. Branch prediction

CPU (TAGE/BTB) штрафует непредсказуемые ветки. Hot path должен минимизировать **data-dependent** branches.

### Предсказуемые ветки

| Техника | Когда |
| :--- | :--- |
| **Сортировка + early break** | RTB candidates по bid/score — после первого fail break предсказуем |
| **Разделение fast/slow path** | `if contentType == protobuf` — один раз на входе, не в filter chain |
| **Unlikely error path в конце** | Сначала happy path, `return err` редко |

### Branch-less / low-branch

| Техника | Пример |
| :--- | :--- |
| **Bitmask** | `deviceMask & reqMask != 0` вместо цепочки `if country == ...` |
| **Lookup table `[256]byte`** | `jsonWhitespace[c]` в `requests_parse_opt.go` |
| **`switch len(key)`** | `matchTrackKey` — 4/7/8/11 байт, не generic string compare |
| **Packed integer compare** | `loadU32(key) == u32Type` вместо посимвольного `key[0]=='t' && ...` |

### Packed key matching (эталон)

```go
// requests_parse_opt.go
const u64ClickID uint64 = 0x64695f6b63696c63 // "click_id" little-endian

func loadU64(b []byte) uint64 { /* len(key) checked in switch before call */ }

switch len(key) {
case 8:
    if loadU64(key) == u64ClickID {
        return keyClickID
    }
}
```

**Правило:** для фиксированных ключей JSON/proto — `switch len` + 1–2 machine-word сравнения, не `bytes.Equal` в цикле по всем полям.

### Непредсказуемые ветки — выносить с hot path

- Geo lookup fail-open
- Редкие reject reasons (fraud L3)
- Metrics sample mask (`MetricsHistogramSampleMask`)

---

## 4. Padding и false sharing

Cache line = **64 bytes** (типично x86). Два atomics на одной линии → MESI invalidation между ядрами.

### Паттерн: pad contended atomics

```go
// ingress_quota.go
type IngressQuotaCell struct {
    maxAllowed uint64
    _          [ingressCacheLine - 8]byte
    currentOps atomic.Uint64
    _          [ingressCacheLine - 8]byte
}
```

```go
// fraud_stream_queue.go — cursors MPSC ring
type FraudStreamWriter struct {
    _           [64]byte
    writeCursor uint64
    _           [64]byte
    allocCursor uint64
    _           [64]byte
    readCursor  uint64
    // ...
}
```

```go
// management/shard_orchestrator.go — EWMA per shard
type PaddedEma struct {
    Value float64
    _     [56]byte
}
```

### Правила padding

1. **Hot global struct** с ≥2 полями, обновляемыми разными горутинами → pad между ними.
2. **Размер pad:** `64 - sizeof(field)` с выравниванием; или `cpu.CacheLinePad` из `golang.org/x/sys/cpu`.
3. **Не pad** единичные read-mostly поля в cold struct.
4. **SoA + pad:** ingress quota — массив `IngressQuotaCell`, не `[]atomic.Uint64`.

### Atomic thunder (читатели)

- Конфиг: `atomic.Value` swap целого snapshot, не по полю.
- В цикле: **один** `Load` снаружи, локальная копия внутри.
- CAS только если значение реально меняется.

---

## 5. Escape analysis и inlining

### Struct layout (size-desc)

Поля от большего к меньшему — меньше padding внутри struct, лучше pack в cache line.

### Конкретные типы вместо интерфейсов

```go
// Плохо (hot path)
func (e *FilterEngine) Check(ctx context.Context, evt *Event) {
    for _, f := range e.filters { // []EventFilter interface
        f.Check(ctx, evt)
    }
}

// Допустимо: цепочка фиксирована при старте; type assert только для UnifiedFilter short-circuit
// Новые фильтры — не добавлять через interface{} на /track без бенчмарка
```

### Inlining

- Функции hot loop — **короткие**, без `defer`, без loop внутри (или развернуть).
- `//go:noinline` только для cold error paths если нужно уменьшить code size hot caller.

---

## 6. Строки и байты

### unsafe.String

```go
// filters.go — lifetime буфера = gnet read frame или evt.StringBuffer
func unsafeString(b []byte) string {
    if len(b) == 0 {
        return ""
    }
    return unsafe.String(&b[0], len(b))
}
```

**Контракт:** view не переживает ring buffer gnet. Нужно за пределы frame → `copy()` в owned `evt.StringBuffer`.

### unsafe.Slice / StringData

```go
// unified_filter.go — Redis args без копии
unsafe.Slice(unsafe.StringData(sv.s), len(sv.s))
```

### append вместо concat

```go
// Плохо
s := s + string(b)

// Хорошо
evt.StringBuffer = append(evt.StringBuffer[:0], b...)
evt.FraudReason = unsafeString(evt.StringBuffer)
```

### UUID без alloc

```go
// requests_parse.go
ParseUUID(valBytes, &v.CampaignID) // не uuid.Parse(string)
```

---

## 7. Пулы и ring buffers

| Паттерн | Где |
| :--- | :--- |
| `sync.Pool` protobuf | `streamEventPool`, `adEventPool` |
| `connContext` per connection | gnet `SetContext` — reuse pb/trackReq/evt |
| MPSC ring fixed slots | `FraudStreamWriter` — lossy, no alloc enqueue |
| Pre-cap slice | `evt.Payload = append(evt.Payload[:0], fields.payload...)` |

**Правило pool:** `Get` в начале request, `Put` на всех exit paths; `Reset()` перед reuse.

---

## 8. Время и дедлайны

```go
// Монотонные наносекунды — без time.Now() на hot path
evt.FilterDeadlineMono = monotonicNano() + timeout.Nanoseconds()

// Проверка
if monotonicNano() > evt.FilterDeadlineMono {
    return context.DeadlineExceeded
}
```

Запрещено: `time.Now()`, `context.WithTimeout` per request.

---

## 9. DFA vs vtproto vs encoding/json

```text
                    ┌─────────────────────────────────────┐
                    │         Hot path ingest?            │
                    └─────────────────┬───────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
        Protobuf wire           Fixed JSON schema        Arbitrary JSON
              │                       │                       │
              ▼                       ▼                       ▼
         vtproto pools          Schema DFA              COLD ONLY
    UnmarshalVT/MarshalVT    ParseTrackRequestJSON*     encoding/json
                             matchTrackKey, varint
```

| Формат | Hot path | Cold path |
| :--- | :--- | :--- |
| `AdEvent` / `AdStreamEvent` protobuf | vtproto | — |
| `/track` JSON (6–7 полей) | DFA (`requests_parse*.go`) | — |
| OpenRTB bid (R6) | DFA FSM (`openrtb26_parse.go`) | `openrtb_validate.go` + json |
| HTTP/1.1 headers | Table FSM (M5-B) | — |
| HTTP/2 frames | Binary frame FSM (M5-C) | — |
| Admin API, outbox, PG JSON columns | — | `encoding/json` / `DecodeJSONStrict` |
| Postback payload | DFA (если RPS растёт) | сейчас json |

**Не добавлять:** sonic, jsoniter, easyjson на tracker без alloc-gate waiver.

---

## 10. Метрики и observability

- Counters/histograms: **pre-bound** labels в `init()` (`handler.go` `preboundTrackMetrics`).
- Sampled histograms: bitmask `MetricsHistogramSampleMask`, не `rand` per request.
- Lossy telemetry: `LatencyRing`, `FraudStreamWriter` — drop, не unbounded buffer.

---

## 11. Чеклист перед merge (hot path PR)

```bash
# 1. Alloc gate
go test -benchmem -run='^$' -bench='BenchmarkYourPath' ./internal/ingestion/...
make test-alloc-gate

# 2. Escape (выборочно)
go build -gcflags=-m=2 ./internal/ingestion/your_file.go 2>&1 | head -50

# 3. ASM (perf-critical)
go test -c -o /tmp/ingestion.test ./internal/ingestion/...
go tool objdump -S /tmp/ingestion.test | rg -A5 'YourHotFunc'

# 4. Perf gate (если меняется tracker/broker baseline)
bash scripts/perf-gate/perf_gate_run.sh
```

PR checklist:

- [ ] `allocs/op == 0` на новом/изменённом бенчмарке
- [ ] Нет новых `interface{}` / closures в request loop
- [ ] BCE hint (`_ = slice[len-1]`) на входе parse loop
- [ ] Contended atomics padded (если добавлены globals)
- [ ] `unsafe.String` lifetime документирован в комментарии если неочевиден
- [ ] JSON на hot path только schema DFA
- [ ] Chaos test если новый write path (см. `GUIDE_CHAOS_RELIABILITY.md`)

---

## 12. Эталонные файлы в репозитории

| Файл | Что смотреть |
| :--- | :--- |
| `internal/ingestion/requests_parse.go` | Schema JSON DFA + BCE |
| `internal/ingestion/requests_parse_opt.go` | Packed keys, whitespace table, branch reduction |
| `internal/ingestion/requests_parse.go` `ParseUUID` | Zero-alloc UUID |
| `internal/ingestion/unified_filter.go` | `StringVal`, scratch pools, no defer |
| `internal/ingestion/ingress_quota.go` | Cache-line padding |
| `internal/ingestion/fraud_stream_queue.go` | MPSC ring, padded cursors |
| `internal/ingestion/sharding.go` | `atomic.Value` slot table |
| `internal/ingestion/handler.go` | Prebuilt HTTP responses, manual JSON 202 |
| `pkg/broker/protocol/protocol.go` | Binary frame parse, fixed buffers |
| `deploy/nginx/lua/edge-parse-dfa.lua` | Varint/proto DFA (Lua reference) |
| `internal/postback/macro_engine.go` | ParseTemplate at config time (cold) |

---

## 13. Антипаттерны (найденные в коде, цель M12)

| Место | Проблема | Цель |
| :--- | :--- | :--- |
| `http1_fsm.go` | table-driven FSM (M5-B) | — |
| `openrtb_parse.go` | substring `bytes.Index` | M12-02 / M7 R6 |
| Production JSON path | `ParseTrackRequestJSON` вместо `Opt` | M12-01 |
| `processor.go` legacy map | `uuid.Parse`, type asserts | M12-03 |
| `registry.go` pubsub | `uuid.Parse` | M12-04 |
