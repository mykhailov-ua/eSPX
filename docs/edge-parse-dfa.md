# Edge byte DFA — технический отчёт

**Дата:** 2026-06-29  
**Область:** `deploy/nginx/lua/edge-parse-dfa.lua`, `access-check.lua` phase 2  
**Цель:** единый DFA-проход по сырым байтам вместо `cjson.decode` и императивного `parse_proto`; немедленное отсечение при превышении лимитов.

---

## 1. Проблема

До изменения phase 2 использовала:

| Путь | Механизм | Недостатки |
|------|----------|------------|
| JSON | `cjson.decode` | Аллокации, полный разбор дерева, reflection-подобная семантика |
| Protobuf | Ручной walk + `string.format` UUID | Полный проход по телу, нет жёсткого scan budget |

Оба пути читали до **1 MiB** и не имели единой политики «срезать сразу».

---

## 2. Решение

Новый модуль **`edge-parse-dfa.lua`** — один API:

```lua
local dfa = require "edge-parse-dfa"

dfa.check_content_length(cl)           -- до read_body → ERR_OVERSIZE
dfa.extract_campaign_id(body, cl)      -- byte DFA → campaign_id | nil, err
```

### 2.1 Лимиты (константы модуля)

| Константа | Значение | Назначение |
|-----------|----------|------------|
| `MAX_BODY_BYTES` | 1 048 576 | Совпадает с tracker / `client_max_body_size` |
| `MAX_SCAN_BYTES` | 8 192 | Макс. байт, которые DFA анализирует на edge |
| `MAX_CAMPAIGN_LEN` | 64 | Макс. длина `campaign_id` (UUID text / raw) |
| `MAX_FIELD_LEN` | 65 536 | Макс. skip одного protobuf length-delimited поля |

### 2.2 Точки отсечения (срез payload)

```
Content-Length > MAX_BODY_BYTES     → 413 (до read_body, метрика parse_oversize)
#body > MAX_BODY_BYTES              → 413
protobuf field_len > MAX_FIELD_LEN  → ERR_OVERSIZE
campaign_id field_len > MAX_CAMPAIGN_LEN → ERR_OVERSIZE
JSON campaign_id string > MAX_CAMPAIGN_LEN → ERR_OVERSIZE
DFA scan window                     → min(#body, Content-Length, MAX_SCAN_BYTES)
```

В `access-check.lua` для in-memory body и spooled file в DFA передаётся **не более `MAX_SCAN_BYTES`** — CPU bounded даже при большом теле на диске.

### 2.3 Два режима DFA (автовыбор по первому значимому байту)

```
leading WS* → '{'  →  JSON object DFA (ключи, skip value без аллокаций)
иначе            →  Protobuf wire DFA (tag → varint → skip / capture field 1)
```

**Protobuf:** при `field == 1` (AdEvent.campaign_id) — захват bytes, early return, без сканирования metadata/user_id.

**JSON:** state machine по ключам; при `"campaign_id"` — извлечение string value; остальные ключи — `skip_json_value` без decode.

Формат UUID из 16 raw bytes — `table.concat` + nibble hex (без `string.format`).

---

## 3. Интеграция в pipeline

```
Phase 1 (без body)
  circuit_breaker → blacklist_cache

Phase 2
  check_content_length(CL)
  read_body (proxy по-прежнему получает полное тело через nginx)
  slice body to MAX_SCAN_BYTES для DFA
  extract_campaign_id → edge_rl.allow(campaign_id)
  proxy_pass
```

Удалено из `access-check.lua`:

- `require "cjson.safe"`
- `decode_varint`, `parse_proto`, `bytes_to_uuid_string`, `parse_track_body`

Новая метрика: `espx_edge_parse_oversize_total` (413 от DFA/CL).

---

## 4. Сравнение ДО / ПОСЛЕ

| | ДО | ПОСЛЕ |
|---|-----|-------|
| JSON | `cjson.decode` всего объекта | Byte DFA, только `campaign_id` |
| Protobuf | Full message walk | Early exit на field 1 |
| Scan budget | До 1 MiB | **8 KiB** на edge |
| Oversize | Только CL > 1 MiB | CL, body, field_len, campaign_len |
| Зависимости hot path | cjson + bit | bit only |

---

## 5. Ограничения (осознанные)

1. **`read_body` всё ещё вызывается** — nginx должен передать тело upstream; DFA не заменяет streaming proxy, только ограничивает **анализ**.
2. **`campaign_id` после 8 KiB** (JSON с ключом в конце) — edge не извлечёт → `edge_rl` не сработает; запрос уйдёт на tracker (как и при пустом parse).
3. **Malformed JSON/proto** — не 400 на edge; tracker отклонит (можно ужесточить отдельно).
4. **Не DFA в смысле таблицы переходов на 256 байт** — schema-specific state machine (как `ParseTrackRequestJSON` в Go), что для фиксированной схемы `/track` эффективнее generic DFA.

---

## 6. Тесты и деплой

```bash
bash scripts/edge-parse-dfa-test.sh   # luajit в espx-nginx-1
docker exec espx-nginx-1 nginx -s reload
curl -s http://127.0.0.1:8180/metrics/edge | grep parse_oversize
```

Покрытие тестов:

- Protobuf AdEvent с 16-byte UUID
- JSON `campaign_id` + `type`
- `Content-Length` > `MAX_BODY_BYTES`
- Protobuf campaign field length > `MAX_CAMPAIGN_LEN`

---

## 7. Файлы

| Файл | Изменение |
|------|-----------|
| `deploy/nginx/lua/edge-parse-dfa.lua` | **новый** — byte DFA |
| `deploy/nginx/lua/access-check.lua` | phase 2 на DFA, slice scan budget |
| `deploy/nginx/lua/edge-metrics.lua` | `espx_edge_parse_oversize_total` |
| `scripts/edge-parse-dfa-test.sh` | **новый** — unit tests |
| `docs/edge-parse-dfa.md` | этот отчёт |

---

## 8. Следующие шаги (не в scope)

- Early-exit уже есть для proto field 1; для JSON — skip без полного parse.
- Полный отказ от `read_body` на edge (socket peek) — только при смене proxy buffering strategy.
- Вынести лимиты в `edge-config` shared dict для prod tuning без reload lua.
