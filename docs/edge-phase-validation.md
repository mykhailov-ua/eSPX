# Edge two-phase validation — metrics, topology, cascade analysis

**Date:** 2026-06-29  
**Bench:** `scripts/edge-phase-bench.sh` on local stack (`espx-nginx-1`, redis-0 up, trackers down)  
**Host:** 12 CPU cores, `worker_processes auto` → 12 nginx workers

---

## 1. Implementation summary

| Layer | Phase | Steps | `read_body` |
|-------|-------|-------|-------------|
| `nginx.conf` | 0 | `limit_req` 100 r/s, `limit_conn` per-IP / global | No |
| `access-check.lua` | **1** | Circuit breaker → `blacklist_cache` dict lookup | **No** — `403` / `503` |
| `access-check.lua` | **2** | Content-Length → `read_body` → proto/JSON scan → `edge_rl` | Yes |

Background (worker 0 timers, not per-request): `edge-blacklist-sync` (SMEMBERS → dict), `edge-config` (HMGET → dict).

**Files changed:** `deploy/nginx/lua/access-check.lua`, `deploy/nginx/lua/edge-metrics.lua`, `scripts/edge-phase-bench.sh`.

**New Prometheus counters** (`GET /metrics/edge`):

| Metric | Meaning |
|--------|---------|
| `espx_edge_phase1_pass_total` | Passed circuit breaker + IP blacklist |
| `espx_edge_phase2_pass_total` | Passed body parse + campaign RL |
| `espx_edge_body_read_total` | `ngx.req.read_body` invoked |
| `espx_edge_circuit_reject_total` | Circuit breaker 503 |
| `espx_edge_blocked_ip_total` | IP blacklist 403 |
| `espx_edge_blocked_campaign_rl_total` | Campaign RL 429 |

---

## 2. Metrics ДО / ПОСЛЕ

### 2.1 Сценарий `blocked_ip` — 127.0.0.1 в `blacklist:manual`, 100 POST, concurrency 10

| | **ДО** (ee3b1a8, per-request Redis) | **ПОСЛЕ** (двухфазная + timer sync) |
|---|--------------------------------------|-------------------------------------|
| HTTP 403 | **0** | **59** |
| HTTP 502 (upstream dead) | **59** | 0 |
| HTTP 429 (`limit_req`) | 41 | 41 |
| `espx_edge_body_read_total` Δ | n/a (метрики не было) | **0** |
| `espx_edge_blocked_ip_total` Δ | 0 | **+59** |
| Wall time | 117 ms | 113 ms |

**Вывод:** старый путь при недоступном Sentinel/Redis на hot path делает **fail-open** → `read_body` + parse + proxy на мёртвый upstream (502). Новый путь отсекает по `lua_shared_dict` в фазе 1 без чтения тела.

Теоретически при рабочем Redis (ДО, cache miss, rotating IPs): **каждый** новый IP → `read_body` + connect + 2× `SISMEMBER` (~100–300 µs RTT каждый) до 403. ПОСЛЕ: **0** `read_body` для любого IP уже в dict.

### 2.2 Сценарий `legit` — IP не в blacklist, 80 POST, concurrency 8

| | **ПОСЛЕ** |
|---|-----------|
| `phase1_pass` Δ | +58 |
| `body_read` Δ | +58 |
| `phase2_pass` Δ | +58 |
| HTTP 502 | 58 (tracker down) |
| HTTP 429 | 22 |

Для легитимного трафика `body_read` / `phase1_pass` = 1:1 — фаза 2 вызывается только после успешной фазы 1. Это ожидаемо.

---

## 3. Распараллеливание по ядрам (ASCII)

### ДО — монолитный pipeline в одном воркере

Один запрос = одна цепочка в **одном** worker thread. Разные запросы распределяются по ядрам, но **внутри** запроса всё последовательно, включая дорогие шаги до отсечения:

```
                    ┌──────── Core 0 ────────┐  ┌──────── Core 1 ────────┐
Client requests ──► │ W0: req-A sequential   │  │ W1: req-B sequential   │  ...
                    │  limit_req             │  │  limit_req             │
                    │  circuit_breaker       │  │  circuit_breaker       │
                    │  cache miss            │  │  cache miss            │
                    │  read_body  ◄── CPU    │  │  read_body  ◄── CPU    │
                    │  parse_proto           │  │  parse_proto           │
                    │  crc32 → shard pick    │  │  crc32 → shard pick    │
                    │  Redis connect SYNC    │  │  Redis connect SYNC    │
                    │  SISMEMBER ×2 SYNC     │  │  SISMEMBER ×2 SYNC     │
                    │  proxy_pass → tracker  │  │  proxy_pass → tracker  │
                    └────────────────────────┘  └────────────────────────┘
                              ▲                           ▲
                              └──── Redis pool contention ┘
```

Проблема: «мусорный» IP проходит `read_body` + parse **до** blacklist. При ротации IP cache miss на каждый запрос → линейный рост CPU и Redis conn на воркер.

### ПОСЛЕ — двухфазная модель (фазы последовательны в воркере, запросы параллельны по ядрам)

```
  Internet
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ OpenResty :8180   worker_processes auto  →  12 workers / 12 cores       │
│                                                                         │
│  ┌─ Core 0 / W0 ─────────────────────────────────────────────────┐   │
│  │ Req-1: [Phase1 cheap] → pass → [Phase2 expensive] → proxy      │   │
│  │ Req-5: [Phase1] → 403 EXIT (no read_body)                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│  ┌─ Core 1 / W1 ─────────────────────────────────────────────────┐   │
│  │ Req-2: [Phase1] → pass → [Phase2] → proxy                       │   │
│  │ Req-6: [Phase1] → 503 circuit breaker EXIT                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│       ... W2..W11 аналогично, каждый воркер независим ...              │
│                                                                         │
│  Phase 1 (все воркеры, без body):                                       │
│    limit_req → circuit_breaker (shared dict) → blacklist_cache GET      │
│                                                                         │
│  Phase 2 (только pass Phase 1):                                         │
│    Content-Length → read_body → parse_proto/cjson → edge_rl (dict)    │
│                                                                         │
│  Worker 0 timer (фон, не блокирует hot path):                           │
│    edge-blacklist-sync: SMEMBERS → dict каждые 5s                        │
│    edge-config: HMGET → dict каждые 5s                                   │
└─────────────────────────────────────────────────────────────────────────┘
      │ proxy_pass (round-robin)
      ▼
  tracker :8181–8184
```

**Ключевое:** для одного запроса фазы 1→2 **строго последовательны** в одном воркере. Параллелизм — между запросами на разных ядрах. Заблокированный IP не занимает Phase 2 слот (нет alloc под body, нет parse).

---

## 4. Синхронные операции в Lua validation (аудит)

### 4.1 Per-request (hot path) — `access-check.lua`

| Операция | Фаза | Блокирует воркер? | ПОСЛЕ |
|----------|------|-------------------|-------|
| `ngx.shared.*:get/incr` | 1, 2 | Да (shm lock, ~µs) | Да |
| `ngx.req.get_headers()` | 2 | Да | Да |
| `ngx.req.read_body()` | 2 | **Да** — ждёт тело от клиента | Только Phase 2 pass |
| `io.open` + `fh:read` (body on disk) | 2 | **Да** — sync disk I/O | Редко (>64k body) |
| `cjson.decode` | 2 | Да — CPU | JSON path only |
| `parse_proto` byte walk | 2 | Да — CPU | Proto path |
| `edge_rl.allow` → `dict:incr` | 2 | Да (shm) | Да |
| `ngx.exit` / `proxy_pass` | — | — | — |

**Удалено с hot path (ДО):** `redis:connect`, `redis:auth`, `redis:sismember` ×2, Sentinel `get-master-addr-by-name`, `ngx.crc32_long` + shard routing.

### 4.2 Background timers (worker 0) — не на hot path, но sync внутри timer

| Модуль | Операция | Интервал | Риск |
|--------|----------|----------|------|
| `edge-blacklist-sync` | TCP connect + AUTH + SMEMBERS ×2 | 5 s | Блокирует timer context, не request worker |
| `edge-config` | TCP connect + AUTH + HMGET | 5 s | То же |
| `init-worker` healthcheck | HTTP GET `/health` ×4 upstream | 2 s | Отдельные cosockets в timer |

### 4.3 `nginx.conf` (до Lua)

| Операция | Блокирует? |
|----------|------------|
| `limit_req` / `limit_conn` | Да — shm, O(1) |
| `proxy_pass` + upstream connect | Да — TCP к tracker |

---

## 5. Потенциальные причины каскадного падения на LB слое

| # | Механизм | ДО | ПОСЛЕ | Каскад |
|---|----------|----|-------|--------|
| C1 | **Per-request Redis blacklist** | connect+SISMEMBER на cache miss | Убрано | Redis latency ↑ → воркеры заняты → `limit_conn` 8192 → 503 для всех |
| C2 | **Fail-open на Redis error** | `return` → proxy | Dict-only; stale ≤5s | Атака проходит на tracker при падении Redis |
| C3 | **`read_body` до blacklist** | Да на miss | Нет в Phase 1 | CPU/RAM ↑ на всех воркерах → latency ↑ → client retry ×2 |
| C4 | **Circuit breaker на Redis err rate** | Открывается от blacklist errors | Err counter не инкрементируется blacklist'ом | Ложный 503 шторм на весь `/track` |
| C5 | **`limit_conn` global 8192** | Без изменений | Без изменений | Slowloris / большие body → исчерпание слотов |
| C6 | **`edge_rl` dict full → fail-open** | — | `incr` fail → allow | Campaign flood на tracker |
| C7 | **Upstream all down + `proxy_next_upstream`** | Retry на 4 tracker | Без изменений | 1 edge req → до 4 upstream попыток |
| C8 | **Blacklist sync single-threaded** | N/A | Worker 0 only | При огромном set SMEMBERS блокирует timer; dict stale до 5s |
| C9 | **Sentinel resolve на request path** | ДО: per-request | Только в timer sync | Sentinel flap → ДО fail-open storm |

**Наиболее вероятный сценарий каскада (ДО):** POST-flood с ротацией IP → каждый miss: `read_body(1MiB)` + Redis RTT → worker CPU 100% + Redis conn storm → circuit breaker OPEN → **503 для легитимного трафика** → клиенты retry → усиление.

**ПОСЛЕ:** blocked IP отсекается в Phase 1 за ~2 dict GET; Redis на hot path = 0; circuit breaker не зависит от blacklist Redis.

---

## 6. Воспроизведение

```bash
# Blocked IP flood
bash scripts/edge-phase-bench.sh blocked_ip 100 10

# Legit (убедитесь что IP не в blacklist)
docker exec espx-redis-0-1 redis-cli -p 6379 -a "$REDIS_PASS" --no-auth-warning SREM blacklist:manual 127.0.0.1
docker exec espx-nginx-1 nginx -s reload && sleep 6
bash scripts/edge-phase-bench.sh legit 80 8

# Метрики
curl -s http://127.0.0.1:8180/metrics/edge
```

Результаты: `var/edge-baseline/before-*.txt`, `after-*.txt`.
