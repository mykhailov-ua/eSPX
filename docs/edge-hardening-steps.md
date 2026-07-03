# Пошаговый план реализации Edge Hardening (Lua + XDP)

Этот документ содержит детальные, пошаговые инструкции по реализации плана защиты сетевого периметра (Edge Hardening) для системы eSPX. План разделен на 6 последовательных этапов (от Фазы 0 до Фазы 5), каждый из которых имеет четкие критерии входа, шаги реализации, шаблоны конфигураций и критерии успешности (SLA gates).

---

## Фаза 0: Подготовка, тюнинг ОС и снятие базовых метрик

**Цель:** Подготовить сетевой стек Linux и сетевую карту к высоким нагрузкам, зафиксировать базовые показатели производительности (SLA) до внесения изменений.

### Шаг 0.1: Тюнинг сетевой карты (NIC) [Выполнено]
Настроить размер кольцевого буфера приема (RX ring) и распределение прерываний (RSS) на сервере ingress.

**Автоматизация (рекомендуется):**
```bash
# Применить тюнинг (RX ring → hardware max, IRQ spread / irqbalance)
sudo bash scripts/edge-nic-tune.sh apply

# Проверить состояние без изменений
bash scripts/edge-nic-tune.sh report

# CI / post-deploy gate: exit 1 если тюнинг не применён
bash scripts/edge-nic-tune.sh verify

# Сохранить настройки после перезагрузки (systemd oneshot)
sudo bash scripts/edge-nic-tune.sh install-systemd
sudo systemctl start espx-edge-nic-tune
```

Переменные окружения: `INGRESS_INTERFACE` (если авто-детект по default route не подходит), `IRQ_STRATEGY=auto|irqbalance|spread`, `DRY_RUN=1`.

**Ручная проверка (эквивалент шагов скрипта):**
1. Определить имя публичного сетевого интерфейса (например, `eth0` или `ens3f0`):
   ```bash
   ip route | grep default
   ```
2. Проверить текущие и максимальные поддерживаемые размеры кольцевых буферов:
   ```bash
   ethtool -g eth0
   ```
3. Установить размер RX ring в максимально поддерживаемое аппаратное значение (обычно 4096):
   ```bash
   sudo ethtool -G eth0 rx 4096
   ```
4. Проверить распределение очередей прерываний (RSS) по ядрам процессора:
   ```bash
   cat /proc/interrupts | grep eth0
   ```
   Убедиться, что прерывания распределены по всем доступным ядрам (с помощью демона `irqbalance` или ручной привязки `/proc/irq/*/smp_affinity`).

### Шаг 0.2: Настройка параметров ядра (sysctl)
Применить оптимизации сетевого стека из `deploy/edge/99-espx-edge.conf`:

**Автоматизация (рекомендуется):**
```bash
sudo bash scripts/edge-sysctl.sh apply
bash scripts/edge-sysctl.sh report
bash scripts/edge-sysctl.sh verify
```

**Ручной вариант** — скопировать в `/etc/sysctl.d/99-espx-edge.conf`:
```ini
# Максимальное количество открытых сокетов в очереди accept()
net.core.somaxconn = 16384

# Максимальный размер очереди полуоткрытых соединений (SYN backlog)
net.ipv4.tcp_max_syn_backlog = 16384

# Включение механизма TCP SYN Cookies для защиты от SYN-флуда
net.ipv4.tcp_syncookies = 1

# Оптимизация буферов приема и передачи для высоконагруженных соединений
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Быстрая утилизация сокетов в состоянии TIME_WAIT
net.ipv4.tcp_tw_reuse = 1
```
Применить изменения:
```bash
sudo sysctl --system
```

### Шаг 0.3: Снятие базовых метрик (SLA Baseline)
**Минимальный замер (достаточно для Phase 0):** однократный снимок из Prometheus после поднятия стека:
```bash
bash scripts/edge-baseline.sh          # stdout + var/edge-baseline/latest.txt
bash scripts/edge-baseline.sh verify   # exit 1, если p95/p99/redis_lua уже выше SLA
```
Переменные: `PROMETHEUS_URL` (по умолчанию `http://127.0.0.1:9190`), `STRICT=1` — падать, если Prometheus недоступен.

Полный 24-часовой soak **опционален** (перед прод-канарейкой). Целевые значения для справки:
1. **Задержка трекера (SLA):**
   - p95 (`ad_http_request_duration_seconds`): < 50 ms.
   - p99: < 80 ms.
2. **Задержка Redis Lua:**
   - p99 (`ad_redis_lua_duration_seconds`): < 15 ms.
3. **Нагрузка на Ingress** (ручной мониторинг / Grafana, если нужен полный soak):
   - CPU OpenResty/Nginx, `nginx_connections_active`, 503/504 на балансировщике.

---

## Фаза 1: Быстрые победы на уровне Nginx (Nginx Hardening)

**Цель:** Защитить OpenResty от перегрузки CPU и памяти при HTTP-флуде за счет ограничения частоты запросов (Rate Limiting) и изменения порядка проверок в Lua (IP-фильтрация до чтения тела запроса).

### Шаг 1.1: Настройка лимитов в `nginx.conf`
Добавить зону ограничения частоты запросов (Rate Limiting) на уровне IP-адресов в секцию `http` файла `deploy/nginx/nginx.conf`:
```nginx
# Ограничение частоты запросов: 100 запросов в секунду с одного IP
limit_req_zone $binary_remote_addr zone=track_req_perip:32m rate=100r/s;
limit_req_status 429;
```
Применить ограничение в `location /track`:
```nginx
location /track {
    limit_req zone=track_req_perip burst=50 nodelay;
    limit_conn track_perip 200;
    limit_conn track_global 8192;
    
    access_by_lua_file /etc/nginx/lua/access-check.lua;
    ...
}
```

### Шаг 1.2: Изменение порядка проверок в `access-check.lua`
Разделить логику скрипта `deploy/nginx/lua/access-check.lua` на две изолированные фазы: **Phase IP** (выполняется мгновенно без чтения тела) и **Phase Body** (выполняется только для доверенных IP).

#### Код-шаблон для реорганизации `access-check.lua`:
```lua
-- 1. Сначала выполняем Phase IP (Проверки, не требующие тела запроса)
local client_ip = ngx.var.remote_addr

-- Быстрая проверка локального кэша блеклиста (shared dict)
local cached_status = blacklist_cache:get(client_ip)
if cached_status == "b" then
    ngx.exit(ngx.HTTP_FORBIDDEN)
elseif cached_status == "c" then
    -- IP доверенный, переходим к Phase Body
else
    -- Cache miss: выполняем проверку в Redis (пока синхронно, до Фазы 2)
    -- ВАЖНО: Подключаемся строго к Shard 0 (или первому доступному), 
    -- так как блеклист реплицирован глобально. Нам НЕ нужен composite_key и CRC32 здесь!
    local target_shard = shards[1] -- Фиксированный Shard 0
    
    local red = redis:new()
    red:set_timeout(100)
    local ok, err = red:connect(target_shard.host, target_shard.port)
    if ok then
        if REDIS_PASS ~= "" then red:auth(REDIS_PASS) end
        
        -- Используем Redis Pipeline для экономии RTT (один сетевой запрос вместо двух)
        red:init_pipeline()
        red:sismember("blacklist:manual", client_ip)
        red:sismember("blacklist:auto", client_ip)
        local results, err = red:commit_pipeline()
        
        local is_blocked = false
        if results then
            if results[1] == 1 or results[2] == 1 then
                is_blocked = true
            end
        end
        
        if is_blocked then
            blacklist_cache:set(client_ip, "b", 300)
            red:set_keepalive(10000, 1024)
            ngx.exit(ngx.HTTP_FORBIDDEN)
        else
            blacklist_cache:set(client_ip, "c", 300)
        end
        red:set_keepalive(10000, 1024)
    else
        -- При ошибке Redis - fail-open (пропускаем дальше), чтобы не блокировать легитимных пользователей
        ngx.log(ngx.ERR, "failed to connect to redis shard 0 for blacklist: ", err)
    end
end

-- 2. Phase Body (Выполняется только если IP прошел проверку блеклиста)
local headers = ngx.req.get_headers()
local cl = tonumber(headers["content-length"] or headers["Content-Length"])
if cl and cl > MAX_BODY_BYTES then
    ngx.exit(ngx.HTTP_REQUEST_ENTITY_TOO_LARGE)
end

-- Только теперь читаем тело запроса!
ngx.req.read_body()
local body = ngx.req.get_body_data()
-- ... далее идет парсинг тела, извлечение campaign_id, проверка edge_rl и проксирование ...
```

### Шаг 1.3: Оптимизация функций парсинга и вынос в Module Scope
Переместить вспомогательные функции, такие как `decode_varint`, `bytes_to_uuid_string` и `parse_proto`, из тела выполнения запроса на уровень модуля (в начало файла `access-check.lua`), чтобы предотвратить их повторное создание и аллокацию памяти при каждом HTTP-запросе.

---

## Фаза 2: Полный отказ от per-request Redis на Edge (Zero-RTT Blacklist)

**Цель:** Исключить сетевые запросы к Redis из горячего пути обработки запросов в Nginx. Перевести проверку блеклистов на 100% локальное чтение из оперативной памяти (`lua_shared_dict`).

### Вариант реализации 2а: Фоновый таймер синхронизации (Рекомендуемый)
Синхронизация блеклиста из Redis в локальную память Nginx по таймеру в фоновом воркере.

1. Увеличить размер `blacklist_cache` в `deploy/nginx/nginx.conf` до объема, достаточного для хранения сотен тысяч IP (например, 32MB или 64MB):
   ```nginx
   lua_shared_dict blacklist_cache 32m;
   ```
2. Создать модуль синхронизации `deploy/nginx/lua/edge-blacklist-sync.lua`:
   ```lua
   local redis = require "resty.redis"
   local _M = {}
   local cache = ngx.shared.blacklist_cache

   function _M.sync()
       local red = redis:new()
       red:set_timeout(500)
       -- Подключение к Shard 0
       local ok, err = red:connect(os.getenv("REDIS_HOST") or "127.0.0.1", tonumber(os.getenv("REDIS_PORT") or 6379))
       if not ok then
           ngx.log(ngx.WARN, "blacklist sync connect failed: ", err)
           return
       end
       if os.getenv("REDIS_PASS") and os.getenv("REDIS_PASS") ~= "" then
           red:auth(os.getenv("REDIS_PASS"))
       end

       -- Получаем все элементы блеклистов
       local manual, err1 = red:smembers("blacklist:manual")
       local auto, err2 = red:smembers("blacklist:auto")
       red:set_keepalive(10000, 8)

       if not manual or not auto then
           ngx.log(ngx.ERR, "failed to fetch blacklists from Redis: ", err1 or err2)
           return
       end

       -- Временная таблица для быстрой сверки
       local new_blocklist = {}
       for _, ip in ipairs(manual) do new_blocklist[ip] = true end
       for _, ip in ipairs(auto) do new_blocklist[ip] = true end

       -- Очищаем старые записи "b" (blocked) и записываем новые
       -- Чтобы не очищать легитимный кэш "c" (clean), используем префиксы или раздельные словари
       for _, ip in ipairs(manual) do cache:set("b:" .. ip, true) end
       for _, ip in ipairs(auto) do cache:set("b:" .. ip, true) end
       
       ngx.log(ngx.INFO, "edge blacklist synced: ", #manual + #auto, " IPs blocked")
   end

   return _M
   ```
3. Запустить периодический таймер в `deploy/nginx/lua/init-worker.lua` (выполняется только на Worker 0):
   ```lua
   local blacklist_sync = require "edge-blacklist-sync"
   local SYNC_INTERVAL = 5 -- каждые 5 секунд

   local function sync_loop(premature)
       if premature then return end
       blacklist_sync.sync()
       ngx.timer.at(SYNC_INTERVAL, sync_loop)
   end

   if ngx.worker.id() == 0 then
       ngx.timer.at(0, sync_loop)
   end
   ```
4. Упростить проверку в `access-check.lua`:
   ```lua
   -- Теперь проверка блеклиста сводится к ОДНОЙ локальной операции чтения из RAM!
   if blacklist_cache:get("b:" .. client_ip) then
       ngx.exit(ngx.HTTP_FORBIDDEN)
   end
   -- Больше никаких подключений к Redis в access-check.lua для проверки IP!
   ```

---

## Фаза 3: Настройка сквозной обсерваемости и алертинга

**Цель:** Обеспечить прозрачный мониторинг работы Edge-защиты, собирать метрики заблокированных запросов и настроить оповещения о DDoS-атаках.

### Шаг 3.1: Экспорт метрик из Lua в Prometheus
Добавить инкремент счетчиков в `access-check.lua` при блокировке запросов:
1. Объявить разделяемый словарь для метрик в `nginx.conf`:
   ```nginx
   lua_shared_dict edge_metrics 1m;
   ```
2. Внедрить запись метрик в Lua-скриптах:
   ```lua
   local metrics = ngx.shared.edge_metrics
   
   -- При блокировке по IP
   metrics:incr("blocked_ip_total", 1, 0)
   
   -- При блокировке по Rate Limit кампании
   metrics:incr("blocked_campaign_rl_total", 1, 0)
   ```
3. Экспортировать метрики через эндпоинт `/metrics` в Nginx (или через sidecar-прокси):
   ```nginx
   location /metrics/edge {
       content_by_lua_block {
           local metrics = ngx.shared.edge_metrics
           ngx.say("espx_edge_blocked_ip_total ", metrics:get("blocked_ip_total") or 0)
           ngx.say("espx_edge_blocked_campaign_rl_total ", metrics:get("blocked_campaign_rl_total") or 0)
       }
   }
   ```

### Шаг 3.2: Настройка правил алертинга в Prometheus
Добавить новые правила в `deploy/monitoring/prometheus.rules.yml`:
```yaml
groups:
  - name: espx_edge_alerts
    rules:
      # Оповещение о резком всплеске блокировок по IP (возможная атака)
      - alert: EdgeIpBlockRateHigh
        expr: sum(rate(espx_edge_blocked_ip_total[1m])) > 500
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "High volume of IP blocks at Edge"
          description: "Nginx is dropping more than 500 requests/sec via IP blacklist. Potential DDoS in progress."

      # Оповещение о рассинхронизации локального кэша блеклиста
      - alert: EdgeBlacklistSyncStale
        expr: time() - espx_edge_sync_last_success_timestamp > 60
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Edge blacklist sync is stale"
          description: "Nginx worker failed to sync blacklist from Redis for more than 60 seconds. Check Redis Shard 0 availability."
```

---

## Фаза 4: Развертывание XDP/eBPF слоя (Сетевая карта)

**Цель:** Перенести фильтрацию «грязного» трафика (SYN-флуд, атаки по высокой частоте пакетов, статические блеклисты) на уровень ядра/сетевой карты, предотвращая выделение ресурсов ОС (sk_buff) на атакующие пакеты.

### Шаг 4.1: Разработка BPF-программы (`edge_filter.c`)
Создать исходный код программы XDP в `deploy/edge-xdp/bpf/edge_filter.c`. Программа должна:
1. Пропускать все пакеты, не предназначенные для порта `:8180` (`XDP_PASS`).
2. Сверять IP источника со структурой `BPF_MAP_TYPE_LPM_TRIE` (блеклист). При совпадении — `XDP_DROP`.
3. Ограничивать частоту SYN-пакетов с одного IP с использованием `BPF_MAP_TYPE_LRU_HASH`.
4. Вести статистику сброшенных пакетов в `BPF_MAP_TYPE_PERCPU_ARRAY`.

### Шаг 4.2: Разработка демона синхронизации `edge-bpf-sync`
Создать Go-утилиту в `cmd/edge-bpf-sync/main.go`, которая:
1. Периодически (каждые 5 секунд) запрашивает блеклисты из Redis Shard 0.
2. Подключается к примонтированной BPF-карте (`/sys/fs/bpf/espx/blocklist_v4`).
3. Вычисляет разницу (diff) и инкрементально добавляет/удаляет IP-адреса в eBPF-карту, минимизируя задержки.

### Шаг 4.3: Настройка деплоя и Docker Compose
Поскольку XDP требует прямого доступа к сетевому интерфейсу хоста:
1. Добавить сервис `edge-xdp` в `docker-compose.yml` с привилегиями `privileged: true` и `network_mode: host`:
   ```yaml
   edge-xdp:
     build:
       context: .
       dockerfile: deploy/edge-xdp/Dockerfile
     privileged: true
     network_mode: host
     volumes:
       - /sys/fs/bpf:/sys/fs/bpf:rw
       - /lib/modules:/lib/modules:ro
     environment:
       - REDIS_ADDRS=127.0.0.1:6479
       - INGRESS_INTERFACE=eth0
     restart: always
   ```

---

## Фаза 5: Канареечный релиз, верификация и регламент отката

**Цель:** Безопасный запуск изменений в продакшене, контроль SLA и быстрое реагирование при возникновении проблем.

### Шаг 5.1: Схема канареечного развертывания
1. Развернуть изменения Фазы 1 и 2 на **один** из четырех узлов Ingress (если используется схема с несколькими балансировщиками).
2. Наблюдать за поведением узла в течение 48 часов.
3. Проверить отсутствие ложных срабатываний (false positives) по метрикам легитимного трафика.

### Шаг 5.2: Чек-лист верификации SLA (SLA Gates)
Перед полной раскаткой на все узлы, убедиться, что выполняются следующие условия:
- [ ] p95 задержки трекера (`ad_http_request_duration_seconds`) под нагрузкой **≤ 50 ms**.
- [ ] p99 задержки трекера под нагрузкой **≤ 80 ms**.
- [ ] Отсутствуют алерты `TrackerLatencyP99Critical` и `RedisLuaLatencyHigh`.
- [ ] Расход CPU процессом Nginx на канареечном узле снизился минимум на **20%** при фоновом шуме/атаках средней интенсивности.
- [ ] Локальный кэш блеклиста в Nginx обновляется без ошибок (`EdgeBlacklistSyncStale` не срабатывает).

### Шаг 5.3: Регламент экстренного отката (Rollback Runbook)

#### Сценарий А: Проблемы с Nginx Lua (Фазы 1-2)
Если новые проверки Lua вызывают деградацию легитимного трафика или утечки памяти:
1. Вернуть оригинальный файл `access-check.lua` из резервной копии/предыдущего коммита:
   ```bash
   git checkout HEAD~1 deploy/nginx/lua/access-check.lua
   ```
2. Выполнить мягкую перезагрузку Nginx (без разрыва соединений):
   ```bash
   docker compose exec nginx openresty -s reload
   ```

#### Сценарий Б: Проблемы с XDP/eBPF слоем (Фаза 4)
Если eBPF-программа блокирует легитимный трафик или вызывает нестабильность сетевого драйвера:
1. Мгновенно отключить XDP-программу от сетевого интерфейса (трафик пойдет напрямую в ядро и Nginx):
   ```bash
   sudo ip link set dev eth0 xdp off
   ```
2. Остановить контейнер `edge-xdp`:
   ```bash
   docker compose stop edge-xdp
   ```
3. Очистить примонтированные карты в файловой системе BPF:
   ```bash
   sudo rm -rf /sys/fs/bpf/espx
   ```
