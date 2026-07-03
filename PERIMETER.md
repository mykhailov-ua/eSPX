# PERIMETER — ingress defence (OpenResty + XDP)

Защита периметра до tracker hot path. Дополняет [ANTIFRAUD.md](ANTIFRAUD.md); не заменяет фильтры в Go/Lua.

## SHIPPED — Edge Lua (2026-06)

Двухфазная валидация на `/track`:

| Фаза | Содержание | `read_body` |
| :--- | :--- | :--- |
| 1 | IP cache, circuit breaker, `limit_req` | Нет → `403/503` |
| 2 | `read_body`, DFA-parse, `edge_rl` | Да |

Production: `EDGE_BODY_MODE=full`, `client_max_body_size 8k`, timer-sync blacklist, `/metrics/edge`.

Устранено: `read_body` до blacklist; per-request Redis `SISMEMBER`; отсутствие `limit_req`.

Детали: `docs/edge-phase-validation.md`, `docs/edge-payload-modes.md`, `docs/edge-parse-dfa.md`, `docs/edge-hardening-plan.md`.

### Edge metrics

`espx_edge_phase1_pass_total`, `espx_edge_phase2_pass_total`, `espx_edge_body_read_total`, `espx_edge_blocked_ip_total`, `espx_edge_blocked_campaign_rl_total`, `espx_edge_blocked_fraud_tier_total`.

### Edge anti-fraud (2026-07)

- Fraud-tier campaign RL по `X-Fraud-Score`; 429/403 + `Retry-After` (`edge-rl.lua`, `edge-phase2.lua`).
- CDN/mobile ASN whitelist в `config:values` → `edge-config.lua` / `edge-asn.lua`; bypass phase-1 blacklist при `X-Client-ASN`.
- Pub/Sub `fraud:quarantine` → немедленный flush `blacklist_cache` (`edge-quarantine-sub.lua`, `init-worker.lua`); sync включает `blacklist:fraud`.
- TLS JA3/JA4 → `X-TLS-Hash` → tracker `DeviceFilter` (`internal/ads/device_filter.go`).
- Stream mode: campaign RL via `X-Campaign-Id`; alert `EdgeStreamModeBodyRead` при `rate(espx_edge_body_read_total[5m]) > 0`.

## TODO — Edge (anti-fraud integration)

_Все пункты ниже реализованы; см. § Edge anti-fraud (2026-07)._

- ~~Rate limit tiers по `fraud_score`; 429 + `Retry-After`.~~
- ~~CDN и mobile ASN whitelist в `config:values` (global replicate).~~
- ~~Pub/Sub `fraud:quarantine` → flush `lua_shared_dict` blacklist (`init-worker.lua`).~~
- ~~TLS JA3/JA4 → header `X-TLS-Hash` → tracker `DeviceFilter`.~~
- ~~Follow-up: campaign RL via `X-Campaign-Id` в stream mode; alert `rate(espx_edge_body_read_total[5m]) > 0` в stream mode.~~

## SHIPPED — Edge ops rollout (2026-07)

Phase 0–3, 5 из `docs/edge-hardening-plan.md`:

| Phase | Артефакты |
| :--- | :--- |
| 0 | `scripts/edge-phase0.sh`, `edge-sysctl.sh`, `edge-nic-tune.sh`, `edge-baseline.sh`, `verify-prod-tuning.sh`, `.env.prod.example` |
| 3 | `deploy/monitoring/grafana/provisioning/dashboards/edge.json`, alerts `EdgeCircuitBreakerRejectHigh`, `EdgeTrack503RateHigh` |
| 5 | `scripts/edge-rollout.sh` (canary 48h → sign-off → `redis-reconcile-post-deploy.sh`) |

**Prod:** `ENV=production` требует `FILTER_TIMEOUT_MS ≤ 100` (`internal/config/env.go`).

**Runbook:**

```bash
bash scripts/edge-phase0.sh .env.prod        # preflight
bash scripts/edge-rollout.sh canary-start      # before traffic shift
# ... 48h soak on canary ingress ...
bash scripts/edge-rollout.sh canary-signoff
bash scripts/edge-rollout.sh post-deploy       # full rollout + reconcile
```

Acceptance: `edge-baseline.sh verify` — tracker p95 ≤50 ms, p99 ≤80 ms; nginx CPU −20% на канареечном узле — сравнение `canary-start.txt` vs `canary-end.txt` + host metrics вручную.

## TODO — Edge (ops rollout)

_Реализовано; см. § SHIPPED — Edge ops rollout (2026-07)._

- ~~Phase 0: baseline metrics, NIC/sysctl tune, `FILTER_TIMEOUT_MS ≤ 100` в prod.~~
- ~~Phase 3: Grafana panels, Prometheus rules (edge circuit breaker, nginx 503).~~
- ~~Phase 5: canary ingress 48h → full rollout → `scripts/redis-reconcile-post-deploy.sh`.~~

## XDP / eBPF

L4-фильтр грубой очистки. Код: `deploy/edge-xdp/bpf/edge_filter.c`, `cmd/edge-xdp`, `cmd/edge-bpf-sync`, `internal/edge/blocklist/`, `internal/edge/allowlist/`.

Дизайн: `docs/edge-xdp-design.md`.

**Без XDP:** SYN/PPS flood доходит до nginx backlog (8192).

### P0 — критическая защита

- [ ] Sync `blacklist:manual` / `blacklist:auto` / `blacklist:fraud` → `blocklist_v4` (DROP до Nginx)
- [ ] PPS limit per IP — `ratelimit_v4`, ~2000 PPS, порт 8180
- [ ] Global SYN limit — `global_syn` per-CPU, ~50k SYN/s

### P1 — операционный контроль

- [ ] Allowlist `allow_v4` LPM — bypass для партнёрских NAT (код sync есть)
- [ ] Bogon DROP — RFC1918, link-local, multicast
- [ ] Dynamic limits — SYN_LIMIT, PPS_LIMIT в BPF config map

### P2 — IP intelligence

- [ ] Hosting CIDR — MaxMind Anonymous IP → `hosting_v4`
- [ ] Tor exit nodes → BPF map
- [ ] TCP anomaly DROP — SYN+FIN, Xmas, nmap signatures

### P3 — автоматизация

- [ ] Fraud stream → `blacklist:auto` → BPF sync (~5 s)
- [ ] IPv6 — `blocklist_v6`, `syn_ratelimit_v6`

### XDP ops rollout

- [ ] Staging: attach XDP, chaos (SYN flood, IP rotation)
- [ ] Canary ingress 48h vs peers
- [ ] Rollout all nodes; rollback: `ip link set dev eth0 xdp off`

| Компонент | BPF map | Источник | Действие |
| :--- | :--- | :--- | :--- |
| L3 Deny | `blocklist_v4` | Redis manual/auto/fraud | DROP |
| Flood | `ratelimit_v4` | BPF dynamic | DROP |
| Hosting/DC | `hosting_v4` | MaxMind export | THROTTLE |
| Bogon | static | RFC | DROP |
| Allowlist | `allow_v4` | config/partners | PASS |

## FILES

`deploy/nginx/lua/access-check.lua`, `edge-phase2.lua`, `edge-parse-dfa.lua`, `edge-rl.lua`, `edge-blacklist-sync.lua`, `deploy/nginx/nginx.conf`, `deploy/edge-xdp/`, `cmd/edge-bpf-sync/`, `cmd/edge-xdp/`
