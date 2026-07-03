# Edge hardening — final plan

Unified rollout for ingestion edge defence: **OpenResty Lua fixes**, optional **XDP/eBPF**, and alignment with **tracker latency SLA**.

**Статус:** Phase 1–2 Lua реализованы (2026-06). См. [edge-phase-validation.md](edge-phase-validation.md), [edge-parse-dfa.md](edge-parse-dfa.md), [edge-payload-modes.md](edge-payload-modes.md). XDP — в плане.

**Related docs:**

| Document | Contents |
|----------|----------|
| [edge-hardening-steps.md](edge-hardening-steps.md) | **Step-by-step implementation guide (Russian)** — exact configuration templates, code blocks, commands |
| [edge-xdp-design.md](edge-xdp-design.md) | XDP stack, NIC rings, BPF maps, XDP-specific phases |
| [architecture.md](architecture.md) | Ingress topology, Redis sharding, filter pipeline |
| `.cursorrules` | Tracker latency SLA (authoritative) |

**Code references:**

- `deploy/nginx/lua/access-check.lua` — edge gate (bottleneck)
- `deploy/nginx/lua/edge-rl.lua`, `edge-config.lua`
- `deploy/nginx/nginx.conf`
- `internal/management/nginx_worker.go` — deny-file export (exists, not wired in default nginx)

---

## 1. SLA boundaries

### 1.1 Tracker hot path (in scope for product SLA)

From `.cursorrules` and `deploy/monitoring/prometheus.rules.yml`:

| Metric | Target | Alert |
|--------|--------|-------|
| End-to-end `/track` wall time | **Hard ceiling 100 ms** | — |
| `ad_http_request_duration_seconds` p95 | **< 50 ms** | `TrackerLatencyP95Warning` |
| `ad_http_request_duration_seconds` p99 | **< 80 ms** | `TrackerLatencyP99Critical` |
| Go parse + `processTrack` + respond p99 | **< 20 ms** | (subsystem budget) |
| `unified-filter.lua` RTT p99 | **< 15 ms** | `RedisLuaLatencyHigh` (> 10 ms) |
| RTB `RunAuction` (when enabled) p99 | **< 15 µs**, 0 B/op | `RtbAuctionLatencyHigh` |

**Measurement:** `LatencyRing` → `ad_http_request_duration_seconds` on tracker `/metrics`. Client RTT and Nginx proxy time are **explicitly out of scope** for the written tracker SLA, but edge overload causes 503/504 and drops requests before they reach the histogram.

**`FILTER_TIMEOUT_MS`:** must remain **≤ 100 ms** in production (see `.env.example`). Default dev value may be higher; prod tuning must respect the ceiling.

### 1.2 Edge layer (operational SLA — this plan)

Nginx/OpenResty has no published p95/p99 product SLA. Edge work is justified when abuse threatens **tracker SLA indirectly**:

| Edge symptom | Impact on tracker SLA |
|--------------|----------------------|
| Edge Redis storm → circuit breaker OPEN | 503 at nginx; tracker idle |
| POST flood passes edge → gnet | Worker pool / Redis saturation → p99 > 80 ms |
| `limit_conn` 8192 exhausted | Legit traffic 503; no tracker samples |
| Edge `SISMEMBER` per request | Redis conn load competes with unified-filter.lua |

**Edge operational targets (proposed):**

| Metric | Target under nominal load | Under attack |
|--------|---------------------------|--------------|
| Nginx worker CPU (ingress host) | < 60% sustained | Plateau, not linear with attack RPS |
| Per-request Redis from `access-check.lua` | **0** (after Phase 2) | 0 |
| `read_body` on IP-blacklist candidate | **0** before IP path completes | 0 for blocked IPs |
| Tracker p95/p99 during soak | Unchanged vs baseline | No `TrackerLatencyP99Critical` |
| Blacklist propagation | ≤ 5 s (dict sync) or ≤ 60 s (deny files) | Emergency block via management outbox |

**Principle:** edge changes must **not** add latency to requests that reach tracker. Hardening reduces noise; tracker hot path stays unchanged.

---

## 2. Problem statement

### 2.1 Current ingress stack

```
Client → OpenResty :8180 → tracker :8181–8184 (gnet) → Redis unified-filter.lua
```

OpenResty runs `access_by_lua_file` on every `/track` request before `proxy_pass`.

### 2.2 Root cause (Lua pipeline order)

On IP blacklist **cache miss**, `access-check.lua` currently:

1. Runs circuit breaker (cheap)
2. Checks `blacklist_cache` — on miss, continues
3. **`read_body`** (up to 1 MiB)
4. Parses protobuf/JSON for `campaign_id` / `user_id`
5. Applies `edge_rl` (per-campaign)
6. Picks Redis shard via `crc32(composite_key)` — **unnecessary for IP blacklist**
7. **Redis connect + 2× `SISMEMBER`** (`blacklist:manual`, `blacklist:auto`)

Blacklist sets are **replicated on all shards** (`SyncSystemState`, management outbox). IP validation does not require body parse or shard routing.

### 2.3 Bottleneck catalog

| ID | Issue | Priority | SLA risk |
|----|-------|----------|----------|
| B1 | Body read before IP blacklist on cache miss | P0 | Redis + CPU flood → tracker starvation |
| B2 | Per-request Redis RTT (2× SISMEMBER) | P0 | Shard load, circuit breaker 503 |
| B3 | Shard pick via `composite_key` for global blacklist | P1 | Forces body parse for IP check |
| B4 | No `limit_req` — only `limit_conn` | P1 | 200 conn/IP × fast POST |
| B5 | Fail-open on Redis blacklist error | P1 | Abuse passes to tracker |
| B6 | `parse_addr_list` / Sentinel config per request | P2 | CPU on miss path |
| B7 | Helper functions redefined in per-request chunk | P2 | Lua alloc under RPS |
| B8 | `edge_rl` fail-open when shared dict full | P2 | Campaign flood to tracker |
| B9 | `NginxConfigWorker` deny files exist but unused | P2 | Duplicate infra |
| B10 | No L4 filter before userspace | P1 | SYN/PPS hits nginx backlog |

Full analysis: sections 3–4 below and [edge-xdp-design.md](edge-xdp-design.md).

---

## 3. Target architecture (final)

```
                    ┌─────────────────────────────────────────┐
  Internet          │  Ingress host (NET_MODE=host)           │
      │             │                                         │
      ▼             │  [Phase 4] XDP native (public NIC)      │
  optional CDN      │    dst :8180 only                       │
      │             │    blocklist map (synced)               │
      ▼             │    per-IP SYN + PPS, global SYN cap   │
                    │           │ PASS                        │
                    │           ▼                             │
                    │  sysctl: syncookies, somaxconn          │
                    │           │                             │
                    │           ▼                             │
                    │  OpenResty :8180                        │
                    │    limit_conn + limit_req               │
                    │    include deny files (optional)        │
                    │    access-check phase-IP (no body)      │
                    │    access-check phase-body              │
                    │      → edge_rl(campaign_id)             │
                    │           │                             │
                    │           ▼ proxy_pass                  │
                    │  gnet tracker :8181–8184                │
                    │    FILTER_TIMEOUT_MS ≤ 100ms            │
                    │    p95 < 50ms, p99 < 80ms               │
                    └─────────────────────────────────────────┘

  Sidecars:
    edge-bpf-sync     Redis blacklist → BPF map (Phase 4)
    edge-blacklist-sync  Redis → lua_shared_dict (Phase 2)
    NginxConfigWorker    Redis → deny files (Phase 2b, existing)
```

### 3.1 Responsibility split (final)

| Layer | Responsibility | Stays out of |
|-------|----------------|--------------|
| XDP | SYN/PPS flood, synced IP blocklist, global SYN cap | HTTP, campaign_id |
| `limit_req` / `limit_conn` | OOM and HTTP rate backstop | Business logic |
| Phase-IP Lua | Circuit breaker, IP blacklist (dict/deny), no body | Campaign parse |
| Phase-body Lua | Content-Length, `read_body`, proto scan, `edge_rl` | Per-request Redis blacklist |
| Tracker | Emergency breaker, fraud/geo, unified-filter.lua | Edge admission |

### 3.2 Target `access-check` pipeline

```
Phase IP (no body):
  circuit breaker
  → limit_req (nginx.conf, before Lua)
  → blacklist_cache / timer-synced dict / deny include
  → 403 or continue

Phase body (IP passed only):
  Content-Length check
  → read_body
  → partial protobuf scan (campaign_id; stop early)
  → edge_rl.allow(campaign_id)
  → proxy_pass
```

**Removed from hot path:** per-request Redis for blacklist; CRC32 shard pick on edge (unused for proxy).

---

## 4. Fix options (decision matrix)

| ID | Measure | Effort | Impact | Tracker SLA touch |
|----|---------|--------|--------|-------------------|
| F1 | Reorder: IP path before `read_body` | S | ★★★★★ | Protects indirectly |
| F2 | `limit_req_zone` per-IP | S | ★★★★ | None on legit path |
| F3 | Pipeline or eliminate per-request Redis | S–M | ★★★★ | Reduces Redis contention |
| F4 | Timer sync blacklist → `lua_shared_dict` | M | ★★★★★ | Reduces Redis load |
| F5 | Wire `NginxConfigWorker` deny includes | M | ★★★★★ | None |
| F6 | Module-level cache (shards, env parse) | S | ★★ | None |
| F7 | Move parse helpers to module scope | S | ★★ | None |
| F8 | Partial proto scan (campaign_id only) | S | ★★★ | None |
| F9 | XDP + `edge-bpf-sync` | L | ★★★ (L4) | None on PASS |
| F10 | Remove edge CRC32 shard pick | M | ★★★★ | None |

**S** = 1–2 days, **M** = 3–5 days, **L** = 1–2 weeks.

**Recommended combination:** F1 + F2 + F4 (or F5) + F8; F9 in parallel after F1–F2 soak.

---

## 5. Unified rollout phases

Phases are ordered by ROI and SLA protection. XDP details in [edge-xdp-design.md](edge-xdp-design.md).

### Phase 0 — Baseline and SLA snapshot

**Duration:** 1–2 days | **Code:** none

- [ ] Record baseline: `ad_http_request_duration_seconds` p95/p99, `ad_redis_lua_duration_seconds` p99 per shard
- [ ] Record nginx worker CPU, active connections, Redis conn count from edge
- [ ] NIC: `scripts/edge-nic-tune.sh apply` (RX ring max, IRQ/RSS)
- [ ] Sysctl: `scripts/edge-sysctl.sh apply` (`deploy/edge/99-espx-edge.conf`)
- [ ] Baseline: `scripts/edge-baseline.sh` (minimal snapshot; 24h soak optional before prod canary)
- [ ] Document ingress interface per environment
- [ ] Confirm prod `FILTER_TIMEOUT_MS ≤ 100`

**Exit:** `edge-sysctl.sh verify` OK; `var/edge-baseline/latest.txt` captured (or Grafana soak if required).

**SLA gate:** Baseline p99 < 80 ms on staging under nominal load.

---

### Phase 1 — Nginx quick wins (P0)

**Duration:** 2–3 days | **Files:** `nginx.conf`, `access-check.lua`

- [ ] **F1:** Split/reorder — IP blacklist path before `read_body`
- [ ] **F2:** `limit_req_zone` + `limit_req` on `/track` (e.g. 100 r/s, burst 50)
- [ ] **F3:** If Redis kept temporarily: pipeline `SISMEMBER` manual+auto; fixed shard 0
- [ ] **F6, F7:** Module-level caches and parse helpers
- [ ] Load test: POST flood rotating IPs — measure body read count and nginx CPU

**Exit:** Cache-miss IP flood does not call `read_body`; `limit_req` fires before Lua Redis.

**SLA gate:** Tracker p95/p99 unchanged ±5% vs Phase 0 baseline under **nominal** load (no regression from added checks on legit path).

---

### Phase 2 — Blacklist without per-request Redis (P1)

**Duration:** 3–5 days | **Choose F4 or F5 (or both)**

**Option 2a — Timer sync (F4)** — recommended default

- [ ] New `edge-blacklist-sync` in `init-worker.lua` (worker 0): `SMEMBERS blacklist:manual`, `blacklist:auto` every 5 s → `lua_shared_dict`
- [ ] Phase-IP: dict lookup only; remove per-request Redis blacklist
- [ ] Keep circuit breaker on dict/edge health, not Redis error rate from blacklist

**Option 2b — Deny files (F5)** — use existing `NginxConfigWorker`

- [ ] Set `NGINX_DENY_EXPORT_PATH`; mount in nginx container
- [ ] `include` `manual.conf` / `auto.conf` in `nginx.conf` `location /track`
- [ ] Automate reload on `reload_required.flg`
- [ ] Remove redundant Redis `SISMEMBER` from Lua

- [ ] **F10:** Remove CRC32 shard pick from edge (blacklist global; proxy is RR)
- [ ] **F8:** Partial protobuf scan for `edge_rl` only

**Exit:** Zero Redis connections from `access-check.lua` on steady state.

**SLA gate:**

- `ad_redis_lua_duration_seconds` p99 unchanged (unified-filter only)
- Blacklist block visible within 5 s (2a) or 60 s (2b) in soak test
- No `TrackerLatencyP99Critical` during soak

---

### Phase 3 — Edge observability

**Duration:** 2–3 days

- [ ] Metrics (proposed): `nginx_edge_blacklist_hit_total`, `nginx_edge_body_read_total`, `nginx_edge_rl_reject_total`
- [ ] Prometheus rules: edge circuit breaker open; nginx 503 rate on `/track`
- [ ] Grafana panel: edge vs tracker latency (correlation under load)

**Exit:** Dashboard distinguishes edge drop vs tracker saturation.

**SLA gate:** Alerts fire in drill before tracker p99 breach.

---

### Phase 4 — XDP layer (optional, L4)

**Duration:** 1–2 weeks | **See [edge-xdp-design.md](edge-xdp-design.md)**

- [ ] `deploy/edge-xdp/` BPF program (native XDP, dst `:8180`)
- [ ] `cmd/edge-bpf-sync` — Redis → BPF blocklist map
- [ ] `espx_xdp_packets_total` metrics and alerts
- [ ] Staging chaos: SYN flood, IP rotation — nginx CPU flat vs Phase 2-only

**Exit:** SYN/PPS attack absorbed before `listen` backlog; legit tracker SLA unchanged.

**SLA gate:** During L4 flood, tracker p95/p99 on **passed** traffic still < 50 ms / 80 ms.

---

### Phase 5 — Production canary and sign-off

**Duration:** gradual

- [ ] Canary ingress node (Phase 1–2, then 4 if deployed)
- [ ] 48 h compare: `TrackerLatencyP95Warning`, `RedisLuaLatencyHigh`, edge metrics
- [ ] Rollout all ingress nodes
- [ ] Update [architecture.md](architecture.md) edge section
- [ ] Run `scripts/redis-reconcile-post-deploy.sh` after deploy

**Exit:** Signed checklist; rollback documented (`ip link set dev eth0 xdp off`, nginx config revert).

---

## 6. Fail-open / fail-closed policy (final)

| Condition | Current | Target |
|-----------|---------|--------|
| Redis blacklist error | fail-open (proxy) | **fail-open** only if dict/deny stale < TTL; no per-request Redis |
| `edge_rl` dict full | fail-open | log + metric; consider fail-closed for campaign RL only |
| Edge circuit breaker (Redis errors) | 503 all | **Remove** dependency on blacklist Redis; breaker on tracker/shard health only |
| XDP blocklist miss (new IP) | N/A | PASS → Nginx phase-IP catches within sync interval |
| Emergency manual block | management outbox | dict sync 5 s + optional deny file immediate on reload |

---

## 7. Verification and CI

| Area | Command / check | Expectation |
|------|-----------------|-------------|
| Tracker SLA regression | `ad_http_request_duration_seconds` during soak | p95 < 50 ms, p99 < 80 ms |
| Redis Lua budget | `ad_redis_lua_duration_seconds` p99 | < 15 ms (< 10 ms alert) |
| Hot path alloc | `make test-alloc-gate` | No regression (edge is Lua, not Go) |
| Perf gate | `scripts/perf-gate-run.sh` on `internal/ads` | No change required for nginx-only work |
| Blacklist parity | management POST `/admin/blacklist` → edge 403 | Within sync SLA |
| Load test | POST flood rotating IPs | `nginx_edge_body_read_total` flat |

Edge Lua changes are **not** in `perf-gate.yml` paths today; add `deploy/nginx/lua/**` to workflow when implementing Phase 1+ (already listed in perf-gate for some paths).

---

## 8. Rollback

| Phase | Rollback |
|-------|----------|
| 1 | Revert `nginx.conf` + `access-check.lua` |
| 2a | Disable timer sync; restore Redis path (temporary) |
| 2b | Remove `include` deny files |
| 4 | Detach XDP program; Nginx remains authoritative |
| All | Tracker unchanged — no binary rollback needed |

---

## 9. Open questions

1. Production upstream scrubbing (CDN)? Affects XDP ROI.
2. Partner NAT allowlist CIDRs for `limit_req` / XDP PPS?
3. IPv6 on public ingress?
4. TLS on `:8180` timeline?
5. Prefer F4 (dict) vs F5 (deny files) vs both?

---

## 10. Decision log

| Date | Decision |
|------|----------|
| 2026-06 | Unified plan: Lua hardening before XDP; both protect tracker SLA indirectly |
| 2026-06 | Per-request Redis blacklist removed in Phase 2 — global sets replicated on all shards |
| 2026-06 | Edge CRC32 shard pick removed — not used for `proxy_pass` |
| 2026-06 | Tracker SLA metrics remain authoritative; edge gets operational targets only |
| 2026-06 | Phase 1 ships without waiting for XDP |

---

## 11. References

- `.cursorrules` — Tracker latency SLA
- `deploy/monitoring/prometheus.rules.yml` — `TrackerLatencyP95Warning`, `TrackerLatencyP99Critical`, `RedisLuaLatencyHigh`
- `docs/rtb-cutover.md` — RTB soak uses same tracker SLA gates
- `docs/edge-xdp-design.md` — XDP/NIC deep dive
- `internal/management/nginx_worker.go` — deny file export
