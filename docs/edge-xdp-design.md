# Edge XDP/eBPF layer — design and rollout plan

L3/L4 packet filter **before** OpenResty on the ingestion path. This document covers stack topology, NIC/ring-buffer constraints, bottlenecks, component boundaries, and XDP-specific implementation detail.

**Canonical rollout plan (Lua + XDP + SLA):** [edge-hardening-plan.md](edge-hardening-plan.md)

**Scope:** `POST /track` ingress on `:8180`. XDP does **not** replace Nginx; it reduces volumetric and connection-level abuse before the expensive L7 Lua path. Deploy in **Phase 4** of the unified plan after Nginx hardening (Phases 0–2).

**Related:**

- Current edge: `deploy/nginx/nginx.conf`, `deploy/nginx/lua/access-check.lua`
- Architecture overview: [architecture.md](architecture.md) (Ingress, Nginx edge Lua)
- Blacklist replication: `internal/management/redis_global.go`, `scripts/redis-reconcile-post-deploy.sh`

---

## Goals and non-goals

### Goals

| Goal | Success criterion |
|------|-------------------|
| Drop SYN/PPS floods before userspace | `xdp_drop_*` absorbs attack; nginx `connections_active` stays flat |
| Early IP blocklist enforcement | Known bad IPs dropped at NIC poll; no TCP to `:8180` |
| Preserve L7 semantics in Nginx | Per-campaign RL, body parse, Redis blacklist fallback unchanged |
| Observable drops | BPF per-CPU counters exported to Prometheus; alert on spike |

### Non-goals

| Non-goal | Reason |
|----------|--------|
| Replace OpenResty | Campaign RL, protobuf parse, upstream health routing require L7 |
| L7 HTTP filtering in BPF | Fragile, duplicates Nginx; no access to `campaign_id` at L4 |
| Per-campaign rate limit in XDP | Needs parsed body; stays in `edge-rl.lua` |
| Live Redis lookups from BPF | Impossible; use userspace sync daemon → BPF maps |

---

## Current topology (baseline)

```
Client
  → OpenResty :8180  (host networking)
      access_by_lua: circuit breaker, IP blacklist cache, read_body, edge_rl, Redis SISMEMBER
      proxy_pass → trackers upstream (RR, keepalive 256)
  → gnet tracker :8181–8184  (SO_REUSEPORT, 2 event loops each)
      FilterEngine → unified-filter.lua (1 Redis RTT)
```

| Component | Config reference | Role |
|-----------|------------------|------|
| Nginx conn limits | `limit_conn` 200/IP, 8192 global | OOM backstop, not anti-DDoS |
| Nginx backlog | `listen 8180 backlog=8192` | Accept queue |
| Edge RL | `edge-rl.lua`, `edge_config.lua` | Per-campaign fixed window |
| Edge blacklist | `access-check.lua` → `blacklist:manual`, `blacklist:auto` | IP block before Go |
| Tracker | `cmd/tracker/main.go` — gnet, `WithReusePort`, `WithNumEventLoop(2)` | Hot path |

### Known weakness (drives this design)

In `access-check.lua`, on IP blacklist **cache miss**, the worker still:

1. Reads request body (up to 1 MiB)
2. Parses protobuf/JSON for `campaign_id`
3. Then queries Redis for blacklist

Under HTTP POST flood with rotating IPs, this loads Nginx CPU/RAM and Redis **before** IP rejection. XDP plus Nginx hardening (see Phase 2) address different parts of this gap.

---

## Target topology

```
Internet
  → [optional upstream scrubbing / CDN]
  → NIC (RX ring, RSS)
  → XDP native (ingress, public interface)
      filter dst_port == 8180 only
      blocklist map lookup
      per-IP SYN + PPS token buckets
      global SYN cap (IP rotation defence)
  → Linux TCP stack (syncookies, somaxconn)
  → OpenResty :8180  (unchanged responsibilities + hardening)
  → gnet trackers :8181–8184

Sidecar: edge-bpf-sync
  Redis shard 0: SMEMBERS blacklist:manual, blacklist:auto
  → incremental update BPF LPM/hash map
  → expose BPF stats on :metrics
```

### Responsibility split

| Layer | Handles | Does not handle |
|-------|---------|-----------------|
| **XDP** | SYN flood, per-IP PPS, static/synced IP blocklist, global SYN rate | HTTP, campaign_id, upstream LB |
| **Nginx** | L7 RL, body parse, live Redis blacklist, proxy, health checks | NIC-level volumetric (too late) |
| **Tracker** | Business filters, budget, fraud, `rl:ip` in unified-filter.lua | Edge connection admission |

---

## Packet path (legitimate `POST /track`)

| Step | Layer | Cost / note |
|------|-------|-------------|
| 1 | NIC RX ring | DMA into pre-registered pages |
| 2 | XDP | PASS — blocklist miss, rates OK (~sub-µs) |
| 3 | NAPI / softirq | `sk_buff` alloc |
| 4 | TCP | 3-way handshake, socket receive queue |
| 5 | listen backlog | `backlog=8192` |
| 6 | Nginx worker | epoll, read headers |
| 7 | access_by_lua | circuit breaker → IP cache → body → edge_rl → Redis |
| 8 | proxy_pass | keepalive to tracker |
| 9 | gnet | REUSEPORT dispatch, DFA HTTP parse |
| 10 | FilterEngine + Redis Lua | 1 RTT, p99 budget 15 ms |

Tracker SLA (internal): p95 < 50 ms, p99 < 80 ms end-to-end on `/track`. XDP PASS adds negligible latency; value is **fewer packets reaching step 5+**.

---

## NIC and ring-buffer constraints

### RX descriptor ring

The NIC DMA engine writes frames into driver-registered pages. The driver polls the ring via NAPI (batched, typically 64–128 frames per poll).

```
NIC ASIC ──DMA──► [desc ring: dma_addr, len, flags] ──► page frames
                        ▲
                        └── hardware tail; overflow = silent drop (before XDP)
```

Inspect and tune:

```bash
ethtool -g eth0          # ring parameters
ethtool -G eth0 rx 4096  # increase RX ring (use hardware max)
ethtool -S eth0          # rx_missed_errors, rx_no_buffer_count
```

| Symptom | Cause | Mitigation |
|---------|-------|------------|
| `rx_missed_errors` / `rx_no_buffer_count` rising | RX ring overflow | Increase ring size; RSS; reduce packets via XDP DROP |
| Single CPU 100% in `ksoftirqd` | IRQ/RSS imbalance | RSS queues, `rps_cpus`, pin XDP to NIC NUMA node |
| Drops under burst PPS | `ring_size / poll_interval` exceeded | Earlier DROP in XDP; more RX descriptors |

**Constraint:** XDP never sees packets the NIC already dropped due to ring overflow. NIC tuning is **Phase 0**, before BPF deployment.

### TX ring (if using `XDP_TX`)

Sending RST from BPF uses the TX ring. Full TX ring → drop or busy. For anti-DDoS, prefer **`XDP_DROP`** (silent) over `XDP_TX` to avoid TX pressure.

### NAPI and softirq

Even with high XDP DROP rate, the driver still **polls** the ring and runs the BPF program. At extreme PPS, softirq CPU can saturate before application limits matter. Monitor per-CPU `softnet_stat` and `ksoftirqd` usage.

### NUMA

Pin NIC IRQs and prefer XDP processing on the same NUMA node as the NIC. Cross-node DMA → cache misses under flood.

---

## XDP modes and hook placement

| Mode | When packets enter BPF | Latency | Use in eSPX |
|------|------------------------|---------|-------------|
| **Native / driver** | NAPI poll, before `sk_buff` alloc | Lowest | **Production** |
| **Generic** | After `sk_buff`, in `netif_receive_skb` | 2–5× higher | Dev/lab only |
| **Offload** | NIC firmware | Lowest CPU | Only if hardware supports BPF subset |

Attach to the **physical/public interface** (`eth0`, `ens*`), not `docker0`. Compose uses `NET_MODE=host`; OpenResty binds `0.0.0.0:8180` on the host.

### BPF program logic (spec, no code yet)

Filter only traffic destined for tracker ingress:

```
if L4 dst_port != 8180 → XDP_PASS     # do not touch Redis, management, metrics ports
if src_ip in blocklist_map → XDP_DROP
if TCP SYN && !ACK && syn_rate(src_ip) exceeded → XDP_DROP
if TCP && pkt_rate(src_ip) exceeded → XDP_DROP
if global_syn_rate exceeded → XDP_DROP
return XDP_PASS
```

**Do not parse HTTP in XDP.** Optional: drop non-SYN first fragments to `:8180` if fragmentation abuse appears (see Risks).

### BPF maps (planned)

| Map | Type | Purpose | Size (initial) |
|-----|------|---------|----------------|
| `blocklist_v4` | `LPM_TRIE` or `HASH` | IPv4 blocklist from Redis sync | 500k entries |
| `blocklist_v6` | `LPM_TRIE` | IPv6 (if dual-stack) | 100k entries |
| `ratelimit_v4` | `LRU_HASH` | Per-IP token bucket (PPS) | 1M entries |
| `syn_ratelimit_v4` | `LRU_HASH` | Per-IP SYN bucket | 500k entries |
| `global_syn` | `PERCPU_ARRAY` | Global SYN counter / window | 1 slot per CPU |
| `stats` | `PERCPU_ARRAY` | pass, drop_blocklist, drop_syn, drop_pps, drop_global | per-CPU |

Use **per-CPU maps** for counters and rate state where possible to avoid lock contention at high PPS.

### Verifier and runtime limits

- Bounded loops only; max stack 512 B
- All packet accesses need `data + off` bounds checks against `data_end`
- CO-RE (libbpf) for portability across kernel versions
- Map updates from userspace via `bpf_map_update_elem` / batch delete — no map flush-all on sync

---

## Kernel TCP limits (between XDP and Nginx)

| sysctl / setting | Recommended | Notes |
|------------------|-------------|-------|
| `net.ipv4.tcp_syncookies` | `1` | SYN flood with IP rotation |
| `net.core.somaxconn` | `≥ 16384` | Must be ≥ nginx `backlog` |
| `net.ipv4.tcp_max_syn_backlog` | `8192`+ | Half-open queue |
| `net.ipv4.ip_local_port_range` | wide enough | Nginx → 4 trackers, keepalive 256 |

**Distributed attack pattern:** many unique IPs, each below per-IP XDP threshold → global SYN table and `limit_conn 8192` still exhaust. **Global SYN cap in XDP** is required alongside per-IP limits.

---

## Bottleneck matrix

| # | Bottleneck | Layer | First symptom under flood | XDP helps? |
|---|------------|-------|---------------------------|------------|
| 1 | RX ring overflow | NIC | `rx_missed_errors` | Indirectly (fewer completed frames if DROP early) |
| 2 | softirq saturation | Kernel | `ksoftirqd` 100% | Partially |
| 3 | Global embryonic SYNs | Kernel TCP | `SYN_RECV` spike | **Yes** (global SYN cap) |
| 4 | Per-IP flood to userspace | XDP/Nginx | CPU nginx | **Yes** |
| 5 | listen backlog 8192 | Nginx/kernel | accept latency | Indirectly |
| 6 | `limit_conn` 8192 global | Nginx | 503 for all | No |
| 7 | `read_body` + Redis on cache miss | Nginx Lua | CPU, Redis RTT | **No** — Nginx hardening |
| 8 | `edge_rl` shared dict pressure | Nginx | LRU eviction, false negatives | No |
| 9 | Redis unified-filter | Tracker | p99 latency, breaker open | No |
| 10 | gnet worker pool ring full | Tracker | ingest degradation | No |

---

## Userspace components (planned, no code yet)

### 1. `edge-xdp` — BPF object + loader

- Location (proposed): `deploy/edge-xdp/` or `cmd/edge-xdp/`
- Loads CO-RE object, attaches XDP to configured interface
- Pins maps under `/sys/fs/bpf/espx/`
- Graceful detach on SIGTERM
- Flags: `--iface`, `--pin-path`, `--dry-run`

### 2. `edge-bpf-sync` — blacklist and config daemon

- Location (proposed): `cmd/edge-bpf-sync/`
- Reads from Redis shard 0 (same as `edge-config.lua`):
  - `SMEMBERS blacklist:manual`
  - `SMEMBERS blacklist:auto`
- Incremental diff → `bpf_map_update_elem` / `bpf_map_delete_elem`
- Sync interval: **5 s** default (align with `edge-config.lua` poll)
- On Redis failure: keep last-good map; log warning; do not flush blocklist
- Optional: push manual allowlist overrides (partner NAT IPs)

### 3. Observability

Export BPF `stats` map as Prometheus metrics:

| Metric | Type | Labels |
|--------|------|--------|
| `espx_xdp_packets_total` | counter | `action=pass\|drop`, `reason=blocklist\|syn\|pps\|global\|fragment` |
| `espx_xdp_blocklist_entries` | gauge | `family=v4\|v6` |
| `espx_xdp_sync_last_success_timestamp` | gauge | — |
| `espx_xdp_sync_errors_total` | counter | `op=redis\|bpf_update` |

Correlate with existing:

- `ad_http_requests_total` (tracker)
- nginx stub_status or `connections_active` if enabled
- `ad_circuit_breaker_state` (Redis pressure from edge Lua)

### 4. Alerting rules (proposed additions to `deploy/monitoring/prometheus.rules.yml`)

| Alert | Expr (sketch) | Severity |
|-------|---------------|----------|
| `XdpDropRateHigh` | `rate(espx_xdp_packets_total{action="drop"}[1m]) > threshold` | warning |
| `XdpSyncStale` | `time() - espx_xdp_sync_last_success_timestamp > 60` | warning |
| `XdpPassRateNearZero` | legit traffic accidentally blocked | critical |

---

## Nginx hardening (parallel track, no XDP dependency)

These changes stay in `deploy/nginx/` and should ship **before or with** XDP Phase 2.

| Change | File | Effect |
|--------|------|--------|
| IP blacklist cache check **before** `read_body` | `access-check.lua` | Cheap reject on cached bad IPs |
| `limit_req_zone` per-IP on `/track` | `nginx.conf` | HTTP request rate before Lua |
| Reorder: `limit_req` → IP cache → Content-Length → body | `access-check.lua` | Reduce body alloc under flood |
| Document fail-open on Redis blacklist miss | `access-check.lua` | Operational awareness |

Per-campaign RL and Redis authoritative blacklist remain after body parse (requires `campaign_id`).

---

## Initial rate-limit parameters (tunable)

| Parameter | Start value | Notes |
|-----------|-------------|-------|
| Per-IP SYN rate | 50/s | Watch carrier-grade NAT; allowlist partners |
| Per-IP TCP PPS to `:8180` | 2000/s | Below legit burst peaks per partner |
| Global SYN rate | 50k/s | IP rotation defence |
| Blocklist LRU max | 500k IPv4 | Eviction → false PASS until Nginx catches |
| Sync interval | 5 s | Match edge-config poll; max staleness for new blocks |

Tune under load test; document overrides in `.env` / systemd unit.

---

## Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Carrier-grade NAT | Legit users share IP; per-IP limits too aggressive | Soft limits, allowlist map, higher PPS for known prefixes |
| Blocklist staleness (5–30 s) | New block not in BPF yet | Nginx Redis check remains authoritative |
| IP fragmentation | Incomplete L4 header in non-first fragments | Drop fragments to `:8180` or assemble in driver if abused |
| IPv6 dual-stack | Split maps and logic | Phase 4 if needed |
| Wrong interface attach | Filter misses traffic or breaks SSH | Explicit `--iface`, dry-run, canary host |
| Kernel upgrade | BPF CO-RE breakage | Pin kernel in prod; CI test on target kernel |
| XDP + TLS termination later | Encrypted payload invisible to L7 at XDP | Per-IP PPS still works on `:443`; L7 stays in Nginx |

---

## Phased rollout plan

> **Unified phases (0–5) with SLA gates:** [edge-hardening-plan.md](edge-hardening-plan.md).  
> Below: XDP-specific sub-phases (map to hardening plan Phase 4).

### Phase 0 — Baseline and NIC tuning (no BPF)

**Duration:** 1–2 days

- [ ] Document public ingress interface name per environment
- [ ] Set RX ring to hardware maximum (`ethtool -G`)
- [ ] Enable RSS; verify IRQ spread across CPUs
- [ ] Enable `tcp_syncookies`, review `somaxconn` / `tcp_max_syn_backlog`
- [ ] Baseline metrics: nginx connections, tracker `ad_http_requests_total`, Redis error rate
- [ ] Optional: `nftables hashlimit` on `:8180` as interim L4 throttle

**Exit criteria:** Documented NIC stats under normal load; sysctl applied in prod runbook.

---

### Phase 1 — Nginx hardening

> Covered in [edge-hardening-plan.md](edge-hardening-plan.md) Phases 1–2 (Lua pipeline, blacklist sync). Required before XDP production value for L7 POST flood.

**Duration:** 2–3 days

- [ ] Reorder `access-check.lua` checks (IP before body)
- [ ] Add `limit_req` per-IP on `/track`
- [ ] Load test: measure CPU/nginx under synthetic POST flood with rotating IPs
- [ ] Update [architecture.md](architecture.md) edge section if behaviour changes

**Exit criteria:** Cache-miss flood no longer triggers body read before IP rejection path; `limit_req` returns 503/429 before Lua Redis call.

---

### Phase 2 — XDP development (lab)

**Duration:** 1–2 weeks

- [ ] Create `deploy/edge-xdp/` layout (BPF C, libbpf skeleton, Makefile target)
- [ ] Implement native XDP: dst `:8180` filter, blocklist, SYN/PPS, global SYN, stats maps
- [ ] Loader attaches/detaches cleanly; pin maps
- [ ] Unit-style tests: BPF bytecode loads on CI kernel; pkt tests with `xdp-tools` or libbpf harness
- [ ] Lab flood: `hping3` / `wrk` — verify drops in `stats` map before nginx saturation

**Exit criteria:** Lab host survives SYN flood that previously filled nginx backlog; `espx_xdp_packets_total` reflects drops.

---

### Phase 3 — `edge-bpf-sync` daemon

**Duration:** 3–5 days

- [ ] Implement Redis → BPF blocklist sync (shard 0, `blacklist:manual` + `blacklist:auto`)
- [ ] Incremental updates; last-good on Redis failure
- [ ] Prometheus metrics endpoint
- [ ] Integration test: add IP to blacklist in management → appears in BPF map within sync interval

**Exit criteria:** Blocklist parity test passes; sync staleness alert fires on simulated Redis outage.

---

### Phase 4 — Staging soak

**Duration:** 7–14 days

- [ ] Deploy XDP + sync on staging ingress (host network, same topology as prod)
- [ ] Enable Prometheus alerts
- [ ] Chaos: SYN flood, POST flood, IP rotation — compare nginx/tracker saturation with/without XDP bypass (feature flag detach)
- [ ] Verify no regression on tracker p95/p99 (`ad_http_request_duration_seconds`)

**Exit criteria:** No false-positive drop rate on legit traffic; attack scenarios show measurable nginx CPU reduction.

---

### Phase 5 — Production canary

**Duration:** gradual

- [ ] Single ingress node canary with XDP attached
- [ ] Compare metrics to non-XDP peers for 48 h
- [ ] Roll out to all ingress nodes
- [ ] Document rollback: `ip link set dev eth0 xdp off` or loader detach

**Exit criteria:** All ingress nodes on XDP; runbook in [development.md](development.md).

---

## Directory layout (proposed)

```
deploy/edge-xdp/
  README.md              # ops quick start, ethtool, attach/detach
  bpf/
    edge_filter.c        # XDP program (Phase 2)
  include/               # vmlinux.h / headers
  Makefile               # clang -target bpf, libbpf skeleton

cmd/edge-bpf-sync/
  main.go                # Redis sync daemon (Phase 3)

cmd/edge-xdp/
  main.go                # loader / supervisor (Phase 2)

deploy/monitoring/prometheus.rules.yml   # XdpDropRateHigh, etc. (Phase 4)
```

No code until Phase 2 kickoff; this layout is planning only.

---

## Decision log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-06 | XDP before Nginx, not replacement | L7 campaign RL and Redis blacklist need OpenResty |
| 2026-06 | Native XDP in prod | Generic XDP allocates `sk_buff` before filter |
| 2026-06 | `XDP_DROP` over `XDP_TX` | Avoid TX ring pressure |
| 2026-06 | Blacklist sync from Redis, not live BPF Redis | BPF cannot call Redis; 5 s staleness acceptable with Nginx fallback |
| 2026-06 | Nginx hardening in parallel | XDP does not fix body-read-on-cache-miss |
| 2026-06 | Filter `dst_port == 8180` only | Preserve management, metrics, Redis ports on host |

---

## Open questions

1. **Upstream scrubbing:** Is there Cloudflare / provider anti-DDoS in production? If yes, XDP ROI is lower for volumetric; still useful for SYN to origin.
2. **Ingress count:** Single host vs multiple ingress nodes — affects canary strategy and global SYN cap calibration.
3. **Partner allowlist:** Static CIDR list for known high-NAT partners?
4. **IPv6:** Is public ingress v6-enabled today?
5. **HTTPS:** Timeline for TLS on `:8180` — affects long-term edge design, not Phase 2 PPS filtering.

---

## References

- Nginx edge: `deploy/nginx/nginx.conf`, `deploy/nginx/lua/access-check.lua`
- Blacklist write path: `internal/management/redis_global.go`
- Tracker gnet: `cmd/tracker/main.go`
- Internal tracker SLA: `.cursorrules` (p95/p99, Redis Lua budget)
- Linux XDP: [https://www.kernel.org/doc/html/latest/networking/af_xdp.html](https://www.kernel.org/doc/html/latest/networking/af_xdp.html)
- libbpf CO-RE: [https://nakov.com/blog/2021/04/15/linux-ebpf-libbpf-bootstrap/](https://nakov.com/blog/2021/04/15/linux-ebpf-libbpf-bootstrap/)
