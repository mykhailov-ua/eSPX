# Edge Layer: Ingress (L4/L7) and Operational State (Redis)

The system edge layer has two tiers: network ingress (OpenResty :8180) and hot-path operational state (Redis, 4 shards). Trackers connect these tiers via the `gnet` library and Lua script execution.

---

# Part I — Network Ingress (L4/L7)

## 0. Multi-Protocol Topology (M5)

Clients negotiate TLS ALPN (`h3` | `h2` | `http/1.1`) on **:443**. OpenResty terminates HTTP/2 and HTTP/3; upstream to gnet trackers is always **HTTP/1.1** (`proxy_http_version 1.1` + keepalive pool). Plain **:8180** remains for local dev without TLS.

| Listener | Client protocol | Upstream to tracker |
| :--- | :--- | :--- |
| `:8180` | HTTP/1.1 | HTTP/1.1 |
| `:443` | H2 / H3 / H1.1 (ALPN) | HTTP/1.1 |

Fraud signals over H2/H3: edge sets `X-Original-Method`, `X-Original-Path`, and passive `X-TLS-Hash` (ClientHello MD5 class) before proxying. Metric: `espx_edge_ingress_protocol_{h1,h2,h3}_total`.

Dev TLS certs: `deploy/nginx/certs/generate-dev-certs.sh`. HTTP/3 requires nginx ≥ 1.25 with `ngx_http_v3_module` (stock OpenResty alpine may lack QUIC — H2 on :443 still works).

## 1. Processing Pipeline (L7)

`access-check.lua` implements two phases:

### Phase 1 (before reading the request body)
*   **Rate limit.** Request rate limiting (baseline 100 r/s).
*   **Circuit breaker.** Local breaker on failures.
*   **Blacklist.** Check against blacklists from a local cache (refreshed from Redis every 5s).
*   **Connection limits.** Connection cap: 200 per IP, 8192 total.

### Phase 2 (after reading the body)
*   Data structure validation (DFA).
*   Per-campaign rate limiting (`edge-rl.lua`).
*   Proxying to the tracker pool.

## 2. Shard Selection

Routing must exactly match tracker logic (Go):
`slot = crc32_castagnoli(campaign_id) & 1023`
`shard = slot_table[slot]`

## 3. Edge Anti-Fraud
*   Blocking by IP and ASN.
*   Limits on SYN packets and open connections.
*   Per-campaign RPS limits.

---

# Part II — Redis Operational State

## 1. Topology
*   4 independent Master nodes. Redis Cluster is not used.
*   In Compose: masters + replicas + 3 Sentinel nodes.

## 2. Lua Scripts and Processing Tiers
*   **Tier B (`budget-fast.lua`).** For impressions. Minimal key set (budget debit, stream write).
*   **Tier C (`unified-filter.lua`).** For clicks. Full validation cycle: TTC, frequency (fcap), pacing, quota refill.

## 3. Global Replication
*   **Shard 0.** Stores update notifications, user lockouts, and creative structures.
*   **Replication.** Global settings (config, blacklists) are copied to all shards via `outbox`.

---

# Part III — Control Plane

## 1. Control Protocols
*   **Outbox.** Atomic delivery of changes from Postgres to Redis.
*   **UDP Quota.** RPS limits from the management service to trackers over UDP (:8190 -> :8191). Used to prevent Redis shard overload.

## 2. Isolation and Concurrency
*   **Padding (64 bytes).** Structure padding to prevent cache-line invalidation (false sharing).
*   **Lock-free.** Quota checks in the tracker use atomic operations without mutexes.

---

# Part IV — SLA and Success Criteria

## 1. Latency
*   **p95:** < 50 ms.
*   **p99:** < 80 ms.
*   **Hard ceiling:** 100 ms (forced request termination).

## 2. Data Consistency
*   Spend in Postgres must not exceed the budget limit.
*   Duplicate `click_id` values must not cause duplicate debits (idempotency).
*   Regional debits in Redis must match Postgres state accounting for sync delta.

---

# Part V — Compliance (defensive vs offensive)

eSPX implements **§1 defensive** measures only (XDP self-defense, passive TLS/TCP metadata, optional tarpit). **§2 offensive** patterns (DOM fingerprinting, hack back, port scanning) are **forbidden**.

- XDP/BPF: `cmd/edge-xdp`, `cmd/edge-bpf-sync` — not `management`.
- Passive fraud: headers + TLS hash → `DeviceFilter`; no browser spyware.
- Blocks: operator RBAC + outbox + immutable allowlist + audit.

Full matrix (Art. 361 UK, CFAA, GDPR, EU AI Act): [`GUIDE_COMPLIANCE.md`](../GUIDE_COMPLIANCE.md). Platform context: [ARCHITECTURE.md](ARCHITECTURE.md).
