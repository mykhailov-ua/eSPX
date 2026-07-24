# Edge Ingress

Nginx/OpenResty L7 ingress and coordination with tracker shard routing. L4 XDP: [EBPF.md](./EBPF.md). Compliance: [COMPLIANCE.md](./COMPLIANCE.md).

---

## Listeners

| Port | Client | Upstream to tracker |
| :--- | :--- | :--- |
| `:8180` | HTTP/1.1 | HTTP/1.1 |
| `:443` | H2 / H3 / H1.1 (ALPN) | HTTP/1.1 |

TLS certs: `deploy/nginx/certs/generate-dev-certs.sh`. HTTP/3 needs nginx ≥ 1.25 with `ngx_http_v3_module`.

H2/H3: edge sets `X-Original-Method`, `X-Original-Path`, `X-TLS-Hash` (ClientHello MD5 class). Metric: `espx_edge_ingress_protocol_{h1,h2,h3}_total`.

---

## L7 pipeline (`access-check.lua`)

**Phase 1 (pre-body):**

| Check | Limit |
| :--- | :--- |
| Rate limit | 100 r/s baseline |
| Circuit breaker | Local |
| Blacklist | Cache refresh 5 s |
| Connections | 200 / IP, 8192 total |

**Phase 2 (post-body):** body DFA (`edge-parse-dfa.lua`), per-campaign RL (`edge-rl.lua`), proxy to tracker pool.

---

## Ingress schema (M12)

`TRACKER_INGRESS_SCHEMA` must match tracker `config.IngressSchema`:

| Schema | Edge field | Tracker parser |
| :--- | :--- | :--- |
| `openrtb_3` (production default) | `request.item[0].id` | `ParseOpenRTB3Ingress` |
| `espx_native` | `campaign_id` scan | `ParseTrackRequestJSON*` / vtproto |

---

## Shard selection

Must match Go `StaticSlotSharder`:

```text
slot  = crc32_castagnoli(campaign_id) & 1023
shard = slot_table[slot]
```

Lua: `edge-slot-map.lua`, `edge-shard-balancer.lua`. Detail: [DATA.md](./DATA.md) Part I.

---

## Optional tarpit (M14-08)

When `EDGE_TARPIT_ENABLED=true`, `edge-tarpit.lua` (invoked from `access-check.lua`) slows or drops requests that exceed header count or body size thresholds before they reach the tracker. Partial closure of GAP-CMP-01. Env: `EDGE_TARPIT_MAX_HEADERS`, `EDGE_TARPIT_BODY_BYTES`, `EDGE_TARPIT_MAX_SEC` (see `.env.example`).

---

## Tracker wire parsers (optional)

| Path | Production use | Files |
| :--- | :--- | :--- |
| HTTP/1.1 FSM | Yes (edge → H1.1 upstream) | `http1_fsm.go` |
| h2c on gnet | Evaluation only | `handler_http2.go`, `http2_*.go` |
| HTTP/3 sidecar | Evaluation only | `cmd/tracker-quic` |

Benchmarks: [GO.md](./GO.md) §10, [CAPABILITIES.md](./CAPABILITIES.md) wire DFA table.
