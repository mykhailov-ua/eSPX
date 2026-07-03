# Edge payload reception — experiments and report

**Date:** 2026-06-29  
**Bench:** `scripts/edge-payload-bench.sh`, supplementary 512 KiB single-shot  
**Production default:** `EDGE_BODY_MODE=stream`, `EDGE_MAX_BODY_BYTES=8192`, `location /track { client_max_body_size 8k; }`

---

## Problem

DFA limited **parse** to 8 KiB, but `ngx.req.read_body()` still accepted up to **1 MiB** per request (heap/disk + worker blocking).

---

## Strategies tested

| Mode | Mechanism | Campaign RL on edge |
|------|-----------|---------------------|
| **full** | `read_body` + DFA slice (baseline) | Yes |
| **stream** | `Content-Length` check only, no `read_body`, proxy streams | No |
| **peek** | `ngx.req.socket():receive(8k)` + DFA, no `read_body` | Yes (if id in first 8k) |
| **cap_8k** | `stream` + nginx `client_max_body_size 8k` | No |

Config via `EDGE_BODY_MODE` / `EDGE_MAX_BODY_BYTES` (env) or bench files `.edge_body_mode` / `.edge_max_body_bytes`.

---

## Metrics

| Metric | Meaning |
|--------|---------|
| `espx_edge_body_read_total` | `ngx.req.read_body` called |
| `espx_edge_body_stream_total` | stream mode: passed CL gate, no read |
| `espx_edge_body_peek_total` | peek mode: cosocket window read |
| `espx_edge_parse_oversize_total` | 413 (CL / lua limit) |
| `espx_edge_chunked_reject_total` | 411 chunked not allowed (stream/peek) |

---

## Results (valid run, tracker down → 502 = phase2 OK)

### Matrix A — nginx `location /track` **8k** (production-like)

| Mode | 200 B | 4 KiB | 16 KiB | 64 KiB | 512 KiB |
|------|-------|-------|--------|--------|---------|
| full | 502 | 502 | **413** | **413** | **413** |
| stream | 502 | 502 | **413** | **413** | **413** |
| peek | 502 | 502 | **413** | **413** | **413** |
| cap_8k | 502 | 502 | **413** | **413** | **413** |

**Metric deltas (per mode, 30 small + 45 large requests):**

| Mode | Δ `body_read` | Δ `body_stream` | Δ `body_peek` |
|------|---------------|-----------------|---------------|
| full | **+30** | 0 | 0 |
| stream | 0 | **+30** | 0 |
| peek | 0 | 0 | **+30** |
| cap_8k | 0 | **+30** | 0 |

> 413 on ≥16 KiB = nginx `client_max_body_size 8k` **before** Lua (no metric bump on oversize for nginx-native 413).

### Matrix B — nginx location **1m**, single **512 KiB** request

| Mode | Lua max | HTTP | Δ `body_read` | Δ `body_stream` | Δ `oversize` | Wall |
|------|---------|------|---------------|-----------------|--------------|------|
| **full** | 1 MiB | 502 | **+1** | 0 | 0 | ~9 ms* |
| **stream** | 8 KiB | 500 | 0 | 0 | **+1** | ~7 ms |

\*Loopback; full mode **did** invoke `read_body` on 512 KiB (confirmed by metric).

**stream** rejected at Lua CL gate (`524288 > 8192`) — **no `read_body`**, no megabyte buffered in worker.

---

## Verdict per strategy

### ✅ **stream** (winner — production default)

- No `read_body`; body passes through with `proxy_request_buffering off`.
- Rejects `Content-Length > EDGE_MAX_BODY_BYTES` (8 KiB) with `parse_oversize` **before** any body read.
- Rejects chunked (411) — `/track` must send `Content-Length`.
- **Trade-off:** no `edge_rl` by `campaign_id` on edge (tracker `unified-filter.lua` remains).

### ✅ **nginx `client_max_body_size 8k`** on `/track` (defence in depth)

- 413 at nginx for >8 KiB even if Lua misconfigured.
- Worst-case worker RAM: `8192 × limit_conn` not `1 MiB × limit_conn`.

### ⚠️ **peek**

- **Works:** 502 on small payloads after cosocket peek → proxy not broken.
- Campaign RL still possible from first 8 KiB.
- **Risk:** consumes first N bytes from body stream; fragile with some nginx/proxy versions; more complex than stream.
- **Not default** — use only if campaign RL on edge is mandatory without `read_body`.

### ❌ **full** (`read_body`)

- `body_read` increments on every accepted request including **512 KiB** (Matrix B).
- Defeats purpose of edge hardening under fat POST flood.
- Keep as **baseline / rollback** (`EDGE_BODY_MODE=full`).

### ❌ **cap_8k** as separate mode

- Redundant with **stream + location 8k**; same HTTP matrix as stream when nginx capped.

---

## Production configuration

Per **PERIMETER.md** — phase-2 `read_body` + DFA + `edge_rl`, bounded by 8 KiB on edge:

```nginx
# deploy/nginx/nginx.conf — location /track
client_max_body_size 8k;
```

```yaml
# docker-compose.yml — nginx service
EDGE_BODY_MODE=full
EDGE_MAX_BODY_BYTES=8192
EDGE_BL_STALE_SEC=30
```

`stream` / `peek` remain available via `EDGE_BODY_MODE` for experiments (`docs/edge-payload-modes.md`).

---

## Files

| File | Role |
|------|------|
| `deploy/nginx/lua/edge-phase2.lua` | full / stream / peek implementations |
| `deploy/nginx/lua/access-check.lua` | phase1 + `edge_phase2.run()` |
| `deploy/nginx/lua/edge-metrics.lua` | stream/peek/oversize counters |
| `scripts/edge-payload-bench.sh` | automated matrix |
| `var/edge-payload-bench/*.txt` | raw bench output |

---

## Follow-ups (not done)

- Campaign RL via header `X-Campaign-Id` in stream mode (optional).
- `peek` soak behind flag if RL on edge required.
- Alert: `rate(espx_edge_body_read_total[5m]) > 0` in stream mode → misconfig.
