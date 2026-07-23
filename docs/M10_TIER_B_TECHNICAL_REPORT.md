# M10 Tier B — Technical Report (SYN-Flood Resilience)

**Date:** 2026-07-22  
**Scope:** M10-B1, M10-B3, M10-B4 (B2 deferred)  
**Depends on:** [M10 Tier A report](./M10_TIER_A_TECHNICAL_REPORT.md)

---

## 1. Summary

| ID | Status | Notes |
| :--- | :--- | :--- |
| M10-B1 | Done | Tail-call `xdp_syn_cookie` + `bpf_tcp_gen_syncookie_ipv4`; off unless `XDP_SYN_COOKIE=1` |
| M10-B2 | Deferred | Spoof block — legal review required |
| M10-B3 | Done | Ringbuf `violations` → `edge-bpf-sync` → `blacklist:auto` + TTL ZSET |
| M10-B4 | Done | `syn_subnet_ratelimit_v4` LRU, `/24` key, default 256/s |

---

## 2. Architecture

```text
SYN to :8180
  → global SYN cap
  → /24 SYN cap (B4)
  → per-host SYN cap
  → on limit + cookie enabled: tail-call xdp_syn_cookie (B1)
  → on limit + cookie off: ringbuf violation + DROP (B3)
PPS limit exceeded → ringbuf violation + DROP (B3)

edge-bpf-sync (250ms poll)
  → drain violations ringbuf
  → SADD blacklist:auto + ZADD blacklist:auto:ttl
  → immediate LPM sync (within SYNC_INTERVAL)
```

### New maps

| Map | Type | Role |
| :--- | :--- | :--- |
| `syn_subnet_ratelimit_v4` | LRU_HASH | Per-/24 SYN window |
| `violations` | RINGBUF | Cold-path autoban events |
| `prog_array` | PROG_ARRAY | Tail call to `xdp_syn_cookie` |

### New stats

| Index | Label |
| :--- | :--- |
| `XDP_STAT_DROP_SYN_SUBNET` | `syn_subnet` |
| `XDP_STAT_SYN_COOKIE` | `syn_cookie` (pass) |

### Config extensions (`edge_config`)

| Field | Default |
| :--- | :--- |
| `syn_subnet_limit` | 256/s per /24 |
| `syn_cookie_enabled` | 0 (set via `XDP_SYN_COOKIE=1` in `edge-xdp`) |

---

## 3. Functional tests

```
go test ./internal/edge/bpf/... -count=1
ok   espx/internal/edge/bpf   1.186s

go test ./internal/edge/blocklist/... -run TestActiveAutoBans -count=1
ok   espx/internal/edge/blocklist   0.004s
```

| Test | Asserts |
| :--- | :--- |
| `TestXDP_dropSynSubnetFlood` | /24 cap at 4 → DROP + stat |
| `TestXDP_subnetCapIndependentPerHost` | Exhaust /24 blocks same subnet, not adjacent /24 |
| `TestXDP_ringbufViolationOnSynDrop` | Ringbuf event on per-host SYN exceed |
| `TestXDP_synCookieDisabledByDefault` | No cookie stat when flag off |
| `TestXDP_synCookiePathWhenEnabled` | Cookie stat or DROP if helper unavailable |
| `TestChaos_XDPSynFloodSynthetic` | 400 hosts × 80 SYN; ~99% drop; control ACK stable; ringbuf violations |
| `TestChaos_XDPAutobanPipelineSynthetic` | SYN violation → ringbuf drain → handler callback |
| `TestActiveAutoBans_expiredLeaseRemoved` | TTL ZSET prunes expired autoban |

---

## 4. Benchmarks (userspace `prog.Test` / `prog.Run`)

**Host:** Docker privileged, golang:1.25.12-bookworm

| Benchmark | ns/op | B/op | allocs/op | Notes |
| :--- | ---: | ---: | ---: | :--- |
| `BenchmarkXDP_passSYN` (`Test`) | ~1035 | 320 | **1** | Go `make([]byte)` in `prog.Test` |
| `BenchmarkXDP_passSYN_run` (`Run`) | ~898 | 0 | **0** | Reused `DataOut` buffer |
| `BenchmarkXDP_dropBlocklist` | ~966 | 320 | 1 | LPM lookup + DROP |
| `BenchmarkXDP_dropAnomaly` | ~954 | 320 | 1 | Early drop, no LRU |
| `BenchmarkXDP_passPPSACK` | ~1033 | 320 | 1 | LRU update path |
| `BenchmarkXDP_dropNonTCP` | ~942 | 320 | 1 | A3 non-TCP on :8180 |

### Allocation overhead (unchanged from Tier A)

The **1 alloc/op** is entirely from cilium/ebpf `Program.Test()`:

```go
out = make([]byte, len(in)+outputPad)  // outputPad=258
```

BPF bytecode has **zero heap**. Use `BenchmarkXDP_passSYN_run` or `prog.Run` with pre-sized `DataOut` for alloc-free measurement.

### ns/op overhead vs Tier A

SYN pass path adds **~0–3%** ns/op (within noise) from:

- `config` map fields (`syn_subnet_limit`, `syn_cookie_enabled`) — 2 extra u32 reads
- `/24 SYN check — one extra LRU lookup+update on SYN packets
- No ringbuf on pass path (B3 compliance)

Tier B hot path does **not** add ringbuf or tail-call on clean SYN allow.

---

## 5. Verifier

Program loads via cilium/ebpf on Linux 6.x test harness. Two XDP programs:

| Program | SEC | Helpers |
| :--- | :--- | :--- |
| `xdp_edge_filter` | main | map_lookup, map_update, ktime_get_ns, ringbuf_reserve/submit, tail_call |
| `xdp_syn_cookie` | tail-call | bpf_tcp_gen_syncookie_ipv4 (163), stat_inc |

`bpf_tcp_gen_syncookie_ipv4` requires kernel ≥ 6.0; test `TestXDP_synCookiePathWhenEnabled` tolerates helper absence in `BPF_PROG_RUN` sandbox.

---

## 6. Synthetic chaos (`chaos_proof`)

**Harness:** `TestChaos_XDPSynFloodSynthetic` — privileged Docker, `BPF_PROG_TEST_RUN`, no live NIC.

```
chaos_proof fault=xdp_syn_flood harness=bpf_prog_test attack_hosts=400 syns_per_host=80 \
  attack_pass=416 attack_drop=31584 drop_ratio=0.987 drop_syn_delta=96 \
  drop_subnet_delta=20105 violations=140 control_stable=true
```

| Metric | Value |
| :--- | ---: |
| Attack packets | 32,000 (400 × 80 SYN) |
| Attack DROP | 31,584 (98.7%) |
| Attack PASS | 416 (first SYNs within per-IP /24 budgets) |
| Control ACK (pre+post flood) | 30/30 PASS |
| Ringbuf violations (unique src) | 140 |

```
chaos_proof fault=xdp_autoban_pipeline harness=ringbuf_drain violations=1 syn_events=1 pps_events=0
```

**Test count:** 23 BPF tests + 2 chaos tests — all PASS (`go test ./internal/edge/bpf/... -short=false`).

---

## 7. Compliance

| Gate | Result |
| :--- | :--- |
| SYN cookie off by default | `syn_cookie_enabled=0`; `XDP_SYN_COOKIE` env gates enable |
| Ringbuf handler | `edge-bpf-sync` only writes Redis — no outbound socket to offender |
| Autoban in LPM | Immediate sync after violation drain + periodic `SYNC_INTERVAL` |
| B2 spoof block | Not implemented (deferred) |

---

## 8. Tier B exit criteria

| Criterion | Status |
| :--- | :--- |
| SYN cookie path off by default | Done |
| Chaos proof `xdp_syn_flood` | Done (synthetic harness; staging accept-queue optional) |
| Ringbuf handler no outbound strike | Done |
| Autoban visible in LPM within `SYNC_INTERVAL` | Done (immediate sync on violation) |

---

## 9. Files changed

| File | Change |
| :--- | :--- |
| `deploy/edge/xdp/bpf/edge_filter.c` | B1/B3/B4 logic |
| `internal/edge/bpf/{violations,violations_handler,config,stats}.go` | Userspace helpers |
| `internal/edge/blocklist/{autoban,sync}.go` | TTL autoban + active filter |
| `cmd/edge-xdp/{main,attach,prog_array}.go` | Pin maps, wire tail call |
| `cmd/edge-bpf-sync/main.go` | Ringbuf drain + autoban |
| `internal/edge/bpf/edge_filter_tier_b_test.go` | Tier B tests |
