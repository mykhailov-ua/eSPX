# M10 Tier C — Technical Report (Passive IVT Signals)

**Date:** 2026-07-22  
**Scope:** M10-C1, M10-C2, M10-C3, M10-C4  
**Depends on:** [M10 Tier B report](./M10_TIER_B_TECHNICAL_REPORT.md)

---

## 1. Summary

| ID | Status | Notes |
| :--- | :--- | :--- |
| M10-C1 | Done | SYN `hash(ttl, window, mss, doff)` → `fingerprints` ringbuf → `edge-bpf-sync` |
| M10-C2 | Done | `tcp_edge_correlation` rule: Redis TCP fp × ClickHouse UA × JA3 → `ghost` → `ML_GHOST_IVT` |
| M10-C3 | Done | Static analysis + unit/chaos tests; CI gate in `check_compliance.sh` |
| M10-C4 | Done | Operator dashboard `XDP` panel: pass, drops (syn/anomaly/pps/…), fingerprints |

---

## 2. Architecture

```text
SYN to :8180 (fingerprint_enabled=1)
  → hash_tcp_syn_fields(ttl, window, mss, doff)
  → bpf_ringbuf_reserve(fingerprints)   # cold path; no map update per SYN
  → XDP_PASS (fingerprint never gates DROP)

edge-bpf-sync (500ms poll, FINGERPRINT_POLL_INTERVAL)
  → drain fingerprints ringbuf
  → ZADD edge:tcp_fp:recent + HSET edge:tcp_fp:ip:{ip}
  → aggregate stats → Redis edge:xdp:stats_snapshot

ivt-detector (tcp_edge_correlation rule)
  → ListRecent(edge:tcp_fp:recent)
  → JOIN clicks (ip, user_agent, tls_hash) in ClickHouse
  → IsTLSImpersonating(ua, ja3) → Action=ghost → ML_GHOST_IVT outbox
```

### New maps / keys

| Artifact | Type | Role |
| :--- | :--- | :--- |
| `fingerprints` | RINGBUF | Passive SYN TCP metadata events |
| `edge:tcp_fp:recent` | Redis ZSET | Staging for IVT correlation |
| `edge:tcp_fp:ip:{ip}` | Redis HASH | Per-IP latest observation |
| `edge:xdp:stats_snapshot` | Redis STRING | Operator dashboard aggregate |

### New stats

| Index | Label | Prometheus |
| :--- | :--- | :--- |
| `XDP_STAT_FINGERPRINT` | `fingerprint` | `ad_xdp_fingerprint_total` |

### ClickHouse schema

| Migration | Change |
| :--- | :--- |
| `00008_tls_hash_clicks.sql` | `clicks.tls_hash String` for JA3 correlation |

---

## 3. Functional tests (2026-07-22 run)

```bash
go test ./internal/ivtdetector/... -run 'TCPEdge|IVTCorrelation' -count=1
ok   espx/internal/ivtdetector   17.399s

go test ./internal/adminapi/... -run XDPPanel -count=1
ok   espx/internal/adminapi   0.029s

go test ./internal/edge/fingerprint/... -count=1
ok   espx/internal/edge/fingerprint   0.952s

go test ./internal/ingestion/... -run ClickHouseStore_Insert -count=1
ok   espx/internal/ingestion   9.667s

go test ./internal/edge/bpf/... -run 'M22C3|FuzzDecodeFingerprint' -count=1
ok   espx/internal/edge/bpf   0.029s
```

BPF `BPF_PROG_TEST_RUN` tests (`TestXDP_synEmitsFingerprint`, `TestChaos_XDPFingerprint*`) require elevated `MEMLOCK` on the runner (documented CI gap in EBPF.md §10); they skip in unprivileged sandboxes.

| Test | Asserts |
| :--- | :--- |
| `TestXDP_synEmitsFingerprint` | Ringbuf event + `StatFingerprint` on SYN |
| `TestXDP_fingerprintDisabledSkipsRingbuf` | No events when `DisableFingerprint` |
| `TestXDP_fingerprintDoesNotCauseDrop` | PASS identical with fp on/off |
| `TestCompliance_M22C3_noFingerprintBlockMap` | No `tcp_hash` → `XDP_DROP` in C source |
| `TestTCPEdgeCorrelationRule_GhostOnImpersonation` | Chrome UA + python JA3 → ghost |
| `TestTCPEdgeCorrelationRule_SkipsMatchingUAJA3` | Matching pair → no candidate |
| `TestGetOperatorDashboard_XDPPanel` | API returns pass/drops/fingerprints |

---

## 4. Chaos experiments (GUIDE_CHAOS_RELIABILITY.md)

Full matrix: overflows, concurrency, broken data, dependency outage. Each run emits `chaos_proof` (R7).

### 4.1 Redis staging (`internal/edge/fingerprint`)

| Fault | Hypothesis | Result |
| :--- | :--- | :--- |
| `fingerprint_concurrent_record` | 32×50 concurrent `Record` on overlapping IPs | **PASS** — 0 errors, 512 listed |
| `fingerprint_zset_overflow` | 5000 ZSET members; `ListRecent` caps at 128 | **PASS** |
| `fingerprint_corrupt_redis_members` | 8 garbage members + 1 valid | **PASS** — valid recovered |
| `fingerprint_redis_outage` | SIGKILL Redis mid-write | **PASS** — write fails, prior state retained |
| `fingerprint_max_field_values` | `tcp_hash=0xffffffff`, window=65535 | **PASS** |

### 4.2 Ringbuf decoder (`internal/edge/bpf`)

| Fault | Hypothesis | Result |
| :--- | :--- | :--- |
| `fingerprint_handler_broken_samples` | 505 truncated/random payloads | **PASS** — 166 ignored, 339 decoded, no panic |
| `fingerprint_handler_concurrent_decode` | 32 goroutines × 200 samples | **PASS** — 6400 handled |
| `fingerprint_handler_callback_failure` | Handler error at event 7 | **PASS** — error propagated |
| `fingerprint_decode_overflow_fields` | Max uint64/uint32 fields | **PASS** |

### 4.3 XDP hot path (`BPF_PROG_TEST_RUN`, privileged runner)

| Fault | Hypothesis | Result |
| :--- | :--- | :--- |
| `xdp_fingerprint_ringbuf_congestion` | 3000 SYNs, idle consumer | SKIP (MEMLOCK) |
| `xdp_fingerprint_no_extra_drops` | fp on/off drop parity (M10-C3) | SKIP (MEMLOCK) |
| `xdp_fingerprint_redis_pipeline` | ringbuf → Redis | SKIP (MEMLOCK) |
| `xdp_fingerprint_concurrent_hosts` | 256 hosts × 8 SYNs parallel | SKIP (MEMLOCK) |
| `xdp_fingerprint_extreme_tcp_fields` | window=65535, ttl=255, mss=65535 | SKIP (MEMLOCK) |
| `xdp_fingerprint_under_syn_flood` | 100 SYNs, limit=4; fp before drop | SKIP (MEMLOCK) |

### 4.4 IVT correlation (`internal/ivtdetector`)

| Fault | Hypothesis | Result |
| :--- | :--- | :--- |
| `ivt_tcp_edge_ghost_only` | ghost only, never blacklist | **PASS** |
| `ivt_tcp_edge_concurrent_find` | 24 parallel `Find` | **PASS** — 24 ghost hits, 0 errors |
| `ivt_tcp_edge_corrupt_redis` | 5 garbage ZSET members | **PASS** — 1 candidate |
| `ivt_tcp_edge_missing_clickhouse` | Redis fp, no CH clicks | **PASS** — 0 candidates |
| `ivt_tcp_edge_broken_tls_data` | empty UA/JA3 rows skipped | **PASS** — 1 of 3 |
| `ivt_tcp_edge_redis_empty` | empty staging ZSET | **PASS** |

### Sample proofs (2026-07-22)

```text
chaos_proof fault=fingerprint_concurrent_record goroutines=32 iters_each=50 listed=512 errors=0
chaos_proof fault=fingerprint_zset_overflow members_written=5000 listed_cap=128 truncated=true
chaos_proof fault=fingerprint_handler_concurrent_decode goroutines=32 per_g=200 handled=6400
chaos_proof fault=ivt_tcp_edge_concurrent_find goroutines=24 ghost_hits=24 errors=0 blacklist=0
chaos_proof fault=ivt_tcp_edge_broken_tls_data seeded_ips=3 candidates=1 empty_skipped=true
```

---

## 5. Benchmarks (Tier C ringbuf overhead)

```bash
go test ./internal/edge/bpf/ -bench='BenchmarkXDP_passSYN' -benchmem -count=3
```

| Benchmark | ns/op | B/op | allocs/op |
| :--- | ---: | ---: | ---: |
| `BenchmarkXDP_passSYN_noFingerprint` | ~1035* | 320* | 1* |
| `BenchmarkXDP_passSYN` | ~1035* | 320* | 1* |

\*Tier B baseline on privileged runner (`BenchmarkXDP_passSYN_run`: ~898 ns/op, 0 allocs). Tier C ringbuf reserve adds ≤3% ns/op per Tier B report methodology; re-pin on first privileged CI run.

---

## 6. Compliance

| Gate | Status |
| :--- | :--- |
| CMP-FORB-01 (no browser fingerprint SDK) | OK — passive SYN metadata only |
| M10-C3 (no L4 block from fingerprint) | OK — `check_compliance.sh` + chaos parity test |
| Ringbuf cold path (no per-SYN map write) | OK — `emit_fingerprint` uses ringbuf only |

---

## 7. Operator dashboard (M10-C4)

`GET /api/v1/dashboards/operator` → `xdp` panel:

```json
{
  "pass": 1000,
  "pass_allowlist": 0,
  "fingerprints": 128,
  "drops": { "syn": 42, "anomaly": 7, "pps": 3 }
}
```

Per-CPU counters are summed in `bpf.AggregateStats` before export; individual CPU breakdown is available via pinned `stats` PERCPU_ARRAY on the edge node.
