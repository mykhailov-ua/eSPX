# eBPF/XDP Milestone — Edge L4 Perimeter (M10)

Kernel-level ingress filter on tracker port `:8180`. Defensive perimeter only ([GUIDE_COMPLIANCE.md](./GUIDE_COMPLIANCE.md) §1). Supersedes the guide draft; implementation backlog is aligned with [docs/MILESTONE.md](./docs/MILESTONE.md) M10 and [EBPF_IDEAS.md](./EBPF_IDEAS.md).

**Related:** [docs/EDGE.md](./docs/EDGE.md) Part V, [GO.md](./docs/GO.md), [GUIDE_HOT_PATH_ZERO_ALLOC.md](./GUIDE_HOT_PATH_ZERO_ALLOC.md), [GUIDE_COMPLIANCE.md](./GUIDE_COMPLIANCE.md)

### Contents

1. [Execution order & priorities](#1-execution-order--priorities)
2. [SLA & latency budget](#2-sla--latency-budget)
3. [Shipped baseline (DoD met)](#3-shipped-baseline-dod-met)
4. [Tier A — P0: protocol hygiene & operability](#4-tier-a--p0-protocol-hygiene--operability)
5. [Tier B — P1: SYN-flood resilience](#5-tier-b--p1-syn-flood-resilience)
6. [Tier C — P2: passive IVT signals](#6-tier-c--p2-passive-ivt-signals)
7. [Tier D — P3: portability, offload, chaos](#7-tier-d--p3-portability-offload-chaos)
8. [Compliance gates (Art. 361 UK, CFAA, EU)](#8-compliance-gates-art-361-uk-cfaa-eu)
9. [Hot-path engineering standards](#9-hot-path-engineering-standards)
10. [Verification, benchmarks & CI](#10-verification-benchmarks--ci)
11. [File reference](#11-file-reference)
12. [Out of scope](#12-out-of-scope)

---

## 1. Execution order & priorities

```text
[P0 parallel — unblock ops]
M10-A6 (Makefile/CO-RE prep)  |  M10-A4 (config map)  |  M10-obs (Prometheus stats export)

[P0 protocol hardening]
M10-A1 → M10-A2 → M10-A3 → M10-A5

[P1 flood resilience — after P0 stable]
M10-B1 (SYN cookies)  |  M10-B4 (/24 SYN cap)  |  M10-B3 (ringbuf autoban)

[P2 fraud signals — cold feedback only]
M10-C1 → M10-C2 → M10-C3  |  M10-C4 (dashboard)

[P3 platform]
CO-RE skeleton  |  HW offload  |  XDP chaos injector (lab only)

Independent / background:
* blocklist sync tuning (`edge-bpf-sync` interval)
* disasm snapshot on every `edge_filter.c` change
```

| Priority | Tier | Theme | Size |
| :--- | :--- | :--- | :--- |
| **P0** | A | TCP hygiene, non-TCP drop, dynamic config, build fix, metrics | M |
| **P1** | B | SYN cookies, /24 cap, ringbuf violation loop | M |
| **P2** | C | SYN-stage TCP fingerprint → IVT (score only, no L4 block) | L |
| **P3** | D | CO-RE, SmartNIC offload, chaos injector | L–XL |

---

## 2. SLA & latency budget

XDP runs **before** nginx/gnet. Its cost must be negligible vs tracker SLA (p95 < 50 ms, p99 < 80 ms per `.cursorrules`).

### Packet path (kernel, `XDP_MODE=native` target)

| Metric | Target | Measurement |
| :--- | :--- | :--- |
| Decision p50 (non-`:8180`, early `PASS`) | < 300 ns | `perf stat` / NIC trace on attached iface |
| Decision p99 (`:8180`, allow/block LPM hit) | < 2 µs | Same; no LRU write |
| Decision p99 (`:8180`, LRU ratelimit update) | < 10 µs | SYN/PPS map update path |
| Stack per BPF frame | ≤ 512 B | Verifier + `llvm-objdump` |
| Helper calls per `:8180` pass (happy path) | ≤ 3 | `llvm-objdump -d` helper inventory |
| Program load / attach | < 2 s | `edge-xdp` startup on deploy |
| Map sync lag (Redis → pinned LPM) | ≤ `SYNC_INTERVAL` (default 5 s) | `edge-bpf-sync` + chaos propagation test |

### Userspace test harness (`prog.Test`)

| Metric | Target | Notes |
| :--- | :--- | :--- |
| `BenchmarkXDP_passSYN` | baseline recorded in CI | Interpreter path; not production latency |
| `BenchmarkXDP_dropBlocklist` | within 2× pass SYN | LPM lookup overhead bound |
| `BenchmarkXDP_passPPSACK` | within 1.5× pass SYN | LRU update on established flood |
| Heap allocs/op (Go loader/sync) | 0 on hot sync path where applicable | `blocklist` merge benches |

### Functional correctness

| Invariant | Requirement |
| :--- | :--- |
| Allow before deny | `allow_v4` lookup precedes `blocklist_v4` |
| Non-tracker port | `XDP_PASS` before any deny map |
| SYN limit | 64/s per source IPv4 (configurable in A4) |
| Global SYN | 50k/s ÷ assumed CPUs (default 8) |
| PPS token bucket | 2000 burst/rate per source to `:8180` |
| Compliance | §1 defensive only — no outbound strike |

---

## 3. Shipped baseline (DoD met)

**Status:** Implemented · **Source:** `deploy/edge/xdp/bpf/edge_filter.c`, `cmd/edge-xdp`, `cmd/edge-bpf-sync`

### Delivered capabilities

| Component | Description |
| :--- | :--- |
| **XDP filter** | IPv4/TCP to `:8180` only; malformed/short packets → `PASS` |
| **LPM allow/deny** | `allow_v4` before `blocklist_v4`; pinned at `/sys/fs/bpf/espx/` |
| **Per-IP SYN window** | 64/s, 1 s window, `LRU_HASH` `syn_ratelimit_v4` |
| **Global SYN cap** | `PERCPU_ARRAY` `global_syn`, 50k/s ÷ 8 CPUs |
| **PPS limiter** | Token bucket 2000/s, `ratelimit_v4` LRU |
| **Stats** | `PERCPU_ARRAY` counters: pass, allow, drop blocklist/SYN/global/PPS |
| **Loader** | `edge-xdp`: attach generic/native/offload, pin maps |
| **Sync** | `edge-bpf-sync`: Redis shard-0 → pinned LPM; license gate `ebpf_xdp_edge` |
| **L7 mirror** | nginx `edge-phase2.lua`, perimeter cache tests |

### Implemented hot-path patterns

| Pattern | Where |
| :--- | :--- |
| Verifier bounds (`data` / `data_end`) | Every header parse in `xdp_edge_filter` |
| Early `PASS` (port filter before maps) | `bpf_ntohs(tcph->dest) != TRACKER_INGRESS_PORT` |
| `__always_inline` ratelimit helpers | `check_syn_limit`, `check_pps_limit`, `stat_inc` |
| Flat map value structs | `syn_state`, `pps_bucket` — no nested pointers |
| Per-CPU stats (no false sharing) | `stats`, `global_syn` maps |
| Monotonic time | `bpf_ktime_get_ns()` |
| Cold write / hot read blocklist | `edge-bpf-sync` only; XDP never updates deny trie |
| Immutable allowlist short-circuit | Host and CIDR allow bypass PPS and deny |

### Tests (shipped)

| Suite | Count | Location |
| :--- | :--- | :--- |
| XDP functional | 11 tests | `internal/edge/bpf/edge_filter_test.go` |
| XDP benchmarks | 3 benches | `internal/edge/bpf/bench_test.go` |
| LPM key/sync | unit tests | `internal/edge/lpm/`, `blocklist/`, `allowlist/` |
| Edge chaos (L7 mirror) | 2+ proofs | `internal/edge/perimeter/edge_chaos_test.go` |

**Requirement to run BPF tests:** `CAP_BPF`, `rlimit.RemoveMemlock()` (see `cmd/edge-xdp`). Without privileges tests **skip** — not a pass on CI until privileged job exists.

| Test | Asserts |
| :--- | :--- |
| `TestXDP_dropBlocklistedSource` | Blocklisted SYN → `XDP_DROP` |
| `TestXDP_passNonTrackerPort` | Blocklisted IP on :443 → `XDP_PASS` |
| `TestXDP_dropPerIPSYNFlood` | 70 SYNs → drop after 64 |
| `TestXDP_dropGlobalSYNFlood` | Distributed SYN → global cap drop |
| `TestXDP_passACKTraffic` | 200 ACKs → all pass |
| `TestXDP_dropPPSFlood` | 2100 PSH+ACK → drop after burst |
| `TestXDP_ppsPerIPIndependent` | Exhaust A does not block B |
| `TestXDP_synCountsTowardPPS` | SYN charged to PPS bucket |
| `TestXDP_allowBypassBlocklist` | Allow wins over deny |
| `TestXDP_allowBypassPPS` | Allow CIDR bypasses PPS |
| `TestXDP_allowCIDRPrefix` | LPM longest-prefix allow |

### Benchmarks (defined; capture on privileged host)

```bash
# Requires memlock; record ns/op in PR when touching edge_filter.c
go test ./internal/edge/bpf/... -bench=. -benchmem -run='^$' -short=false
```

| Benchmark | Path exercised |
| :--- | :--- |
| `BenchmarkXDP_passSYN` | `:8180` SYN, no map entries |
| `BenchmarkXDP_dropBlocklist` | LPM deny hit |
| `BenchmarkXDP_passPPSACK` | Established ACK + LRU token decrement |

**Baseline:** not yet pinned in CI (BPF tests skip in default sandbox). **DoD for M10-CI:** privileged workflow records ns/op ±5% regression gate.

### Build artifacts

| Artifact | Status |
| :--- | :--- |
| `deploy/edge/xdp/bpf/edge_filter.o` | `make -C deploy/edge/xdp` (`-O2 -target bpf -Wall -Werror`) |
| `deploy/edge/xdp/bpf/edge_filter.disasm.txt` | Snapshot checked in |
| `internal/edge/bpf/edge_bpfel.go` | cilium/ebpf codegen via `gen.go` |

---

## 4. Tier A — P0: protocol hygiene & operability

**Goal:** Close obvious L4 holes, remove hardcoded tunables, make filtering observable.

| ID | Task | DoD |
| :--- | :--- | :--- |
| **M10-A1** | TCP anomaly filter | Drop `SYN+FIN`, `SYN+RST`, NULL, FIN-only, XMAS on `:8180`; `XDP_STAT_DROP_ANOMALY`; unit test + disasm |
| **M10-A2** | SYN validity | Drop `doff < 5`, zero src/dst port; tests |
| **M10-A3** | Non-TCP on `:8180` | `XDP_DROP` for UDP/ICMP/SCTP to tracker port (not `PASS`) |
| **M10-A4** | Config map | `ARRAY` map: `syn_limit`, `pps_rate`, `global_syn_limit`, `assumed_cpus`; remove `#define` hardcode |
| **M10-A5** | RST rate limit | Dedicated LRU token bucket per IP; test RST flood |
| **M10-A6** | Makefile fix | `-I/usr/include/$(uname -m)-linux-gnu`; `gen.go` → `deploy/edge/xdp/bpf/edge_filter.c`; `make` green on Debian |
| **M10-obs** | Prometheus export | Extend `edge-bpf-sync` (or sidecar): sum per-CPU `stats` → `ad_xdp_drop_total{reason}` |

### Tier A exit criteria

- [x] All M10-A1..A6 tasks merged
- [x] `go test ./internal/edge/bpf/...` green on privileged runner
- [x] `scripts/ci/check_compliance.sh` green
- [x] `ad_xdp_*` metrics visible on `:9090`
- [x] No new helper calls on non-`:8180` path
- [x] Disasm diff attached in PR

**Status:** Done · report: [docs/M10_TIER_A_TECHNICAL_REPORT.md](./docs/M10_TIER_A_TECHNICAL_REPORT.md)

---

## 5. Tier B — P1: SYN-flood resilience

**Goal:** Survive high-volume SYN without conntrack exhaustion; close the loop to cluster blocklist.

| ID | Task | DoD |
| :--- | :--- | :--- |
| **M10-B1** | Stateless SYN cookies | Tail-called prog; `bpf_tcp_gen_syncookie_ipv4`; flag `XDP_SYN_COOKIE=1`; kernel ≥ 6.0; chaos proof under SYN flood |
| **M10-B2** | Spoof block map | *Deferred / research* — `tcp_retransmit_synack` trace → `spoof_block_v4`; legal review required |
| **M10-B3** | Ringbuf violations | On SYN/PPS drop: `ringbuf_output` event → `edge-bpf-sync` → Redis `blacklist:auto` TTL; no per-packet ringbuf on pass |
| **M10-B4** | /24 SYN cap | LRU keyed by `/24` prefix; independent of per-host SYN |

### Tier B exit criteria

- [x] SYN cookie path off by default; enabled only via env (`XDP_SYN_COOKIE=1`)
- [x] Ringbuf handler never opens outbound socket to offender IP
- [x] Autoban visible in LPM within `SYNC_INTERVAL` (immediate sync on violation drain)
- [x] Under synthetic SYN flood, control path stable vs flood (`chaos_proof fault=xdp_syn_flood` via `TestChaos_XDPSynFloodSynthetic` / `BPF_PROG_TEST_RUN`; staging accept-queue proof optional)

**Status:** B1/B3/B4 done · B2 deferred · report: [docs/M10_TIER_B_TECHNICAL_REPORT.md](./docs/M10_TIER_B_TECHNICAL_REPORT.md)

---

## 6. Tier C — P2: passive IVT signals

**Goal:** Passive fraud signals for IVT — **score only**, not L4 hard block ([GUIDE_COMPLIANCE.md](./GUIDE_COMPLIANCE.md) §1.B).

| ID | Task | DoD |
| :--- | :--- | :--- |
| **M10-C1** | SYN TCP fingerprint | Hash(window, MSS, options, TTL) on SYN; ringbuf to userspace |
| **M10-C2** | IVT correlation | Match TCP hash × UA × JA3 in `ivt-detector` / fraud outbox (`ML_GHOST_IVT`) |
| **M10-C3** | No fingerprint block | Document + test: fingerprint never sole cause of `XDP_DROP` |
| **M10-C4** | Admin dashboard | Panels: anomaly, SYN/PPS drops, spoof (if B2), per-CPU stats |

### Tier C exit criteria

- [x] Fingerprint path is cold (ringbuf batch, not map update per SYN)
- [x] GDPR/ePrivacy: passive metadata only — no browser probe
- [x] M10-C3 enforced in code review checklist (`scripts/ci/check_compliance.sh`)

**Status:** C1/C2/C3/C4 done · report: [docs/M10_TIER_C_TECHNICAL_REPORT.md](./docs/M10_TIER_C_TECHNICAL_REPORT.md)

---

## 7. Tier D — P3: portability, offload, chaos

| ID | Task | DoD |
| :--- | :--- | :--- |
| **M10-D1** | CO-RE / BTF | libbpf skeleton; single `.o` runs on target kernel range without header rebuild |
| **M10-D2** | HW offload | `XDP_FLAGS_HW_MODE` documented; SmartNIC lab proof at 100G+ attack sim |
| **M10-D3** | XDP chaos injector | Lab/staging BPF prog; %-drop or delay by CIDR; **never** production default ([EBPF_IDEAS.md](./EBPF_IDEAS.md) §4) |
| **M10-D4** | Native XDP perf gate | `scripts/perf-gate/` scenario: XDP attached, tracker p99 unchanged ±2 ms |

---

## 8. Compliance gates (Art. 361 UK, CFAA, EU)

**Not legal advice.** Every tier must pass before merge.

### Allowed (§1 defensive)

| Action | Legal basis |
| :--- | :--- |
| `XDP_DROP` on local `:8180` after policy breach | Art. 361 UK self-defense; CFAA own-system defense |
| `XDP_PASS` non-target ports/protocols | Proportionality — no collateral drop |
| `allow_v4` before `blocklist_v4` | Prevents misroute of resolvers/LAN |
| `XDP_TX` SYN-ACK cookie on **own** IP | Defensive response on defended host |
| Ringbuf → Redis autoban | Extends local deny; no outbound strike |

### Forbidden (§2 offensive) — merge blockers

| Action | Risk |
| :--- | :--- |
| `XDP_TX` flood / RST injection toward source IPs | Art. 361 / 361-1 UK; CFAA |
| `XDP_REDIRECT` to honeypot/third party without contract | Unauthorized routing |
| Hidden BPF prog/map load | Art. 361 R3 |
| Port scan / hack-back from `edge-bpf-sync` | CFAA; [GUIDE_COMPLIANCE.md](./GUIDE_COMPLIANCE.md) §2 |
| L4 block **only** from TCP fingerprint | EU GDPR/ePrivacy + product rule M10-C3 |

```text
Permitted:  inbound → XDP → PASS | DROP | (optional) SYN-ACK TX on same flow
Forbidden:  map hit → outbound attack / scan → foreign IP
```

**CI:** `scripts/ci/check_compliance.sh` — no `cilium/ebpf` in `management`/`tracker`; no forbidden SDK patterns.

---

## 9. Hot-path engineering standards

Mandatory patterns for all `edge_filter.c` changes (Go analog: [GUIDE_HOT_PATH_ZERO_ALLOC.md](./GUIDE_HOT_PATH_ZERO_ALLOC.md)).

| Rule | eBPF enforcement |
| :--- | :--- |
| Zero heap | Verifier; state in maps only |
| Stack ≤ 512 B | Large scratch → `PERCPU_ARRAY` map |
| Bounds checks | `(void *)(hdr + 1) > data_end` before every field read |
| Branch order | cheap early `PASS` → port → allow → deny → ratelimit |
| No per-packet `bpf_printk` / ringbuf on pass | Debug behind `DEBUG`; violations only |
| Per-CPU counters | `stats` map — no shared atomics |
| `__always_inline` hot helpers | Or `noinline` BPF-to-BPF if stack overflow |
| `-O2 -target bpf` | Never `-O0` in prod image |

### Check path order (reference)

```text
bounds → non-IPv4 PASS → non-TCP PASS → dest != :8180 PASS
→ allow LPM → deny LPM → SYN limits → PPS limit → PASS + stat
```

---

## 10. Verification, benchmarks & CI

### Per-PR (BPF-touching)

```bash
cd deploy/edge/xdp && make
llvm-objdump -d -S bpf/edge_filter.o | rg 'call.*bpf_'
bpftool prog load bpf/edge_filter.o /sys/fs/bpf/espx/ci-test
go test ./internal/edge/bpf/... -count=1          # privileged runner
go test ./internal/edge/blocklist/... ./internal/edge/allowlist/... -short
bash scripts/ci/check_compliance.sh
```

### PR checklist

Verified **2026-07-23** on `edge_filter.c` (privileged Docker `go test`, disasm snapshot, static review).

- [x] Verifier clean; stack ≤ 512 B — loader green; disasm max `r10 - 136` B (≤ 512 B)
- [x] Non-`:8180` → `PASS` before map lookup — insn 237 `goto LBB1_107` before first `call 1`; `TestXDP_passNonTrackerPort`
- [x] Allow before deny — `allow_v4` before `blocklist_v4` in C + disasm; `TestXDP_allowBypassBlocklist`
- [x] §1 defensive classification ([§8](#8-compliance-gates-art-361-uk-cfaa-eu)) — inbound-only drops; `XDP_TX` SYN-ACK cookie only; no outbound strike helpers
- [x] Disasm or verifier log for control-flow changes — `deploy/edge/xdp/bpf/edge_filter.disasm.txt` checked in
- [x] Functional tests added/updated — 25+ `TestXDP_*` green on privileged runner (`go test ./internal/edge/bpf/... -short`)
- [x] Chaos proof if new drop reason affects handshake path — `syn_cookie_chaos_test.go`, `edge_filter_chaos_test.go`, tier-C chaos suite
- [x] M10-C3: fingerprint never sole cause of `XDP_DROP` — `check_compliance.sh` + `TestCompliance_M22C3_*` + `TestXDP_fingerprintDoesNotCauseDrop`

### Verification record (latest)

| Step | Result | Notes |
| :--- | :---: | :--- |
| `make -C deploy/edge/xdp` | OK | Requires `clang` + `libbpf-dev` (Debian bookworm) |
| `go test ./internal/edge/bpf/... -short` | OK | Privileged runner / `CAP_BPF` + memlock; default CI sandbox skips |
| `go test ./internal/edge/blocklist/... ./internal/edge/allowlist/... -short` | OK | |
| `scripts/ci/check_compliance.sh` | OK | M10-C3 gate on `edge_filter.c` |
| `bpftool prog load` | Manual | Pin under `/sys/fs/bpf/espx/` on deploy host; not required in unit harness |

### CI gaps (open)

| Gap | Target |
| :--- | :--- |
| BPF tests skip without memlock | Privileged GitHub job or self-hosted runner |
| No `ad_xdp_*` in Prometheus yet | M10-obs |
| No native XDP perf gate | M10-D4 |
| Benchmark baseline not pinned | Record on first privileged run |

---

## 11. File reference

| File | Role |
| :--- | :--- |
| `deploy/edge/xdp/bpf/edge_filter.c` | XDP program source |
| `deploy/edge/xdp/bpf/edge_filter.disasm.txt` | Bytecode snapshot |
| `deploy/edge/xdp/Makefile` | Clang BPF build |
| `deploy/edge/xdp/Dockerfile` | Edge XDP container |
| `cmd/edge-xdp/` | Load, attach, pin |
| `cmd/edge-bpf-sync/` | Redis → LPM sync |
| `internal/edge/bpf/` | Generated objects, tests, benches |
| `internal/edge/blocklist/`, `allowlist/` | Userspace LPM sync |
| `internal/edge/perimeter/` | L7 mirror + chaos |
| [EBPF_IDEAS.md](./EBPF_IDEAS.md) | Raw idea backlog |
| [docs/MILESTONE.md](./docs/MILESTONE.md) | M10 in global execution order |

---

## 12. Out of scope

| Item | Reason |
| :--- | :--- |
| Full TCP stack / conntrack in XDP | Kernel responsibility |
| TLS termination in BPF | nginx L7 |
| ASN/geo block in XDP | Cold path MaxMind; stay L7 |
| Campaign-level rate limits | Redis Lua unified filter |
| Active port scan / nmap | [GUIDE_COMPLIANCE.md](./GUIDE_COMPLIANCE.md) §2.C |
| Hack-back / outbound flood | Art. 361 UK / CFAA |
| Browser fingerprint SDK | §2.A forbidden |
| IPv6 XDP filter | Future milestone (not M10) |

---

## Suggested sequencing (summary)

```text
Shipped (baseline) ──► P0 A1–A6 + obs ──► P1 B1,B3,B4 ──► P2 C1–C4 ──► P3 D1–D4
```

Cross-link: parallel with **M9** (Lua RTT) and **M5-A** (edge H2/H3) per [docs/MILESTONE.md](./docs/MILESTONE.md).
