# eSPX — Architectural & Commercial Proposals

Unified reference document for all proposed system capabilities, feature evolutions, and commercial licensing models. Proposals documented herein represent optional architectural enhancements and do not alter core system invariants until formally accepted into milestone deliverables.

---

## 1. Executive Summary

This document consolidates proposals for the eSPX platform:

1. **ESPX-LP-2026-V1**: Hybrid Volume Licensing & Feature Tiering Model
2. **ESPX-EDGE-LOC-2026**: Zero-Latency Edge Landing Page Localization Engine
3. **ESPX-XDP-FILTER-2026**: High-RPS eBPF/XDP Line-Rate Packet Filter & SmartNIC Offload
4. **Abstract Pricing Units (PU)**: Currency-independent commercial packaging model

---

## 2. Proposal 1 — Hybrid Volume Licensing (ESPX-LP-2026-V1) `Shipped (M3-T)`

### 2.1 Overview & Architecture

Shifts commercial monetization from rigid peak-RPS constraints to a prepaid monthly volume model (billable events) combined with subsystem feature flags:
- **Infrastructure Protection**: RPS and RPD (requests per day) remain active as local infrastructure protection gates (burst limits and daily ceilings) enforced in tracker ingress cells.
- **Cold-Path Metering**: All license verification, billable volume calculations, and quota comparisons execute on the cold path inside `cmd/management` and `cmd/processor`.
- **Zero-Latency Hot Path**: Hot-path ingestion (`/track`, gnet) accesses local memory snapshots only (`atomic.Value` pointers), performing zero network calls to external license servers.

### 2.2 Subsystem Feature Controls & Limits

The deployment license JWT specification (verified via public key in `internal/licensing`) defines:
- **Commercial Tier Rating**: Tiers 1 through 3 representing prepaid volume scale.
- **Quantitative Limits**: Maximum allowed RPS, maximum requests per day (RPD), active campaign caps, maximum active regions, and Redis master node ceilings.
- **Feature Controls**: Independent boolean flags governing subsystem execution (`ingestion_gnet`, `openrtb_engine`, `ivt_ml_detector`, `ebpf_xdp_edge`, `ml_fraud_boost`, `multi_region`).
- **Billable Event Discount Weights**: Multipliers applied during cold-path hourly rollups (`accepted_event = 1.0`, `lua_dedup_fcap_reject = 0.1`, `ebpf_l4_drop = 0.0`).

### 2.3 End-to-End Event Accounting & Storage Lifecycle

1. **Hot-Path Event Ingestion**: An incoming HTTP request reaches `/track`. The tracker evaluates local rate limiters and passes the payload to Redis Lua scripts for budget debit.
2. **Event Classification**: The return code from Lua indicates whether the event was accepted, rejected via frequency capping/deduplication, or rate-limited.
3. **Stream Persistence**: Accepted events flow through Redis streams (`ad:events:stream`) to `cmd/processor`.
4. **Cold-Path Hourly Rollup**: `VolumeMeterWorker` in `cmd/management` executes a governed ClickHouse aggregation query over `clicks` and `impressions` tables, counting event types by rejection reason.
5. **Weighted Volume Calculation**: Applies billable event weights (`billable_units = Σ (count[type] × weight[type])`) and appends the result to `usage_meters` in PostgreSQL within an 8 KiB OS page write.

### 2.4 Chaos Testing & Vulnerability Analysis

- **Steady-State Invariant**: Ingestion SLA (p99 < 80 ms) remains stable even during monthly quota transitions or overage alerts.
- **Fault Injection**: Network disconnect to vendor license server, expired local JWT token, corrupted claims payload, concurrent quota updates.
- **Invariants**: When the vendor license server is unreachable, the system continues operating on the last-known-good JWT memory snapshot until the grace period expires (`ESPX_LICENSE_MODE=file`).

---

## 3. Proposal 2 — Edge Landing Page Localization Engine (ESPX-EDGE-LOC-2026)

### 3.1 Overview & Architecture

Eliminates cross-region HTTP redirects for localized landing pages by dynamically replacing locale tokens directly within the edge proxy (Nginx / OpenResty) or Go ingress gateway.

### 3.2 End-to-End Client-to-Disk Lifecycle & OS/Hardware Mechanics

1. **Client TCP Connection**: User connects over TLS. Nginx or the Go ingress gateway accepts the socket (`epoll_wait`).
2. **Zero-Copy GeoIP Lookup**: Performs an $O(1)$ lookup over MaxMind / MMDB binary data structures mapped directly into userspace memory via `mmap`.
3. **In-Flight Buffer Substitution**: Replaces localized tokens (e.g. `{{country_name}}`, `{{currency_symbol}}`) directly within output buffer chains (`ngx_chain_t` or byte slices) without triggering heap allocations.
4. **OS Page Cache & Kernel Send**: The rendered HTML payload is written directly to the TCP socket buffer via `sendfile` / `writev` syscalls, leveraging the Linux OS page cache.

---

## 4. Proposal 3 — High-RPS eBPF/XDP Line-Rate Packet Filter & SmartNIC Offload (ESPX-XDP-FILTER-2026)

### 4.1 Overview & Architecture

Offloads malicious IP blocking, volumetric DDoS mitigation, and protocol fingerprinting directly into kernel network interface driver space (Driver XDP) or hardware Network Processing Units (Hardware Offloaded XDP).

### 4.2 Advanced eBPF/XDP Enhancements & Mechanics

#### 4.2.1 SmartNIC Hardware Offloading (`XDP_FLAGS_HW_MODE`)
- **Hardware Acceleration**: Loads the compiled eBPF bytecode directly into the NPU/ASIC memory of supported SmartNICs (e.g., Mellanox ConnectX-6/7, Netronome Agilio) via `XDP_FLAGS_HW_MODE`.
- **Zero Host Overhead**: Malicious packets matching hardware BPF maps are dropped directly inside the SmartNIC ASIC before PCI Express DMA transfers to host DRAM occur. Eliminates host CPU utilization, PCIe bus contention, and host memory bandwidth consumption during 100+ Gbps volumetric floods.
- **Driver Mode Fallback**: If hardware offloading is unsupported or SmartNIC BPF map capacity is exceeded, automatically degrades to Native Driver XDP (`XDP_FLAGS_DRV_MODE`) inside the Linux NIC driver RX ring buffer.

#### 4.2.2 Linux Kernel eBPF Verifier Compliance Mechanics
- **Explicit Boundary Validation**: All protocol header offset calculations MUST include explicit bounds checks against `ctx->data_end`:
  $$\text{data} + \text{sizeof(struct ethhdr)} + \text{sizeof(struct iphdr)} + \text{sizeof(struct tcphdr)} \le \text{data\_end}$$
  Prevents verifier rejection by mathematically proving memory safety before register dereferencing.
- **512-Byte Stack Limit Mitigation**: To remain strictly within the 512-byte stack frame limit, scratchpad data structures and lookup keys are stored inside `BPF_MAP_TYPE_PERCPU_ARRAY` maps rather than allocated on the eBPF function stack.
- **Bounded Loops & Complexity Caps**: All loops MUST be fully unrolled (`#pragma unroll`) or strictly bounded to guarantee execution under 1,000,000 instructions, ensuring deterministic verifier approval.

#### 4.2.3 Stateless Line-Rate SYN Flood Mitigation (`XDP_TX`)
- **Stateless TCP SYN Cookies**: Generates $O(1)$ cryptographic SipHash TCP SYN cookies directly inside the XDP hook during volumetric SYN floods.
- **Driver Transmission (`XDP_TX`)**: Reverses Ethernet MAC addresses, IP addresses, and TCP ports in place, setting the calculated SYN cookie sequence number, and emits the `SYN-ACK` packet directly back out the receiving NIC interface (`XDP_TX`).
- **Socket Allocation Bypass**: Eliminates kernel TCP socket allocation (`struct sock`) and connection tracking (`conntrack`) table entries until a valid client `ACK` packet arrives.

#### 4.2.4 Layer 7 Protocol Payload & TLS SNI / JA3 Inspection
- **Fixed-Offset Payload Parsing**: Verifies `tcph->doff` offset and inspects the initial bytes of incoming TCP payloads (`data + tcph_bytes`).
- **TLS Client Hello Verification**: Validates TLS record headers (`ContentType == 22`, `HandshakeType == 1`) and extracts Server Name Indication (SNI) hostnames or JA3 fingerprints using unrolled byte comparisons.
- **Driver Drop Action**: Malicious scraping bots or unapproved TLS SNI requests hit `XDP_DROP` before user-space socket context switches occur.

#### 4.2.5 Per-Subnet Rate Limiting (`BPF_MAP_TYPE_LRU_HASH`)
- **LRU Map Storage**: Tracks packet frequencies per IPv4 `/24` subnet or IPv6 `/48` subnet using Least-Recently-Used maps (`BPF_MAP_TYPE_LRU_HASH`).
- **Atomic Token Bucket**: Executes lock-free atomic updates (`__sync_fetch_and_add`) over per-subnet token counters. Excess traffic exceeding configured packets-per-second (PPS) thresholds is dropped (`XDP_DROP`).

#### 4.2.6 Direct Zero-Copy Bypass via AF_XDP (`XDP_REDIRECT`)
- **Kernel Bypass**: High-priority tracker endpoints leverage AF_XDP (XSK - XDP Sockets) to bypass the entire Linux kernel TCP/IP network stack.
- **Direct UMEM Transfer**: Clean packets matching valid campaign paths execute `XDP_REDIRECT`, transferring raw packet frames directly into user-space memory-mapped ring buffers (`UMEM`) owned by `gnet` worker threads.

---

## 5. Abstract Pricing Unit (PU) Matrix

Commercial packaging defines abstract Pricing Units (PU) without currency binding. The operator multiplies total PU by their local `pu_rate` during billing:

$$\text{monthly\_PU} = \kappa_{\text{base}}[\text{band}] + \sum \text{enabled\_module} \times \kappa_{\text{module}}[\text{band}]$$

### 5.1 Volume Bands & Ratings

| Band | Billable Events / Month | Base PU ($\kappa_{\text{base}}$) | All-In Bundle PU |
| :--- | :--- | ---: | ---: |
| **Small (S)** | ≤ 10,000,000,000 | 100 | 200 |
| **Medium (M)** | ≤ 50,000,000,000 | 250 | 480 |
| **Large (L)** | ≥ 100,000,000,000 | 500 | 950 |

### 5.2 Add-On Component Modules

| Module | Subsystem Flag | $\kappa$[S] | $\kappa$[M] | $\kappa$[L] |
| :--- | :--- | ---: | ---: | ---: |
| **OpenRTB Engine** | `openrtb_engine` | 50 | 120 | 250 |
| **eBPF/XDP Filter** | `ebpf_xdp_edge` | 40 | 100 | 200 |
| **ML IVT + Analytics** | `ivt_ml_detector` | 40 | 80 | 150 |
