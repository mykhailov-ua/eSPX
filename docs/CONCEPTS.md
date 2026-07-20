# eSPX Design Technical Guide: System Concepts and Mechanics

This document defines fundamental principles for designing high-performance systems, system call mechanics, memory hierarchy, network behavior, and Go runtime execution, illustrated with concrete mechanics from the **eSPX** architecture.

The entire eSPX architecture is built on **Data-Oriented Design (DOD)** — software design centered on memory layout, CPU cache locality, and hardware efficiency without unnecessary abstraction layers.

---

## 1. System Resources and Latency Hierarchy

Designing high-performance systems requires an exact understanding of hardware physical constraints (Zen 5 / Emerald Rapids architectures, PCIe Gen 5, DDR5 memory):

| Operation | Time (ns) | CPU Cycles (approx.) | Relative Scale (1 ns = 1 sec) |
| :--- | :--- | :--- | :--- |
| L1 Cache Access (32–48 KB/core) | ~1 ns | 4 | 1 sec |
| Branch Mispredict | ~3 ns | 12 | 3 sec |
| L2 Cache Access (1–2 MB/core) | ~4 ns | 16 | 4 sec |
| L3 Cache Access (shared, 32–96 MB) | ~12 ns | 48 | 12 sec |
| Atomic CAS (intra-core) | ~15 ns | 60 | 15 sec |
| Syscall via vDSO (`clock_gettime`) | ~20 ns | 80 | 20 sec |
| Atomic CAS (cross-core, NUMA) | ~80 ns | 320 | 1.3 min |
| Main Memory Access (DDR5 DRAM, local) | ~100 ns | 400 | 1.6 min |
| Cross-Socket NUMA DRAM Access | ~180 ns | 720 | 3 min |
| Syscall (User $\rightarrow$ Kernel transition) | ~200 ns | 800 | 3.3 min |
| OS Thread Context Switch | ~1,500 ns | 6,000 | 25 min |
| Random 4 KB Read from NVMe Gen 5 | ~15,000 ns | 60,000 | 4 hours |
| Sequential 1 MB Read from DDR5 RAM | ~20,000 ns | 80,000 | 5.5 hours |
| Rack Network Latency (LAN) | ~150,000 ns | 600,000 | 1.7 days |
| Datacenter TCP Latency | ~300,000 ns | 1,200,000 | 3.5 days |
| Cross-AZ WAN Latency | ~1,500,000 ns | 6,000,000 | 17 days |

---

## 2. Go Runtime & OS Kernel Mechanics

### 2.1 System Calls (Syscalls) & User-to-Kernel Transitions
Every system call (`read`, `write`, `epoll_wait`) forces the CPU to switch from User Space (Ring 3) to Kernel Space (Ring 0):
1. Saves general-purpose registers onto the kernel stack.
2. Switches thread context to the kernel stack.
3. Validates permissions and pointer arguments.
4. Flushes the Translation Lookaside Buffer (TLB) when KPTI Meltdown mitigations execute.
5. Returns to User Space and restores register context.

#### Engineering Rule (Waterline Batching)
Hot-path code MUST NOT issue synchronous system calls per event. Events accumulate in memory-mapped buffers or ring queues and write in single batch operations (e.g. 20,000 events or 5-second flushes in `cmd/processor`), reducing kernel transition overhead by $10,000\times$.

### 2.2 Thread Pinning & CPU Affinity (EEVDF Scheduler)
Modern Linux kernels use the Earliest Eligible Virtual Deadline First (EEVDF) scheduler. When a worker thread blocks or loses its CPU time slice, the scheduler migrates the thread across physical CPU cores. This **Thread Migration** invalidates the L1/L2 caches of the target core.

#### Engineering Rule (Cache Pinning)
In `cmd/tracker`, worker goroutines pin to OS threads via `runtime.LockOSThread()`. Deployment scripts set process CPU affinity (e.g. `taskset -c 0-3`), guaranteeing that `CampaignRegistry` data remains hot inside the assigned core's L1/L2 cache.

### 2.3 Go G-M-P Scheduler & Non-Blocking I/O
Standard `net/http` allocates one goroutine per connection. At 100,000 concurrent Keep-Alive connections, 100,000 goroutines consume over 200 MB of stack RAM and trigger high GC scanning overhead.

#### Engineering Rule (gnet Event Loop)
`cmd/tracker` replaces standard stdlib HTTP servers with `gnet`. `gnet` runs a fixed number of worker threads equal to physical CPU cores. Each worker runs an epoll edge-triggered event loop (`epoll_wait`), reading non-blocking socket buffers without per-connection goroutine creation.

---

## 3. Hot-Path Engineering Rules & Memory Guidelines

### 3.1 Heap Allocation Avoidance Rules

1. **Zero String Concatenation**: Concatenating strings via `+` creates immutable heap allocations. Hot-path keys (e.g. `dedup:<campaign_id>:<click_id>`) MUST be formatted using fixed stack byte arrays (`var buf [128]byte`) and converted using `unsafe.String`.
2. **No Reflection or `fmt.Sprintf`**: `fmt` functions cause interface boxing and runtime reflection. Serializers MUST use precompiled binary protocols (`vtproto`) or stack-bound byte buffers.
3. **Flat Context Structs**: Passing parameters via `context.WithValue` creates heap-allocated linked lists with $O(N)$ lookup costs. Hot-path methods pass a flat `FilterContext` struct pointer allocated on the caller stack.

### 3.2 Lock-Free Ring Buffers vs. Channels
Standard Go channels use internal mutexes (`hchan.lock`). Under heavy contention, channel sends trigger CPU bus lock contention and goroutine parking (`gopark`).

#### Engineering Rule
High-frequency inter-thread communication uses Multi-Producer Single-Consumer (MPSC) lock-free ring buffers operating via atomic cursor swaps (`atomic.AddUint64`).

---

## 4. Cache Line Alignment & False Sharing Prevention

CPU memory subsystems transfer data in 64-byte cache lines. When adjacent CPU cores write to independent variables located inside the same 64-byte cache line, the MESI protocol repeatedly invalidates the cache line across cores (**Cache Line Bouncing**), slowing atomic updates by $15-20\times$.

#### Engineering Rule (56-Byte Padding)
High-frequency atomic counters stored in shared structures MUST include explicit 56-byte array padding (`_ [56]byte`) between fields to force each atomic variable into its own dedicated 64-byte cache line.

---

## 5. Storage Engines: PostgreSQL (B+-Tree) vs. ClickHouse (LSM-Tree)

### 5.1 PostgreSQL (B+-Tree & MVCC)
PostgreSQL implements Multi-Version Concurrency Control (MVCC). Executing `UPDATE` statements writes new physical tuple versions to disk while leaving old versions marked dead until `VACUUM` cleans them.
- **Hot-Path Rule**: Direct `UPDATE campaigns SET balance = balance - cost` on every event is strictly prohibited due to write amplification and index bloat.
- **Cold-Path Rule**: `cmd/processor` updates balances in 10-second consolidated batches, or appends immutable spend rows to `balance_ledger` with unique `click_id` keys (`ON CONFLICT DO NOTHING`).

### 5.2 ClickHouse (LSM-Tree & MergeTree)
ClickHouse organizes storage into immutable compressed column parts. Small, frequent inserts create thousands of microscopic data parts, leading to `Too many parts in all data parts` exceptions.
- **Batch Insertion Rule**: Events are buffered in userspace ring queues until reaching 20,000 rows or a 5-second window before emitting a single `INSERT`.
- **Columnar Scanning**: Analytical queries SELECT only required columns (e.g., `campaign_id`, `cost`), allowing ClickHouse to open only corresponding column files on disk rather than scanning entire rows.

---

## 6. File Descriptor (FD) Budget & OS Resource Limits

Linux tracks open files, sockets, pipes, and epoll instances using file descriptors.
- **Resource Constraints**: Default system process limits (1024 FDs) cause `EMFILE: Too many open files` during traffic bursts if connections or response bodies leak.
- **Engineering Rules**:
  1. Systemd units configure `LimitNOFILE=1048576`.
  2. All HTTP client calls enforce static connection pools (`MaxIdleConns`, `IdleConnTimeout`) and explicit request timeouts.
  3. Background monitoring reads `/proc/self/fd` to export `ad_allocated_fds` metrics. Alerts trigger at 80% capacity.
