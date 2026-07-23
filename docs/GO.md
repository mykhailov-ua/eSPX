# Go Hot Path: Runtime, Allocations, and Internal Processes

Tracker traffic ingestion must strictly meet SLA targets: p95 < 50 ms, p99 < 80 ms. This document covers use of the `gnet` library, worker pools, zero-allocation policy, and compiler tuning.

**Detailed checklist (BCE, branches, padding, DFA vs vtproto):** [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md)

---

## 1. gnet and PinnedWorkerPool

The standard `net/http` library creates one goroutine per request. At high RPS this increases scheduler and GC load.

### gnet event loop
*   Multiplexing via `epoll`. Fixed thread count (typically 2 per CPU core).
*   HTTP/1.1 request-line and headers: table-driven FSM (`http1_fsm.go`, M5-B); incremental feed from gnet ring buffer.

### Worker pool (PinnedWorkerPool)
After parsing, work is handed off to a worker. Worker pinning is based on campaign hash:
*   Improves L1/L2 cache locality for a given campaign.
*   Queue indices are padded (64 bytes) to avoid false sharing.

---

## 2. Zero-Allocation Policy

Heap allocations on the hot path are forbidden. CI blocks changes if benchmarks report `allocs/op > 0`.

### Techniques used
*   **vtproto.** Reuse Protobuf structures via pools.
*   **Zero-copy parse.** Parse request parameters from byte slices of the original socket buffer.
*   **unsafe.String.** Convert bytes to strings without copying memory (under strict buffer lifetime control).
*   **Pre-bound metrics.** Use pre-created Prometheus counters. Dynamic label creation at runtime is forbidden.

---

## 3. Monotonic Time and Deadlines

System wall clock is not used for timeout calculations. On request entry a deadline is computed:
`deadline = monotonicNano() + timeout`

This keeps the system stable across NTP time jumps.

---

## 4. Compiler Analysis

See [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §2–3 for BCE patterns, branch reduction, and verification commands.

*   **BCE (Bounds-Check Elimination).** Use compiler hints to remove bounds checks in loops.
*   **Escape Analysis.** Ensure hot-path variables stay on the stack and do not escape to the heap.
*   **Inlining.** Keep functions simple so the compiler can inline them (reducing call overhead).
*   **ASM Analysis.** Use `go tool objdump` to verify hot loop logic. Watch for:
    *   `CALL runtime.morestack`: Indicates a non-inlined call or deep stack requirement.
    *   `CALL runtime.panicIndex`: Indicates failed BCE (Bounds Check Elimination).
    *   Register pinning: Ensure hot variables (counters, pointers) stay in registers (AX, CX, R8-R15) rather than flushing to the stack frame.

---

## 5. Data-Oriented Design (DoD)

Hot path performance is dominated by memory latency and CPU cache utilization (L1/L2).

### Structure of Arrays (SoA)
For processing candidate lists (e.g., RTB auctions), prefer SoA over AoS (Array of Structures).
*   **AoS (Bad):** `[]Campaign` where each campaign has 10 fields. Iterating to check one field (e.g., `DeviceMask`) pulls unnecessary data into the cache line.
*   **SoA (Good):** `type Registry struct { DeviceMasks []uint8; Bids []int64; ... }`. Iterating `DeviceMasks` packs 64 candidates into a single cache line (64 bytes).

### Pointer Chasing
Forbid nested pointers on the hot path. Every `ptr.Field` that is also a pointer incurs a potential L3/DRAM stall. Materialize all necessary data into flat slices during the cold catalog rebuild path.

---

## 6. Branch Prediction and Pipeline Density

See [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §3–4 for packed key matching, lookup tables, and padding examples (`ingress_quota.go`, `fraud_stream_queue.go`).

CPU execution pipelines stall when branch predictions fail (TAGE/BTB).

### Predictability
*   **Monotonicity.** Presort candidate buckets (e.g., by score or bid). This makes "early break" conditions (`if score < maxScore { break }`) highly predictable after the first failure.
*   **Bitwise Filtering.** Use bitmasks (`mask & req != 0`) instead of complex nested `if/else` or map lookups. Bitwise operations are data-dependent but branch-less or easily predicted.

### Eliminating Branches
*   **BCE.** A single window check `if end <= len(slice) { ... }` before a loop allows the compiler to remove the branch for `panicIndex` inside the loop.
*   **Lookup Tables.** Use small stack-fixed arrays or `[256]T` tables for mapping codes to weights to avoid conditional logic.

---

## 7. Atomic Operations and Synchronization

See [GUIDE_HOT_PATH_ZERO_ALLOC.md](../GUIDE_HOT_PATH_ZERO_ALLOC.md) §4 for cache-line padding rules and atomic snapshot patterns.

The `sync/atomic` package is essential for lock-free hot paths, but carries significant hardware-level risks.

### False Sharing
Adjacent atomic variables share the same CPU cache line (typically 64 bytes). Updating one causes the hardware to invalidate the line across all cores (**MESI protocol**), forcing a re-fetch even for unrelated variables.
*   **Bottleneck:** High latency on `Store` or `Add` operations even without logic contention.
*   **Methodic:** Use `cpu.CacheLinePad` or manual padding (`_ [56]byte`) between contended fields in global structs.

### Cache Line Invalidation (Thunder)
Frequent `atomic.Store` on a global variable (e.g., a shared configuration snapshot or budget) causes every reader core to evict that line from L1/L2.
*   **Bottleneck:** "Thunder" effect where readers stall on DRAM while waiting for the updated line.
*   **Methodic:** 
    *   **Compare-and-Swap (CAS).** Only write if the value changed.
    *   **Local Caching.** Read the atomic once into a local register and use that value throughout the loop.
    *   **Atomic Snapshots.** Use `atomic.Value` or `atomic.Pointer` to swap entire structures rather than updating individual fields.

### Atomic Loop Overhead
Using `atomic.Load` inside a tight loop (e.g., 1000 iterations) adds a hardware memory barrier on every cycle.
*   **Bottleneck:** Prevents the CPU from performing speculative execution and instruction reordering.
*   **Methodic:** Load the atomic variable **once** outside the loop. If consistent state is required within the loop (e.g., a shared budget), ensure the loop body is large enough to amortize the load cost.

---

## 8. Filter Engine (Go Layer)

Checks in `FilterEngine.Check` run before calling Redis:
1.  **Emergency Breaker.** Global traffic circuit breaker.
2.  **Fraud Check.** Primary IP check via MaxMind.
3.  **Geo Filter.** Geography check.
4.  **Schedule.** Campaign availability by time.
5.  **ML Boost.** Fetch anti-fraud coefficients from a state snapshot (0 allocations).
6.  **UnifiedFilter.** Invoke Lua scripts in Redis.

---

## 9. Runtime Risks

*   **GC pressure.** Garbage collection cost depends on total heap size and allocation rate. Even with 0 allocations on the hot path, cold goroutines can trigger STW pauses.
*   **RAM limits.** OOM is controlled via `GOMEMLIMIT`. Tracker and Processor should run in separate containers to isolate risk.
*   **Goroutine storm.** Redis delays increase blocked goroutines and stack memory usage. Mitigated by a hard filter timeout (100 ms) and circuit breakers.
