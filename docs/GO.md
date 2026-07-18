# Go Hot Path: Runtime, Allocations, and Internal Processes

Tracker traffic ingestion must strictly meet SLA targets: p95 < 50 ms, p99 < 80 ms. This document covers use of the `gnet` library, worker pools, zero-allocation policy, and compiler tuning.

---

## 1. gnet and PinnedWorkerPool

The standard `net/http` library creates one goroutine per request. At high RPS this increases scheduler and GC load.

### gnet event loop
*   Multiplexing via `epoll`. Fixed thread count (typically 2 per CPU core).
*   HTTP 1.1 parsing via a DFA scanner directly from the socket buffer without heap allocations.

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

*   **BCE (Bounds-Check Elimination).** Use compiler hints to remove bounds checks in loops.
*   **Escape Analysis.** Ensure hot-path variables stay on the stack and do not escape to the heap.
*   **Inlining.** Keep functions simple so the compiler can inline them (reducing call overhead).

---

## 5. Filter Engine (Go Layer)

Checks in `FilterEngine.Check` run before calling Redis:
1.  **Emergency Breaker.** Global traffic circuit breaker.
2.  **Fraud Check.** Primary IP check via MaxMind.
3.  **Geo Filter.** Geography check.
4.  **Schedule.** Campaign availability by time.
5.  **ML Boost.** Fetch anti-fraud coefficients from a state snapshot (0 allocations).
6.  **UnifiedFilter.** Invoke Lua scripts in Redis.

---

## 6. Runtime Risks

*   **GC pressure.** Garbage collection cost depends on total heap size and allocation rate. Even with 0 allocations on the hot path, cold goroutines can trigger STW pauses.
*   **RAM limits.** OOM is controlled via `GOMEMLIMIT`. Tracker and Processor should run in separate containers to isolate risk.
*   **Goroutine storm.** Redis delays increase blocked goroutines and stack memory usage. Mitigated by a hard filter timeout (100 ms) and circuit breakers.
