# Distributed Deduplication and Idempotency Key Generation (ESPX-ID-CORE)

This document defines the architecture and technical specifications for transactional key generation on the eSPX platform. Target metrics: p99 < 80 ms, hard limit 100 ms in a multi-region network.

---

## 1. Processing Pipeline and Filter Tiers

Request processing at the network edge follows this sequence:

1.  **Tier 1: L4/L7 filtering (eBPF/Nginx).** Drop traffic by IP blacklists and rate limits. Execution time: microseconds.
2.  **Tier 2: Token generation (Go ASM).** Assign each valid request a 64-bit key. Runs before heavy checks and database writes.
3.  **Tier 3: ML scoring and filters (Cold Path).** Run resource-intensive checks (LightGBM, Isolation Forest). The generated token serves as an end-to-end transaction identifier.

---

## 2. 64-Bit Token Specification (Go Assembly)

The key is a 64-bit integer assembled by the `ORQ` instruction in Go Assembly (AMD64) in a single CPU cycle.

### 2.1 Token Structure

| Component | Size | Purpose |
| :--- | :--- | :--- |
| **Region ID** | 4 bits | Region identifier (0–15). Prevents collisions across regional cells. |
| **UNIX Timestamp** | 40 bits | Time in milliseconds. Capacity ~34.8 years from platform epoch. |
| **Instance ID** | 10 bits | Tracker instance identifier (0–1023) within a region. |
| **Monotonic Counter** | 10 bits | Local counter (0–1023) of increments per millisecond per instance. |

Maximum throughput: 1,024,000 requests per second per pod.

### 2.2 Implementation (ASM)

Using `NOSPLIT` and direct register control (AX, BX, CX, DX) avoids stack and heap allocations. Assembly uses bit shifts (`SHLQ`) and logical OR (`ORQ`).

---

## 3. Buffer Sharding and Cache-Line Alignment

To avoid **false sharing** (cache-line bouncing) during parallel processing on different CPU cores, counters are segmented.

1.  **Core-local counters.** Each worker increments its own counter.
2.  **Padding (64 bytes).** Counter structures are padded to cache-line size (64 bytes), preventing L1 cache invalidation on neighboring cores.
3.  **Waterline Merge.** At a threshold of 10,000 requests, data is moved to a global ring buffer via atomic `LOCK XADD`.
4.  **Forced Flush (50 ms).** Timer-driven flush of buffer remainder to honor token lifetime limits.

---

## 4. Network Serialization and MTU

1.  **In-Memory.** Data in tracker memory is stored with alignment padding.
2.  **Network Boundary.** Padding is stripped on transfer (UDP/TCP).
3.  **MTU Efficiency.** A 1440-byte UDP packet holds exactly 180 raw 64-bit tokens. This avoids IP fragmentation at the standard 1500-byte MTU.

---

## 5. Concurrency and Idempotency

1.  **SET NX.** In Redis the token is used as a key in `SET NX` or `HSETNX` operations. On duplicate `click_id`, only the first operation succeeds; subsequent ones return a duplicate error.
2.  **Additive Delta.** Budget updates use atomic increments (`INCRBY`). This prevents data loss under concurrent writes (race conditions).
3.  **Config Epoch.** Redis configuration updates check a monotonic version (`epoch`). Events with `epoch` less than or equal to the current value are ignored.

---

## 6. Fault Tolerance and Postgres SPOF

1.  **SPOF Mitigation.** The hot path (Tracker/Redis) is isolated from Postgres. Postgres unavailability does not block incoming request processing.
2.  **Write-Ahead Log (mmap).** Events are written to a local spool file (mmap) before acknowledgment in the Redis Stream. This preserves data when Postgres or ClickHouse is unavailable.
3.  **Budget Reconciliation.** On Postgres–Redis mismatches, automatic reconciliation runs on the cold path. Postgres balance is authoritative (System of Record).
4.  **HA Topology (L3).** Sync Standby and PITR are required to minimize RPO/RTO.

---

## 7. Verification and Testing

- **T-ID-01 (Allocations).** Verify zero heap allocations on the hot path (`go test -benchmem`).
- **T-ID-02 (Collisions).** Test concurrent generation across 1024 threads to confirm no duplicates.
- **T-ID-03 (Clock Skew).** Verify generator behavior when system time steps backward (NTP skew).
- **T-ID-05 (Partial Flush).** Simulate crash during buffer flush and verify recovery via mmap logs.
