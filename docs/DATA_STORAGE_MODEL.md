# Data Storage and Processing Model Analysis in eSPX

This document provides a detailed technical analysis of the eSPX platform's data storage, replication, and processing architecture. It covers the end-to-end event path (Client → gnet → Redis → Broker → PostgreSQL/ClickHouse) and Control Plane management mechanisms.

---

## 1. End-to-End Data Path (Ingress to Storage)

The architecture is split into two isolated planes: **Hot Path** (a latency-sensitive real-time request processing loop with zero heap allocations) and **Cold Path** (asynchronous event persistence to databases, background analytics, and quota management).

```
[Client] 
   │
   ▼ (HTTP POST /track)
[gnet Event Loop] ──► (Zero-copy DFA Parse) ──► [PinnedWorkerPool] 
                                                    │
                                                    ▼
                                                [FilterEngine] (Go filters)
                                                    │
                                                    ▼ (EVALSHA)
                                                [Redis Shard (Lua)] ──(XADD)──► [Redis Stream]
                                                    │
                                                    ▼ (Success)
                                                [logger.Logger] 
                                                    │
                                                    ▼ (Write to active.log)
                                                [log-shipper]
                                                    │
                                                    ▼ (Produce TCP)
                                                [Custom Broker (mmap WAL)]
                                                    │
                                                    ▼ (Fetch TCP)
                                                [Processor (Cold Path)]
                                                 ├──► [PostgresStore] ──► PostgreSQL (OLTP)
                                                 └──► [ClickHouseStore] ──► ClickHouse (OLAP)
```

1. **Request intake (gnet)**: Client requests are accepted on the `gnet` network engine (uses `epoll`/`kqueue` and fixed event-processing loops). HTTP header and body parsing is zero-copy via a deterministic finite automaton (DFA) directly from the socket ring buffer.
2. **Thread distribution (PinnedWorkerPool)**: The request is handed to a worker pool where the execution thread is pinned to the `campaign_id` hash. This keeps data localized in L1/L2 CPU cache and avoids false sharing of cache lines.
3. **Filtering (FilterEngine)**: A chain of Go filters performs fast static checks (license, in-memory MaxMind geo lookup, campaign schedule, local blocklists, ML boost coefficients).
4. **Validation and budget debit (Redis Lua)**: An atomic `unified-filter.lua` script runs on the appropriate Redis shard.
5. **Disk logging (logger.Logger)**: After filters pass, the transaction is written to the local segmented log file `/var/log/espx/active.log` on the tracker disk.
6. **Broker delivery (log-shipper)**: The async `log-shipper` utility reads local log segments and sends events to the distributed message broker (`pkg/broker/server`).
7. **Asynchronous persistence (Processor)**: The `processor` service reads messages from broker partitions and performs batch writes to PostgreSQL and ClickHouse.

---

## 2. Redis Lua Performance Analysis (Unified Filter)

### Is Lua a bottleneck?
In the current implementation, the Lua script is **not** a system bottleneck. Performance comes from these architectural choices:

1. **No blocking operations**: The `unified-filter.lua` script uses only non-blocking commands with $O(1)$ or $O(\log N)$ time complexity for small $N$: `MGET`, `GET`, `INCR`, `SET NX`, `XADD`, `SADD`, `EXPIRE`, `INCRBY`. Commands such as `KEYS` and blocking wait operations are forbidden.
2. **RTT (Round Trip Time) minimization**: All checks (idempotency, frequency cap fcap, time-to-click TTC, daily and total budget, rate limit) and state mutations (balance debit, adding campaign ID to the dirty set, writing the event to a Redis Stream via `XADD`) are packed into **one network round trip** (`EVALSHA`).
3. **Atomicity without locks**: Because Redis executes commands single-threaded, the script guarantees linearizable budget-debit transactions without distributed locks (mutex/redlock) or optimistic transactions with retries.
4. **Local quota mechanism (Phase 1.3 Quotas)**: To reduce load at high request rates (RPS), campaigns receive local quotas (budget chunks) via `budget:quota`. The Lua script debits from the shard's local quota. When the remainder falls below a threshold (`refill_threshold_pct`), the script sets a non-blocking `budget:refill_lock` and enqueues the campaign on `budget:refill_needed`. This prevents overloading the Redis master.
5. **Upstream short-circuiting**: Invalid or fraudulent traffic is dropped at eBPF/XDP and `FilterEngine` before Redis. The network round trip to the Lua script happens only for legitimate requests.

---

## 3. Redis Sharding Strategy

The system uses client-side sharding based on a fixed slot table.

### Sharding algorithm (StaticSlotSharder)
```
campaign_id (UUID) ──► crc32Castagnoli ──► slot = hash & 1023 ──► slot_table[1024] ──► shard_id (0..3)
```

1. **Hashing**: A checksum is computed from the campaign's 16-byte UUID using hardware-accelerated CRC32 (SSE4.2 `CRC32` instruction on `amd64`).
2. **Slot mapping**: The value is projected onto 1024 virtual slots with a bit mask (`hash & 1023`).
3. **Routing table (slotTable)**: A `[1024]uint8` array maps each slot to a physical Redis shard index (by default 4 independent Master instances).
4. **Hot Path thread safety**: The slot table is wrapped in `atomic.Value` (`SlotMapSnapshot`). `GetShard` runs without mutex locks in **~5.7 ns** with zero heap allocations.
5. **Key co-location (hash tags)**: All keys for a single campaign (budget, limits, event stream, migration flags) use curly braces with the campaign UUID (e.g. `{campaign_uuid}:budget`, `{campaign_uuid}:stream`). This ensures all related keys land on the same Redis instance, avoiding multi-key Lua execution errors.

### Slot migration
When scaling the cluster, data movement between shards uses `SlotMigrationOrchestrator`:
1. A global migration barrier is set in Redis (`budget:migration_fence`).
2. Debit operations for the migrating slot are temporarily blocked on trackers (returns code `11 debit fenced`).
3. Data is copied from the source shard to the target with `DUMP` / `RESTORE`.
4. The slot table in PostgreSQL is updated; the new state is applied atomically on trackers via `SwapSnapshot`.
5. The migration barrier is cleared and traffic redistribution completes.

---

## 4. Broker Replication Strategy

The custom distributed log (`pkg/broker/server`) uses **Leader-Follower** replication coordinated through Redis.

1. **Partition leader election (lease election)**:
   - Leadership for each topic/partition is acquired by taking a time-bounded lease in Redis: key `espx:topics:{topic}:leader` is created with `SETNX`, value `nodeID`, and a TTL.
   - The leader renews the lease in a background `runHeartbeatLoop`.
   - On leader change, epoch `espx:topics:{topic}:leader_epoch` is atomically incremented (fencing token). Stale leaders are rejected via epoch checks on write.
2. **Data writes (mmap segment append)**:
   - The leader accepts messages over TCP, appends them to local memory-mapped log segments (`mmap`), and updates High-Water Mark `espx:topics:{topic}:log_hwm` in Redis.
3. **Subscriber replication (follower tailing)**:
   - Follower brokers run a `replicate` goroutine that reads the current partition leader address from Redis, opens a direct TCP connection, and requests data batches (`Fetch`) starting from local offset `NextOffset()`.
   - Data is appended to the follower's local log via `AppendReplicatedAt`. On offset gaps, replication stops with `ErrReplicationGap` to prevent data corruption.
4. **Failover recovery**:
   - When the leader is lost (Redis lease expires), followers start a new election. The new leader increments the epoch.
   - For 5 seconds (`recoverLeaderReadiness`) the new leader does not accept writes while its local log catches up to the Redis-published `log_hwm`. After the timeout it forcibly sets `log_hwm` to the local offset and opens for writes.

---

## 5. Final Database Storage Model

### OLTP model (PostgreSQL)
Serves as the financial source of truth (client balances, campaign settings, aggregated advertiser statistics).

* **Deduplication and partitioning schema**:
  - Table `events` is partitioned by day: `PARTITION BY RANGE (created_date)`.
  - Unique index `click_id + created_date` guarantees deduplication within a daily partition.
* **Batch writing**:
  - Method `InsertEventsBatch` accepts column slices built without boxing values into interfaces, avoiding heap allocation.
  - Writes use a CTE that returns inserted rows:
    ```sql
    WITH inserted AS (
        INSERT INTO events (...) VALUES (...)
        ON CONFLICT (click_id, created_date) DO NOTHING
        RETURNING campaign_id, event_type, created_date
    )
    ```
  - Returned rows are aggregated and atomically increment counters in `campaign_stats` (`ON CONFLICT DO UPDATE SET...`). This gives exactly-once protection against double-counting when the broker retries failed batches.
* **Spend synchronization (SyncWorker)**:
  - Background worker `SyncWorker` reads accumulated debits from Redis dirty sets (`budget:dirty_campaigns`).
  - Via Lua script `prepareSyncScript`, the debit amount is moved to temporary storage `budget:inflight:campaign:{campaign_id}` under local lock `budget:lock:...`.
  - Spend is written to Postgres (`UpdateSpendBatch`). On success, `commitSyncScript` clears inflight entries in Redis. On insufficient balance, the campaign is set to `PAUSED` in PostgreSQL and the change is propagated back to Redis slots.

### OLAP model (ClickHouse)
Used for real-time analytics, fraud detection (IVT), and machine learning model training.

* **Entity separation**:
  - The processor splits incoming events by type into separate tables: `impressions`, `clicks`, `conversions`, `fraud_events`.
* **Batch accumulation**:
  - Events are buffered (batch size `CHBatchSize`, flush timeout `CHFlushIntervalMs`) and written in large blocks. This avoids ClickHouse LSM tree fragmentation (the "too many parts" problem).
* **Hardware deduplication**:
  - To guard against duplicates on network failures, ClickHouse block deduplication is used: inserts run with `insert_deduplicate=1` and a unique `insert_deduplication_token` computed as SHA-256 over `click_id` values and event timestamps in the batch.
* **Write resilience (CHSpool WAL)**:
  - When ClickHouse is unavailable, batches go to `CHSpool` — a local write-ahead log based on memory-mapped files (`mmap`).
  - On reconnect, background replayer `replaySpoolOnce` reads WAL records, sends them to ClickHouse, and frees disk segments on the tracker. This keeps the event pipeline from blocking on transient OLAP outages.
