# eSPX Design Technical Guide: System Concepts and Mechanics

This document describes fundamental principles for designing high-performance systems, system calls, memory and network behavior, and runtime environments, illustrated with concrete examples from the **eSPX** architecture.

The entire eSPX architecture is built on **Data-Oriented Design (DOD)** — design centered on data layout, memory locality, and efficient CPU processing, avoiding unnecessary abstraction layers.

---

## 1. System Resources and Latency

Designing high-performance systems requires a deep understanding of hardware physical limits. The latency table (current as of 2026, Zen 5 / Emerald Rapids architectures, PCIe Gen 5, DDR5) shows the time gaps between memory hierarchy levels and the network.

| Operation | Time (ns) | CPU cycles (approx.) | Relative cost (scale: 1 ns = 1 sec) |
| :--- | :--- | :--- | :--- |
| L1 cache access (32–48 KB/core) | ~1 ns | 4 | 1 sec |
| Branch mispredict | ~3 ns | 12 | 3 sec |
| L2 cache access (1–2 MB/core) | ~4 ns | 16 | 4 sec |
| L3 cache access (shared, 32–96 MB) | ~12 ns | 48 | 12 sec |
| Atomic CAS (intra-core) | ~15 ns | 60 | 15 sec |
| Syscall via vDSO (`clock_gettime`) | ~20 ns | 80 | 20 sec |
| Atomic CAS (cross-core, NUMA) | ~80 ns | 320 | 1.3 min |
| Main memory access (DDR5 DRAM, local) | ~100 ns | 400 | 1.6 min |
| Syscall (typical User → Kernel transition) | ~200 ns | 800 | 3.3 min |
| Cross-socket NUMA transition (DRAM cross-socket) | ~180 ns | 720 | 3 min |
| OS thread context switch | ~1,500 ns | 6,000 | 25 min |
| Random 4 KB read from NVMe SSD Gen 5 | ~15,000 ns | 60,000 | 4 hours |
| Sequential 1 MB read from DDR5 RAM | ~20,000 ns | 80,000 | 5.5 hours |
| Network request within one rack (LAN) | ~150,000 ns | 600,000 | 1.7 days |
| Network request within one DC (TCP) | ~300,000 ns | 1,200,000 | 3.5 days |
| Request between Availability Zones (cross-AZ, WAN) | ~1,500,000 ns | 6,000,000 | 17 days |
| Network request between coasts (US-E → US-W) | ~65,000,000 ns | 260,000,000 | 2 years |
| Network request across ocean (US → EU → US) | ~100,000,000 ns | 400,000,000 | 3.1 years |

### SLA Risks and eSPX Time Budgets
Our hard request-processing limit on the tracker is **100 ms** (100,000,000 ns), with a target p99 of **80 ms**.

*   **Abstract risk:** A single cross-region WAN request completely destroys our time budget. Every context switch or extra syscall on the hot path reduces peak CPU RPS.
*   **Concrete eSPX example:**
    In the `/track` handler (`cmd/tracker` service) we must check campaign budget, deduplicate clicks, and run fraud scoring.
    If the tracker makes a **synchronous (blocking) gRPC call** to the billing service in another region (for example, from a Frankfurt DC to a New York DC), network RTT is roughly ~75 ms (75,000,000 ns). Together with packet parsing and serialization we spend ~80 ms just waiting on the network. Less than 20 ms remains for local checks, logging, and the client response, which immediately pushes us past the p99 SLA (80 ms) and toward the hard 100 ms timeout with any network jitter.
    *Solution:* Regional budget caches in local Redis shards, async replication from Postgres, and local deduplication via fast key generation (single-cycle token in ASM).

---

## 2. Go Runtime and Linux Internals

### 2.1 System Calls (Syscalls) and Their Cost
Every system call (for example, `read`, `write`, `epoll_wait`) forces the CPU to execute the `syscall` instruction (on x86_64). This leads to:
1.  Switching the CPU from ring 3 (User Space) to ring 0 (Kernel Space).
2.  Saving general-purpose registers onto the kernel stack.
3.  Switching to the kernel stack.
4.  Validating permissions and arguments.
5.  Potential TLB (Translation Lookaside Buffer) flush due to page-table switching (with active KPTI for Meltdown mitigation).
6.  Switching back and restoring context.

*   **Abstract solution:** Use **batching** — coalesce operations. Instead of writing each event to disk or sending it over the network individually, data accumulates in memory and is flushed in one transaction or one system operation.
*   **Concrete eSPX example:**
    The `cmd/processor` service reads click and impression events from a message queue and writes them to the ClickHouse analytics database.
    *   *What NOT to do (Anti-pattern):* Write each event individually. For every 100,000 clicks we perform 100,000 `write()` calls to the ClickHouse network socket. The CPU spends 95% of its time on User/Kernel context switches (100k × 200 ns = 20 ms of pure overhead on kernel transitions alone, excluding network work and ClickHouse parsing).
    *   *How eSPX does it (Pattern):* Processor accumulates events in an internal ring buffer until a waterline is reached (for example, 10,000 events) or a 50 ms timeout fires. The buffer is then serialized into a ClickHouse RowBinary stream and sent in **one** `write()` syscall. Syscall cost drops by 10,000×.

---

### 2.2 Linux Scheduler (CFS / EEVDF)
Modern Linux kernels (starting with 6.6) use the **EEVDF** (Earliest Eligible Virtual Deadline First) scheduler.
*   EEVDF distributes CPU time based on each thread's virtual deadline.
*   If a thread frequently blocks (for example, on synchronous I/O or mutexes), the scheduler moves it to `TASK_INTERRUPTIBLE` (sleep) and context-switches to another thread.
*   When the blocked thread wakes up, it may be scheduled on a different free CPU core. This causes **Thread Migration** — moving the thread between cores, which fully invalidates the current core's L1/L2 caches and triggers cascading cache misses.

*   **Abstract solution:** Keep hot caches warm by pinning threads to physical CPU cores.
*   **Concrete eSPX example:**
    In the tracker (`cmd/tracker`) it is critical to compute campaign hashes and check local caches as fast as possible (caches that fit in the processor's multi-megabyte L2/L3 cache).
    *   *Problem without optimization:* Worker goroutines handle traffic. If the Go scheduler moves a goroutine from thread `M1` (running on Core 0) to thread `M2` (running on Core 8), the Linux EEVDF scheduler migrates the thread. `CampaignRegistry` structures (~1.5 MB) that lived in Core 0's L2 cache are absent from Core 8's L2 cache. The CPU must fetch them from slow RAM (100 ns latency instead of 4 ns). Campaign check time increases from 10 µs to 120 µs.
    *   *eSPX solution:* At tracker startup, `PinnedWorkerPool` workers call `runtime.LockOSThread()`. This permanently binds the goroutine to OS thread `M`. At the OS level, deploy scripts in `scripts/edge-tuning` set CPU affinity for the process (for example, via `taskset -c 0-3 ./tracker`), binding threads `M` to physical cores `0, 1, 2, 3`. L1/L2 cache always stays hot.

---

### 2.3 Go Scheduler (G-M-P Model)
The Go runtime uses its own scheduler on top of OS threads.

**Scheduler flow:**
Logical processor (P) → acquires OS thread (M) → selects and runs goroutine (G).

**Task distribution mechanics:**
Local P queue (G1, G2...) → if empty → Work Stealing (steal from another P) → if empty → Global goroutine queue.

*   **Work Stealing:** Logical processor `P` takes goroutines from its local queue. If empty, it steals from a neighbor or hits the global queue (under a mutex).
*   **Sysmon (System Monitor):** A separate OS thread without a `P`. It detects goroutines blocked in syscalls for more than 10 ms, releases logical processor `P`, and hands it to another free thread `M`.
*   **Netpoller:** Go scheduler integration with `epoll` on Linux. Threads do not block at the OS level while waiting on the network.

*   **Abstract risk:** The standard "one goroutine per connection" model (as in `net/http`) at high traffic volume creates massive scheduling overhead and goroutine stack allocation.
*   **Concrete eSPX example:**
    At peak load with 100,000 concurrent Keep-Alive connections from Edge servers:
    *   *Standard stdlib `net/http` problem:* The Go runtime creates a separate goroutine `G` per connection. 100,000 goroutines require at least 200 MB of memory for base stack sizes alone (2 KB each). The scheduler burns huge CPU cycles switching between these goroutines, and the garbage collector must scan their stacks, degrading p99 to 150–200 ms.
    *   *eSPX solution:* Use the `gnet` library in `cmd/tracker`. `gnet` does not spawn goroutines per connection. It runs a strictly fixed number of worker threads (equal to CPU core count). Each worker listens on its own `epoll` instance and processes incoming bytes from system sockets directly in an event loop, using non-blocking I/O and reusable connection buffers (`connContext`). Memory usage stays stable (~10 MB), and Go scheduler overhead is effectively zero.

---

### 2.4 Linux Network Stack: NAPI and softirqd
Packet path from NIC to application:
1.  **DMA Transfer:** The network interface card (NIC) writes the packet directly into host RAM (Rx Ring Buffer) via DMA.
2.  **Hard IRQ:** The NIC raises a hardware interrupt to the CPU.
3.  **NAPI (New API):** The driver disables interrupts and switches to polling mode, protecting against interrupt storms.
4.  **softirqd:** The kernel background thread `ksoftirqd` polls the NIC, wraps data in a kernel `sk_buff` structure, and passes it up the protocol stack (IP → TCP/UDP).
5.  **Socket Buffer:** The packet lands in the socket receive buffer. The netpoller wakes the goroutine, which reads data into application address space via a non-blocking syscall.

*   **Concrete eSPX example:**
    When Edge Nginx forwards a click event to our tracker on port `:8181`, the network packet passes through the Intel X550 (10GbE) NIC Rx Ring Buffer.
    Thread `ksoftirqd/0` (pinned to core 0) reads the packet, processes the TCP header, verifies the checksum, and places data in the Socket Receive Buffer. Our tracker, running `gnet` on core 0, immediately receives notification via `epoll_wait` (already waiting on the same core), avoiding cross-core data migration and minimizing network stack latency to ~10 µs.

---

## 3. Hot Path Risks

On the hot processing path (`/track` requests at RPS > 100k per node), any suboptimal code construct degrades performance due to heap allocations and scheduler blocking.

### 3.1 Heap Allocations and Garbage Collection (GC)

#### 1. String concatenation via the `+` operator
*   **Explanation:** Strings in Go are immutable. Any `str1 + str2` concatenation allocates a new heap region for the result string and copies data.
*   **Concrete eSPX example (Redis key generation):**
    We need to generate a click deduplication key using the pattern `dedup:<campaign_id>:<click_id>`.
    ```go
    // ANTI-PATTERN: Causes 3 heap allocations
    key := "dedup:" + strconv.Itoa(int(campaignID)) + ":" + clickID
    ```
    At 100,000 RPS this code generates 300,000 heap objects per second, overwhelming the garbage collector.
    ```go
    // eSPX PATTERN (Zero-allocation):
    // Use a byte buffer from sync.Pool or a stack-allocated fixed-size array
    var buf [128]byte
    b := buf[:0]
    b = append(b, "dedup:"...)
    b = strconv.AppendUint(b, uint64(campaignID), 10)
    b = append(b, ':')
    b = append(b, clickID...)
    
    // unsafe conversion to string without heap allocation
    key := unsafe.String(&b[0], len(b))
    ```

#### 2. Using the `fmt` package and reflection
*   **Explanation:** `fmt.Sprintf` accepts arguments as `interface{}`, causing boxing (heap allocation of values). Format strings are parsed at runtime via reflection.
*   **Concrete eSPX example (Tracker response formatting):**
    The tracker must return a JSON response with a redirect.
    ```go
    // ANTI-PATTERN: Allocates interface wrappers, parses "%s", allocates result on heap
    response := fmt.Sprintf(`{"status":"ok","redirect":"%s","cost":%d}`, url, cost)
    ```
    ```go
    // eSPX PATTERN (Zero-allocation):
    // Precompiled templating or manual assembly into []byte
    type TrackResponse struct {
        Status   string `json:"status"`
        Redirect string `json:"redirect"`
        Cost     uint64 `json:"cost"`
    }
    // Use a fast serializer that generates code without reflection (easyjson or vtproto for protobuf)
    ```

#### 3. Using `context.Context` (Nested contexts)
*   **Explanation:** Contexts are singly linked lists. `WithValue` allocates a struct on the heap. Key lookup is a list walk with type checks — O(N). `WithTimeout` starts heavy system timers.
*   **Concrete eSPX example (Passing parameters through filters):**
    In `FilterEngine` we need to pass user IP, campaign ID, and anti-fraud flag through 10 different filters.
    ```go
    // ANTI-PATTERN: Allocates on every step and linear search deep in filters
    ctx = context.WithValue(ctx, "ip", userIP)
    ctx = context.WithValue(ctx, "campaign_id", campaignID)
    ```
    ```go
    // eSPX PATTERN (Zero-allocation):
    // Pass a flat FilterContext struct by pointer. Allocated on the worker stack.
    type FilterContext struct {
        UserIP     uint32
        CampaignID uint32
        IsBot      bool
        Deadline   uint64 // Monotonic time in ns
    }
    // Filter signature:
    func CheckGeoFilter(fCtx *FilterContext) bool { ... }
    ```

---

### 3.2 Channel Usage Risks
*   **Explanation:** Under the hood, a channel is an `hchan` struct with an internal mutex `hchan.lock`. Every send or receive requires a CPU bus lock. On overflow or empty, the runtime parks the goroutine via `gopark`, causing a context switch.
*   **Concrete eSPX example (Passing events from tracker to processor):**
    We need to pass parsed click events from `cmd/tracker` goroutines to a background buffer for database delivery.
    ```go
    // ANTI-PATTERN: CPU bottleneck on channel mutex locks at RPS > 50k
    eventChan := make(chan *Event, 100000)
    func OnRequest(e *Event) {
        eventChan <- e // All workers compete for eventChan.lock
    }
    ```
    ```go
    // eSPX PATTERN (Lock-free):
    // Use sharded non-blocking ring buffers (MPSC Ring Buffer)
    // based on sync/atomic pointer operations.
    type RingBuffer struct {
        writeCursor uint64
        readCursor  uint64
        storage     [1024]unsafe.Pointer
    }
    // Worker writes event via atomic cursor increment:
    // index := atomic.AddUint64(&buf.writeCursor, 1) % 1024
    // atomic.StorePointer(&buf.storage[index], unsafe.Pointer(event))
    ```

---

## 4. Atomics, Cache Lines, and Coherency

### 4.1 Cache Coherency (MESI) and Cache-line Bouncing
Processor memory is divided into 64-byte cache lines. The MESI protocol maintains consistency between cores.

**Event flow on data modification:**
Core 0 (write to cache line) → Data bus → Invalidate signal → Cores 1, 2, 3 (line marked Invalid) → On read attempt: Cache miss → Fetch from L3 cache or DRAM.

### 4.2 False Sharing
Occurs when logically unrelated variables from different threads physically reside in the same 64-byte cache line.

*   **Concrete eSPX example (Worker statistics counters):**
    In `CampaignRegistry` we collect processed-click statistics per worker for load balancing.
    ```go
    // ANTI-PATTERN: All counters sit in one cache line (8 workers × 8 bytes = 64 bytes)
    type BadTrackerStats struct {
        WorkerClicks [8]uint64
    }
    ```
    When Worker 0 on Core 0 does `atomic.AddUint64(&stats.WorkerClicks[0], 1)` and Worker 1 on Core 1 does `atomic.AddUint64(&stats.WorkerClicks[1], 1)`, the hardware subsystem continuously shuttles the cache line between cores (**Cache-line Bouncing**). Atomic performance drops 15–20× due to constant cache misses.

    ```go
    // eSPX PATTERN (Cache-line alignment):
    type ShardedCounter struct {
        val uint64
        pad [56]byte // Pads struct to 64 bytes (cache line size)
    }
    
    type GoodTrackerStats struct {
        WorkerClicks [8]ShardedCounter
    }
    ```
    Each counter now guaranteedly occupies its own cache line. CPU cores increment their counters at L1 cache speed (~1 ns) fully in parallel.

---

## 5. Signals and Graceful Shutdown

When updating or restarting eSPX services (for example, on `SIGTERM` from Kubernetes) it is critical to finish transactions cleanly and not lose events sitting in in-memory buffers.

### 5.1 OS Signals and Data Loss Risks
*   **SIGTERM (15):** Caught by the application. Allows time to flush buffers.
*   **SIGKILL (9):** Forced process termination. RAM buffers vanish without trace.

### 5.2 eSPX Solution: mmap-backed WAL (Write-Ahead Log)
To provide data durability without performance loss from `write()` syscalls, eSPX uses memory-mapped files.

**Mechanics and synchronization:**
1. Application (Worker) → Write to mmap buffer (RAM) → Instant return (OK).
2. Linux kernel (Page Cache) → Automatic mapping of memory pages to disk.
3. OS scheduler → Async flush (fsync) → Physical storage.

**Crash scenario (SIGKILL):**
Process destroyed → Data remains in kernel Page Cache → Kernel guaranteedly writes it to disk.

*   **Concrete eSPX example (WAL implementation in `pkg/broker`):**
    On click receipt the tracker must record the event in the WAL before responding `HTTP 200 OK` to the client.
    *   *How it works:*
        At startup the service opens file `data/wal/segment_001.bin` sized 128 MB and calls `mmap`:
        ```go
        // Map file directly into process address space
        fd, _ := os.OpenFile("data/wal/segment_001.bin", os.O_RDWR, 0644)
        data, _ := syscall.Mmap(int(fd.Fd()), 0, 128*1024*1024, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
        ```
        On request handling the worker writes the click structure directly into slice `data` at an atomically advanced offset:
        ```go
        offset := atomic.AddUint64(&wal.writeOffset, uint64(eventSize)) - uint64(eventSize)
        copy(data[offset:], serializedEvent)
        ```
        This runs at RAM write speed (~10–20 ns, zero syscalls). Linux kernel Page Cache handles syncing pages to physical disk.
        
        *If the process is killed with `SIGKILL`:* The eSPX process virtual memory is destroyed, but physical pages in the OS kernel Page Cache remain active. The kernel guaranteedly writes these dirty pages to physical disk. On restart the tracker reads `segment_001.bin` from disk and fully recovers all events.

---

## 6. Deep Dive into Databases: PostgreSQL and ClickHouse

eSPX splits storage by workload type (OLTP vs OLAP): Postgres handles transactional financial data; ClickHouse handles analytics telemetry.

### 6.1 PostgreSQL: Write Model, MVCC, and Locking

#### 1. Disk write model and MVCC (Multi-Version Concurrency Control)
*   **Explanation:** Postgres does not update rows in place. Any `UPDATE` creates a new physical row version (tuple). The old version is marked dead (`xmax` filled with the deleting transaction ID) and remains on disk until the background `VACUUM` process runs. This causes data file **Bloat** and excess disk writes (**Write Amplification**).
*   **Concrete eSPX example (Updating campaign balance in `balance_ledger`):**
    We have a campaign with a $1000 balance. 1,000 clicks per second each deduct $0.05.
    *   *Problem without optimization:* Executing 1,000 separate queries:
        ```sql
        UPDATE campaigns SET balance = balance - 0.05 WHERE id = 42;
        ```
        In one second Postgres creates 1,000 new row versions on disk for the campaign. The table bloats, and `VACUUM` loads the disk subsystem (I/O bottleneck) trying to purge old row versions.
    *   *eSPX solution:* On the hot path the tracker deducts balances locally in Redis. Once per second (or on spend waterline) `cmd/processor` aggregates all spend per campaign into one batch (for example, total deduction of $50.00 for 1,000 clicks) and executes **one grouped update** in Postgres, minimizing dead row generation.

#### 2. Budget race risks and transactions
*   **Concrete eSPX example (Parallel deduction transactions):**
    Two workers concurrently process clicks for campaign #42. Current balance is $0.08. Click cost is $0.05. The campaign should stop after the first click because there is not enough money for a second.
    
    *Scenario under **Read Committed** isolation (Anti-pattern):*
    1.  Transaction A (Worker 1) reads balance: `SELECT balance FROM campaigns WHERE id = 42` → returns `$0.08`.
    2.  Transaction B (Worker 2) concurrently reads balance: `SELECT balance FROM campaigns WHERE id = 42` → returns `$0.08`.
    3.  Transaction A subtracts `$0.05`, gets `$0.03`, writes: `UPDATE campaigns SET balance = 0.03 WHERE id = 42;` and commits.
    4.  Transaction B subtracts `$0.05`, gets `$0.03`, writes: `UPDATE campaigns SET balance = 0.03 WHERE id = 42;` and commits.
    *Result:* Balance became `$0.03` instead of going negative. We got a **Lost Update** and served two clicks totaling `$0.10` on a `$0.08` budget (overspend).

    *eSPX solution (DOD patterns):*
    1.  **Atomic conditional update:**
        ```sql
        UPDATE campaigns SET balance = balance - 0.05 WHERE id = 42 AND balance >= 0.05;
        ```
        If balance is less than cost, the update returns `0` affected rows. The application immediately records budget exhaustion.
    2.  **Idempotent balance ledger write:**
        Instead of directly updating balance in the campaign table, we write each deduction transaction to a detailed log with a unique click key (`click_id`):
        ```sql
        INSERT INTO balance_ledger (campaign_id, amount, click_id) 
        VALUES (42, -0.05, 'click_uuid_123')
        ON CONFLICT (click_id) DO NOTHING;
        ```
        Total balance is computed as `SUM(amount)` over the log. A repeated click with the same `click_id` never deducts money twice.

---

### 6.2 ClickHouse: Write Model, Data Scattering, and MergeTree

#### 1. Write model (MergeTree) and "Too many parts"
*   **Explanation:** Each ClickHouse insert forms a separate directory (part) on disk. A background process merges them into larger parts. Sending small inserts causes part count to exceed the limit (usually 300), and ClickHouse blocks new data with `Too many parts`.
*   **Concrete eSPX example (Click log inserts):**
    We need to store 50,000 clicks per second in the `clicks` table.
    *   *Problem without batching:* Sending clicks one-by-one as they arrive. In one second ClickHouse tries to create 50,000 tiny data parts on disk. The server dies in the first second, blocking writes.
    *   *eSPX solution:* `cmd/processor` uses buffering. Click events are packed into batches of 20,000 rows in memory and sent as a single `INSERT`. Exactly one part is created on disk per operation. ClickHouse server settings are tuned for grouped inserts:
        ```sql
        SET async_insert = 1;
        SET async_insert_busy_timeout_ms = 200;
        ```

#### 2. Data scattering (Columnar Scattering) and Point Selects
*   **Explanation:** In columnar databases each column is stored in a separate file set. Reading one row with all columns requires opening all files on disk, which is extremely inefficient.
*   **Concrete eSPX example (Analytical query vs Point query):**
    Our `clicks` table has 80 columns (geo, device, IP, browser, campaign parameters, referrer, etc.).
    *   *What NOT to do (Anti-pattern):* Trying to fetch full information for one click to display in the admin UI:
        ```sql
        -- Terribly slow: forces ClickHouse to open 80 separate files on disk!
        SELECT * FROM clicks WHERE click_id = 'click_uuid_123' LIMIT 1;
        ```
    *   *Correct approach (eSPX pattern):* Analytical conversion calculation by campaign:
        ```sql
        -- Blazing fast: reads files for ONLY two columns (campaign_id and cost)
        SELECT campaign_id, SUM(cost) 
        FROM clicks 
        WHERE click_time >= today() 
        GROUP BY campaign_id;
        ```
        For point lookup of a specific click by ID in the admin UI, data is duplicated on the cold path in Postgres, which is ideally suited for row-by-row reads by primary key.

---

## 7. File Descriptor Exhaustion

A file descriptor (FD) is a numeric index in a Linux process open-file table referencing sockets, files, or pipes.

**Example eSPX process descriptor table:**
*   FD 0, 1, 2: Standard streams (stdin, stdout, stderr).
*   FD 3: TCP socket for incoming Edge connection (8181).
*   FD 4: mmap WAL file descriptor on disk.
*   FD 5: Redis connection (6479).
*   FD 6: epoll instance for event monitoring.

### 7.1 Causes
1.  **Connection leaks:** Unclosed HTTP response bodies or gRPC clients.
2.  **Hung connections:** Slow clients (Slowloris attack) holding sockets open due to missing server timeouts.
3.  **Uncontrolled database pool growth.**

### 7.2 Consequences
When the limit is reached (for example, the default process limit of 1024 FDs) the kernel returns `EMFILE: Too many open files`. The eSPX server stops accepting new TCP connections (`accept` returns an error), cannot open log files, and fully stops working.

### 7.3 Concrete eSPX Example and Solution
*   **Descriptor leak scenario:**
    In the `cmd/notifier` microservice (responsible for Telegram notifications when campaign balance drops) a developer makes an HTTP client mistake:
    ```go
    // ANTI-PATTERN: Response body not closed; socket stuck in ESTABLISHED/CLOSE_WAIT
    resp, err := http.Post("https://api.telegram.org/bot...", "application/json", body)
    if err != nil { return err }
    // Missing: defer resp.Body.Close()
    ```
    During mass notification dispatch (1,000 campaigns went into overspend) the service instantly consumes all 1,024 available file descriptors and crashes, blocking the entire gRPC notification chain.

*   **eSPX engineering solution:**
    1.  **Static connection pools with timeouts:**
        ```go
        var secureHTTPClient = &http.Client{
            Transport: &http.Transport{
                MaxIdleConns:        100,
                MaxIdleConnsPerHost: 10,
                IdleConnTimeout:     30 * time.Second,
            },
            Timeout: 5 * time.Second, // Hard timeout on entire request
        }
        ```
    2.  **Raising process system limits in systemd (`/etc/systemd/system/espx-tracker.service`):**
        ```ini
        [Service]
        LimitNOFILE=1048576
        ```
    3.  **Monitoring instrumentation:**
        Worker code periodically counts descriptors by reading `/proc/self/fd/`:
        ```go
        func GetAllocatedFDs() int {
            files, err := os.ReadDir("/proc/self/fd")
            if err != nil { return 0 }
            return len(files)
        }
        ```
        The metric is exported to Prometheus as `espx_allocated_fds`. When it exceeds 80% of the limit, a P0 alert fires.

---

## 8. Execution Environments: Lua, C, eBPF/XDP

### 8.1 Lua Architecture in Redis
Redis is a single-threaded server. Executing a Lua script (`EVALSHA`) fully blocks the Redis thread. All other clients wait until the script completes.

*   **Concrete eSPX example (Filtering and budget deduction):**
    Campaign budget deduction in Redis must be atomic.
    *   *What NOT to do (Anti-pattern):*
        Write a complex Lua script that loops over a million keys to find inactive campaigns:
        ```lua
        -- Terrible Lua code: blocks Redis for several seconds!
        local keys = redis.call('keys', 'campaign:*:status')
        for _, key in ipairs(keys) do
            -- some complex logic
        end
        ```
        While this script runs, all 4 eSPX tracker instances stop getting responses from that Redis shard. gnet buffers overflow and we get a cascading failure with 100% SLA violation.
    *   *How eSPX implements it (Pattern):*
        The `unified_filter.lua` script performs only atomic point operations in O(1):
        ```lua
        -- eSPX script (Fast O(1) pass):
        local budget_key = KEYS[1]
        local cost = tonumber(ARGV[1])
        local current_budget = tonumber(redis.call('GET', budget_key) or '0')
        if current_budget >= cost then
            redis.call('DECRBY', budget_key, cost)
            return 1 -- Deduction successful
        end
        return 0 -- Budget exhausted
        ```
        Such a script executes in less than 10 microseconds on the CPU (p99 < 10 µs), guaranteeing uninterrupted operation of the single-threaded Redis shard.

---

### 8.2 eBPF and XDP Verifier
eBPF allows running safe C code directly at the NIC level (XDP) for instant filtering of malicious traffic.

**Load and verification pipeline:**
C code → Clang/LLVM → eBPF bytecode → Verifier (bounds, loops, stack checks) → JIT compilation → Kernel execution (XDP).

**Verifier criteria:**
*   No infinite loops.
*   Stack size strictly up to 512 bytes.
*   Pointer validity (packet bounds check against data_end).
*   Instruction count limit (up to 1M).

*   **Concrete eSPX example (XDP IP blacklist filter):**
    We need to drop packets from botnet networks on the blacklist before they enter the Linux network stack and wake our goroutines.
    *   *What NOT to do (Verifier will reject the code):*
        ```c
        // VERIFIER REJECTS: No packet bounds check!
        // Kernel does not know whether the incoming packet contains an IP header of the required size.
        struct iphdr *ip = (struct iphdr *)(data + sizeof(struct ethhdr));
        if (ip->saddr == blacklist_ip) {
            return XDP_DROP;
        }
        ```
    *   *How eSPX implements it (Pattern that passes verification):*
        ```c
        void *data = (void *)(long)ctx->data;
        void *data_end = (void *)(long)ctx->data_end;
        
        // Explicit Ethernet frame bounds check
        struct ethhdr *eth = data;
        if ((void *)(eth + 1) > data_end) {
            return XDP_PASS; // Packet too small; pass to standard stack
        }
        
        // Explicit IP header bounds check
        struct iphdr *ip = (void *)(eth + 1);
        if ((void *)(ip + 1) > data_end) {
            return XDP_PASS; 
        }
        
        // Safe read and lookup in eBPF map (blacklist)
        __u32 src_ip = ip->saddr;
        __u32 *blocked = bpf_map_lookup_elem(&blacklist_map, &src_ip);
        if (blocked) {
            return XDP_DROP; // Instant drop by the NIC!
        }
        
        return XDP_PASS;
        ```

---

## 9. Mechanics Quick Reference (Guide)

### io_uring
*   **Mechanics:** Shared ring buffers (Submission/Completion Queue) in memory shared between User Space and Kernel Space.
*   **Effect in eSPX:** `cmd/processor` writes a write descriptor into the SQ; the Linux kernel reads it asynchronously without a single interrupt syscall. Disk I/O performance increases 40% compared to classic `epoll` + `write`.

### OS Page Cache & mmap
*   **Mechanics:** Map a disk file directly into process virtual memory.
*   **Effect in eSPX:** `pkg/broker` writes click logs directly into a byte slice in RAM. On process crash via `SIGKILL`, the Linux kernel guarantees data durability because it physically resides in the OS kernel Page Cache.

### OS Thread & LockOSThread
*   **Mechanics:** Hard-bind a goroutine to OS thread `M` followed by CPU affinity.
*   **Effect in eSPX:** Prevents EEVDF scheduler thread migration between CPU cores. Tracker worker L1/L2 caches always stay warm with campaign data.

### Sharded atomics
*   **Mechanics:** Array of structs padded with filler bytes to 64 bytes (processor cache line size).
*   **Effect in eSPX:** Eliminates False Sharing and Cache-line Bouncing when incrementing click statistics counters in parallel on different CPU cores.

### Waterline batching
*   **Mechanics:** Accumulate small events in a memory buffer until capacity limit (for example, 10,000 rows) or timeout (50 ms).
*   **Effect in eSPX:** Grouping data minimizes disk write syscalls and network transactions with ClickHouse.

### TCP Multiplexing (epoll)
*   **Mechanics:** The `epoll_wait` syscall reports only active sockets ready for read/write.
*   **Effect in eSPX:** The `gnet` network library handles 100,000 Keep-Alive connections with a fixed worker pool (one per CPU core) without context-switch overhead.

### ClickHouse Batching
*   **Mechanics:** Write logs in large batches of 10,000 to 50,000 rows at once.
*   **Effect in eSPX:** Prevents on-disk table fragmentation and the `Too many parts in all data parts` error that arises from frequent row-by-row inserts.

### XDP MTU (1500B)
*   **Mechanics:** Limit maximum packet size to standard Ethernet frame size.
*   **Effect in eSPX:** At MTU 1500, packets from the Edge proxy guaranteedly fit in one contiguous linear NIC memory buffer. This lets the eBPF/XDP verifier quickly prove safe packet pointer traversal, reducing CPU overhead on network frame parsing.

---

## 10. Cold-Path Durability and Write Concurrency (Concept)

This section captures **reusable design principles** for processor cold-path work: stream consumers, database writers, mmap WAL, and failure containment. Apply them when adding new sinks (brokers, regional replicas, settlement batches) or tuning production throughput. Complements hot-path rules in `.cursorrules` and style rules in `GUIDE_STYLE_CODE.md`.

**Related:** `docs/MILESTONE.md` §1.1b–1.7, `docs/REMEDIATION.md` §5–7, `docs/DEVELOPMENT.md` (env vars).

### 10.1 Durability and acknowledgment order

1. **Ack after durable write.** Redis `XAck` runs only after `EventStore.StoreBatch` returns success (Postgres commit, ClickHouse `Send()`, or CH mmap spool `fsync`). Never ack on channel enqueue or optimistic success.
2. **PEL is the recovery queue.** Transient store failures must retain messages in the Pending Entries List. DLQ is for non-retriable poison pills after bounded retries — not for connection refused, pool closed, or partition.

*Effect in eSPX:* `StreamConsumer.flushBatch` blocks `XAck` until the store confirms; `isRetriableStoreError` prevents `recoverPending` from routing partition outages to DLQ (`write_path_db_fail_pre_ack` chaos proof).

### 10.2 Concurrency and backpressure

3. **Explicit write budgets.** `pgxpool.MaxConns` / `CH_MAX_CONNS` cap TCP connections but not goroutine fan-out. Named gates (`ProcessorPgGate`, `ProcessorChGate`) sized from `.env` (`PROCESSOR_*_GATE_SLOTS`, `0` = pool minus reserve) bound **logical** concurrent writers.
4. **Stream-level backpressure.** When the store circuit breaker is open, pause `XREADGROUP` (`pauseStreamReads`). Pressure belongs in the Redis stream (bounded lag), not in unbounded in-process batches.
5. **Separate PG and CH gates.** Postgres and ClickHouse have different failure modes, pool sizes, and outage strategies (spool vs PEL). Never share one semaphore across both.

*Effect in eSPX:* 24 parallel `StoreBatch` workers cap at gate capacity (chaos `processor_pg_gate_overflow`); CH outage sets `ad_processor_stream_backpressure_active=1` while spool absorbs writes.

### 10.3 Resources and configuration

6. **Lazy FD discipline.** Rotating mmap WAL (`events.wal` + sealed `events.wal.NNNN`) keeps one active FD; sealed segments map on `Scan` and unmap after replay.
7. **Env defaults preserve behavior.** New tunables use `0` or documented defaults equal to pre-change production values so compose upgrades do not silently change throughput.
8. **Document backlog separately.** Planned but unimplemented items (installer wizard, management pool gate) live in `docs/REMEDIATION.md` §7 with explicit DoD — not mixed into "done" milestone checklists.

*Effect in eSPX:* long CH outage rotates segments with `open_fds=1` (chaos `ch_spool_rotation`); `CH_SPOOL_SEGMENT_MB` / `CH_SPOOL_MAX_SEGMENTS` tune retention without code changes.

### 10.4 Verification

9. **Chaos over mocks for write path.** Durability, gates, and partition tests use testcontainers (real Postgres, Redis, ClickHouse) and emit `chaos_proof fault=...` for CI grep. Unit mocks remain for hot-path allocation gates only.
10. **Prove the invariant, not only happy path.** Each failure scenario in `REMEDIATION.md` §7.1 maps to a chaos proof that asserts PEL retention, backpressure, segment fault, or gate saturation — not merely "no panic".

*Effect in eSPX:* `internal/ingestion/write_path_chaos_integration_test.go` covers gate overflow, spool rotation, max segments, and PG stop without DLQ — reusable pattern for future broker or multi-region writers.
