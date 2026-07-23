# eSPX — Architectural Expansion: Microservices, Systems, and Mathematical Models

This document defines architectural extensions for the eSPX platform. It establishes low-level execution invariants for high-throughput ingestion (Hot Path), detailed functional specifications for operational microservices (Cold Path), speculative execution models, mathematical formulations, administrative capabilities, and end-to-end client-to-disk lifecycle specifications.

---

## 1. Workload Classification & Engineering Invariants

In accordance with `GUIDE_IDEAS_MICROSERVICES.md`, `GUIDE_STYLE_CODE.md`, `GO.md`, and `CONCEPTS.md`, all system components adhere to two execution models:

### 1.1 Hot-Path Workload Invariants

1. **Zero-Allocation Memory Model**: The request processing path MUST NOT trigger heap allocations (`0 B/op`, `0 allocs/op`). All data structures are allocated at startup or recycled via pooled buffers.
2. **CPU Cache Line Locality**: Data structures use Struct-of-Arrays (SoA) layouts or flat contiguous slices aligned to 64-byte cache line boundaries. Adjacent atomic counters use 56-byte padding (`_ [56]byte`) to prevent CPU cache line false sharing across core caches.
3. **Compiler Bounds Check Elimination (BCE)**: Loops iterating over slices MUST include explicit length check hints prior to iteration to allow the Go compiler to eliminate bounds checks.
4. **Zero-Copy Byte Conversions**: Strings and byte slices are converted using `unsafe.String` and `unsafe.Slice` to avoid memory copying.
5. **No Defers or Closures in Loops**: The `defer` keyword, closures, and anonymous functions are strictly prohibited in hot loops.
6. **Thread Pinning & Lock-Free Concurrency**: Critical worker threads are pinned to OS threads via `runtime.LockOSThread()` to optimize NUMA node access. Synchronization relies on lock-free atomics and MPSC ring buffers.

### 1.2 Cold-Path Workload Invariants

1. **Idiomatic Go**: Uses standard Go stdlib patterns, explicit error returns (`if err != nil`), and standard control flows for maintainability.
2. **Database Connection Pools**: Interacts with PostgreSQL using `pgxpool` with explicit acquire timeouts and statement caching.
3. **Transactional Outbox & Idempotency**: All state mutations write through `outbox_events` within a single PostgreSQL transaction using `SELECT FOR UPDATE SKIP LOCKED`.
4. **Governed ClickHouse Queries**: All analytical queries are wrapped with `chquery` enforcing strict execution settings (`max_memory_usage`, `max_execution_time`, `readonly=1`).

---

## 2. Hot-Path In-Process Modules & Micro-Engine Specifications

### 2.1 Real-Time Bot Cloaking & Lander Protection (`internal/ingestion/cloaker`)

#### 2.1.1 Overview & Architecture
Filters invalid traffic (search engine crawlers, ad network review bots, scrapers) during HTTP ingestion. Implemented as a modular package inside `cmd/tracker` (Matrix Score: 6/18) to eliminate IPC and network RTT overhead.

#### 2.1.2 Functional Capabilities
- **Tri-Layer Signature Matching**: Evaluates traffic across three independent detection layers in sequence:
  1. *IPv4 CIDR Ranges*: Matches client IP against blacklisted data-center subnets (AWS, GCP, Azure, DigitalOcean, known bot ranges).
  2. *User-Agent Signatures*: Substring search over known search engine crawlers (Googlebot, Bingbot, Yandex, FacebookExternalHit) and headless browser User-Agents.
  3. *TLS JA3/JA4 Fingerprints*: Exact matching of SSL/TLS client hello fingerprints to identify non-browser HTTP clients.
- **Dynamic Rule Hot-Reloading**: Subscribes to Redis Pub/Sub channels (`cloaker:rules:update`). Updates memory pointers via lock-free `atomic.Value` swaps without restarting the tracker binary.
- **Routing & Action Engine**:
  - *White-Page Routing*: Redirects matched bot traffic to a safe, compliant offer page (White Page) or returns HTTP 200 with generic HTML.
  - *Money-Page Routing*: Directs verified legitimate human traffic to the high-converting offer page (Money Page).
  - *Alternative Actions*: Configurable per campaign: Silent HTTP 204 No Content, HTTP 403 Forbidden, or customizable HTTP 302 Location redirect.

#### 2.1.3 Operator Control Interface
- **Admin API Endpoints**: `GET /api/v1/cloaker/rules` (query active rules), `POST /api/v1/cloaker/rules` (add IP/UA/JA3 patterns), `GET /api/v1/cloaker/stats` (real-time match counters by rule ID).
- **Campaign Level Overrides**: Media buyers can enable/disable cloaking per campaign or toggle specific filter layers (e.g. strict JA3 check vs. IP-only check).

#### 2.1.4 Codebase & Optimization Guidelines
- **Memory Layout**: Stores IPv4 CIDRs as flat uint32 range pairs (`IP`, `Mask`) in a single contiguous slice. User-Agent and JA3 fingerprint signatures are stored as contiguous byte arrays.
- **BCE Enforcement**: Executes a length check prior to loop entry, ensuring the compiler omits array bounds checking.
- **Escape Analysis**: String views and slice pointers do not escape to the heap (`-gcflags="-m"`). Inlining is enforced on matching helper functions.

#### 2.1.5 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Network Stack & Epoll**: Client initiates TCP connection. Kernel network stack completes handshake (`SO_RCVBUF`). `gnet` event loop receives epoll edge-triggered notification (`epoll_wait`) and reads bytes into a pre-allocated ring buffer.
2. **Kernel-to-Userspace Parsing**: Zero-copy HTTP parsing extracts client IP, User-Agent, and JA3 TLS fingerprint.
3. **Memory & L1/L2 Cache Evaluation**: The cloaking engine performs an $O(N)$ contiguous memory scan over the flat `IPRange` slice. Memory is 64-byte cache-line aligned; CPU hardware prefetcher loads range blocks directly into L1/L2 cache, executing checks under 15 ns.
4. **Filtering Action**:
   - Bot Identified: Returns configured fallback response (HTTP 204/403/302). Zero disk I/O, zero database queries.
   - Legitimate Traffic: Proceeds to campaign filtering and Redis Lua budget debit.
5. **Disk Persistence**: Reject metrics increment on lock-free atomic counters. Cold-path metrics collectors periodically scrape counters to store rollups in ClickHouse (`ReplacingMergeTree`).

#### 2.1.6 Chaos Testing & Bottleneck Analysis
- **Steady-State Invariants**: Hot path throughput remains unchanged during active filter rejections; zero heap allocation spikes.
- **Fault Injection**: Injected corrupt User-Agent strings, out-of-bounds IP inputs, and concurrent dictionary updates via lock-free atomic pointer swaps.
- **Bottleneck**: CPU cache misses if the blacklist exceeds 100,000 entries. Mitigated by partitioning ranges into an $O(1)$ radix tree or bitmask array if size exceeds L3 cache capacity.

---

### 2.2 Dynamic Path Obfuscator & Domain Rotator (`internal/ingestion/obfuscator`)

#### 2.2.1 Overview & Architecture
Decodes ephemeral tracking path tokens to protect domain reputation from automatic crawler blocklists. Implemented as a modular package inside `cmd/tracker` (Matrix Score: 7/18).

#### 2.2.2 Functional Capabilities
- **Ephemeral Path Token Parsing**: Parses encrypted or obfuscated URL tokens (e.g., `/t/e9a8f2c1b0...`) containing campaign ID, creative ID, and timestamp.
- **AES-NI In-Memory Decryption**: Decodes path tokens using CPU AES-NI instructions or fast XOR key tables without allocating memory buffers.
- **Domain Health Probing & Rotation**: Monitors domain reputation across external ad network blocklists. When a domain is flagged, the domain rotator updates routing snapshots in Redis, directing new traffic to healthy fallback domains.
- **Token Replay & Expiration Protection**: Validates token timestamp delta (e.g. max age 300 seconds) to reject replayed tracking links or scrapers reusing expired paths.

#### 2.2.3 Operator Control Interface
- **Domain Pool Manager**: Admin API endpoints to register tracking domains, view SSL certificate status, and toggle automated domain rotation policies.
- **Obfuscation Key Rotation**: Periodic background rotation of symmetric token encryption keys.

#### 2.2.4 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Client Request**: Client sends `GET /t/e9a8f2c1b0...` over TLS.
2. **Parsing & Decoding**: gnet reads the URI path. `ZeroAllocExtractToken` creates a non-allocating string view. The token is decoded via an in-memory XOR/AES-NI key table stored in CPU cache.
3. **Registry Match**: Decoded parameters (`campaign_id`, `creative_id`) are verified against the in-memory tracker registry (`atomic.Value` snapshot).
4. **Execution & Storage**: Proceeding traffic triggers a single Redis Lua script debit. Log streams append to the processor's memory-mapped WAL file before ClickHouse ingestion.

#### 2.2.5 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: Malformed path tokens, invalid base64 payloads, and truncated paths.
- **Verification**: Evaluated via `chaos_proof fault=obfuscator_malformed_token_rejected`. Confirms system drops invalid tokens without heap allocations or panics.

---

### 2.3 Kernel-Space Edge Filter via eBPF/XDP (`cmd/edge-bpf-sync`)

#### 2.3.1 Overview & Architecture
Offloads malicious IP drops, volumetric DDoS defense, and protocol fingerprinting to the kernel network driver layer (`XDP_DROP`) or SmartNIC hardware offload engine (`XDP_FLAGS_HW_MODE`) before user-space socket buffer allocation. Matrix Score: 5/18 (Node Utility).

#### 2.3.2 Functional Capabilities
- **SmartNIC Hardware Offloading (`XDP_FLAGS_HW_MODE`)**: Loads compiled eBPF bytecode directly into the NPU/ASIC memory of hardware-offloaded SmartNICs (ConnectX-6/7, Netronome Agilio). Matched packets drop in hardware before PCIe DMA transfers to host RAM, protecting host CPU and host memory bandwidth during 100+ Gbps attacks. Automatically falls back to Native Driver XDP (`XDP_FLAGS_DRV_MODE`) if hardware offload is unsupported.
- **Stateless Line-Rate SYN Flood Protection (`XDP_TX`)**: Generates $O(1)$ SipHash cryptographic TCP SYN cookies inside the XDP hook. Transmits `SYN-ACK` responses directly back out the interface (`XDP_TX`) with reversed MAC/IP/TCP headers, bypassing Linux kernel TCP socket (`struct sock`) and `conntrack` memory allocation.
- **Layer 7 Protocol & TLS Fingerprinting**: Verifies TCP payload offset (`data + tcph_bytes`) and unrolls byte comparisons for HTTP request lines (`GET /`, `POST /`) and TLS Client Hello records (`ContentType == 22`, `HandshakeType == 1`). Drops unwanted scrapers or mismatched TLS SNI hostnames before user-space socket read.
- **Per-Subnet Token Bucket Rate Limiting (`BPF_MAP_TYPE_LRU_HASH`)**: Tracks packet rates per `/24` IPv4 or `/48` IPv6 subnet using LRU maps. Atomic token counters drop excess traffic exceeding packets-per-second thresholds (`XDP_DROP`).
- **Direct Zero-Copy Bypass via AF_XDP (`XDP_REDIRECT`)**: Redirects clean traffic directly into user-space memory-mapped ring buffers (`UMEM`) owned by `gnet` worker threads, bypassing the Linux TCP/IP network stack for microsecond ingestion.
- **Outbox Synchronization**: Consumes `BLOCK_IP` events from PostgreSQL via outbox relay workers and updates pinned BPF maps (`/sys/fs/bpf/espx_blocklist`) using kernel syscalls (`bpf_map_update_elem`).

#### 2.3.3 Verifier & Memory Safety Invariants
- **Explicit Pointer Bounds Checking**: Every protocol header calculation includes explicit boundary verification (`data + sizeof(eth) + sizeof(ip) + sizeof(tcp) <= data_end`), mathematically proving memory safety before register dereferences to pass kernel verification.
- **Per-CPU Scratchpad Maps**: Uses `BPF_MAP_TYPE_PERCPU_ARRAY` maps for scratchpad buffers, ensuring stack frame utilization stays strictly under the 512-byte eBPF stack limit.
- **Unrolled Loop Bounding**: Loops use `#pragma unroll` or strict loop bounds to guarantee instruction counts stay below 1,000,000 instructions.

#### 2.3.4 Operator Control Interface
- **CLI Diagnostic Tool (`espx-bpf-status`)**: Inspects active eBPF map entries, SmartNIC hardware offload status (`HW_MODE` vs `DRV_MODE`), dropped packet counters per interface, and map memory utilization.
- **Prometheus Metrics**: Exposes `ad_ebpf_dropped_packets_total`, `ad_ebpf_syn_cookies_issued_total`, and `ad_ebpf_map_entries` for real-time telemetry.

#### 2.3.5 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Frame Arrival**: Network interface card (NIC) receives an Ethernet frame into its RX ring buffer.
2. **Hardware/Driver XDP Hook**: The eBPF program executes in SmartNIC ASIC or driver RX ring queue.
3. **Map Verification & Payload Check**: Performs $O(1)$ lookup in `BPF_MAP_TYPE_LPM_TRIE` and inspects L7 payload headers.
4. **Packet Drop or Redirect**:
   - Blacklisted or Malicious: Returns `XDP_DROP` (discarded instantly without kernel RAM allocation or context switches).
   - SYN Flood: Returns `XDP_TX` with calculated SipHash SYN cookie.
   - High-Priority Ingest: Executes `XDP_REDIRECT` to AF_XDP socket (zero-copy UMEM transfer).
   - Standard Clean Traffic: Returns `XDP_PASS` to Linux TCP/IP stack for `gnet` epoll ingestion.
5. **Sync Path**: Cold-path `management` writes block events to PostgreSQL outbox. Outbox worker emits `BLOCK_IP` events, updating pinned BPF maps via syscalls (`bpf_map_update_elem`).

#### 2.3.6 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: BPF map capacity exhaustion, out-of-order map updates, NIC driver restart, hardware offload driver failure.
- **Verification**: `chaos_proof fault=ebpf_map_exhaustion_fallback_to_userspace`. Verified to sustain 1,000,000 packets/sec attack bursts at line rate.

---

### 2.4 Dynamic Creative & Content Personalization Engine (`internal/ingestion/dco`)

#### 2.4.1 Overview & Architecture
Assembles dynamic creative elements (headlines, media URLs, CTA buttons) on the hot path in real time based on user context. Implemented as an in-process package inside `cmd/tracker` (Matrix Score: 6/18).

#### 2.4.2 Functional Capabilities
- **Zero-Allocation Template Assembly**: Assembles HTML/JSON payloads from pre-compiled byte slices stored in `sync.Pool` without string concatenation or reflection (`0 B/op`).
- **Contextual Personalization**: Dynamically injects client Geo records (MaxMind MMDB), device models (User-Agent parser), ISP operator names, and local day/time context into creative templates.
- **Asset Fallback Protection**: Automatically substitutes secondary creative variants if primary assets are flagged or unavailable.

#### 2.4.3 Operator Control Interface
- **DCO Template Builder**: Admin API endpoints (`POST /api/v1/dco/templates`) to register modular creative components.
- **Performance Telemetry**: Monitors conversion rates (CR) per dynamic element combination.

---

## 3. Cold-Path Microservices & Functional Specifications

### 3.1 Server-to-Server (S2S) Postback Dispatcher (`cmd/postback-sender`)

#### 3.1.1 Overview & Architecture
Forwards conversion events to external ad platforms (Facebook Conversions API, Google Ads, TikTok API, custom affiliate networks). Matrix Score: 13/18 (Standalone Service due to external network calls, secret storage, and retry isolation).

#### 3.1.2 Functional Capabilities
- **Multi-Platform Conversion Dispatch**: Supports specialized API integration adapters:
  - *Facebook Conversions API (CAPI)*: Transmits hashed user data (SHA-256 email/phone), event name (`Purchase`, `Lead`), event time, and `fbclid`.
  - *Google Ads Offline Conversion Imports*: Dispatches conversion value, currency, and `gclid` via Google Ads API.
  - *TikTok Events API*: Sends event details with `ttclid` and user context.
  - *Custom S2S Webhooks*: Renders custom HTTP GET/POST postback URLs with macro substitution (`{click_id}`, `{payout}`, `{tx_id}`, `{subid1}`).
- **Macro Substitution Engine**: Replaces dynamic tokens in outbound URLs from conversion payload context.
- **Token Bucket Rate Limiting**: Enforces rate limits per target network domain to avoid triggering third-party API 429 rate limit responses.
- **Exponential Backoff & DLQ**: Retries transient failures (5xx, network timeouts) up to 5 attempts with exponential backoff and jitter. Moves unrecoverable failures to a Dead-Letter Queue (DLQ) for manual inspection.
- **Idempotency & Deduplication**: Generates deterministic event hashes (`customer_id + click_id + event_type`) to ensure external APIs never receive duplicate conversion postbacks.

#### 3.1.3 Operator Control Interface
- **Postback Configuration Dashboard**: Interface to set up global or campaign-level postback URLs, custom headers, secret API keys, and event mappings.
- **DLQ Management Console**: Displays failed postbacks with failure reasons; allows one-click bulk retry or deletion.
- **Dispatch Telemetry**: Real-time graphs for dispatch latency, success rates (2xx vs 4xx/5xx), and queue depth per downstream platform.

#### 3.1.4 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Conversion Event**: Conversion request reaches `/postback`. `cmd/processor` writes event to `postbacks` in PostgreSQL within a single transaction.
2. **Outbox Event**: An `outbox_events` record (`type=SEND_POSTBACK`) is enqueued.
3. **Outbox Polling**: `cmd/postback-sender` polls PostgreSQL using `SELECT FOR UPDATE SKIP LOCKED`.
4. **HTTP Dispatch**: Worker fetches customer OAuth credentials from PostgreSQL, constructs JSON payload, and sends HTTP POST to third-party API over TLS.
5. **OS Page Cache & Persistence**: PostgreSQL appends transaction records to Write-Ahead Log (WAL), flushing to disk using 8 KiB OS page writes. Successful dispatches mark outbox event status as `DELIVERED`.

#### 3.1.5 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: External API timeout (30-second hang), DNS resolution failure, HTTP 500 errors, rate-limit 429 responses.
- **Invariants**: Conversion events are never lost; retry loop adheres to exponential backoff; core `/track` ingestion experiences zero latency degradation.

---

### 3.2 Automated Cost API Synchronization Engine (`cmd/cost-sync`)

#### 3.2.1 Overview & Architecture
Imports campaign spend from advertising platforms periodically and updates financial ledgers. Matrix Score: 11/18 (Standalone Cron Worker).

#### 3.2.2 Functional Capabilities
- **Multi-Network API Connectors**: Regularly connects to major ad networks (Facebook Ads, Google Ads, TikTok, Exoclick, Outbrain, Taboola) to fetch actual cost data.
- **OAuth2 Token Lifecycle Management**: Automatically refreshes access tokens using stored refresh tokens stored securely in PostgreSQL.
- **Placement-Level Granularity**: Downloads spend data broken down by date, campaign ID, ad set ID, creative ID, and placement (SubID).
- **Currency Conversion Engine**: Downloads daily exchange rates (ECB/Federal Reserve APIs) and converts foreign spend (EUR, GBP, JPY) to base system currency micro-units (`BIGINT`).
- **Spend Reconciliation & Ledger Sync**: Calculates discrepancy between tracker-estimated spend and actual network API spend, creating balancing entries in `balance_ledger`.

#### 3.2.3 Operator Control Interface
- **Network API Credentials Manager**: Interface to authorize ad networks via OAuth2 or API key input.
- **Sync History Log**: Displays fetch status per network, imported spend totals, currency conversion rates applied, and error logs.
- **Manual Trigger API**: Endpoint `POST /api/v1/cost-sync/run` to trigger immediate manual spend synchronization for specific campaigns or date ranges.

#### 3.2.4 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Cron Trigger**: Internal scheduler triggers hourly fetch jobs.
2. **API Ingestion**: Connects to network APIs, downloads daily spend reports, and parses JSON/CSV data in memory.
3. **Database Transaction**: Opens PostgreSQL transaction (`pgxpool`), inserts cost line items into `campaign_costs`, and updates `balance_ledger`.
4. **Storage Mechanics**: Updates B+-tree indexes in PostgreSQL. Balance changes propagate to Redis budget keys asynchronously via `SyncWorker`.

#### 3.2.5 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: Database connection pool exhaustion, duplicate cost report ingestion, network disconnect mid-transaction.
- **Invariants**: Ledger idempotency verified via `(customer_id, campaign_id, date, network)` unique key constraint; rollbacks leave zero orphan ledger rows.

---

### 3.3 Placement Margin Guard & Offer Auto-Pauser (`cmd/margin-guard`)

#### 3.3.1 Overview & Architecture
Monitors real-time ROI per traffic placement (SubID) in ClickHouse and automatically pauses unprofitable placements. Matrix Score: 12/18 (Standalone Worker).

#### 3.3.2 Functional Capabilities
- **Real-Time Margin Evaluation**: Continuously analyzes ClickHouse materialized views (`mv_placement_stats_hourly`) to calculate placement performance:
  $$\text{profit} = \text{revenue} - \text{spend}$$
  $$\text{ROI} = \left(\frac{\text{profit}}{\text{spend}}\right) \times 100$$
- **Rule Evaluation Engine**: Evaluates user-defined rules:
  - *Min Clicks Threshold*: Requires minimum sample size (e.g., $\ge 50$ clicks) before evaluation to prevent premature pausing.
  - *ROI Floor*: Triggers pause if ROI falls below threshold (e.g. $< -30\%$).
  - *Consecutive Zero-Conversion Clicks*: Pauses placement if it reaches $N$ clicks (e.g. 100) with 0 conversions.
- **Automated Blacklist Propagation**: Writes `PAUSE_PLACEMENT` outbox events, emitting Redis commands (`HSET blacklist:placement:{id}`) that hot-path trackers absorb within 5 seconds.
- **Alert Dispatch**: Sends real-time notifications to Telegram/Slack channels with performance metrics when a placement is auto-paused.

#### 3.3.3 Operator Control Interface
- **Policy Builder Interface**: Allows operators to set global or campaign-specific margin rules (min clicks, target ROI, max loss threshold).
- **Auto-Pause Activity Log**: Displays historical pause events, showing exact placement IDs, spend, revenue, ROI at trigger time, and override buttons.
- **Placement Allowlist**: Excludes VIP or strategic traffic sources from auto-pausing.

#### 3.3.4 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **ClickHouse Analytical Query**: Every 60 seconds, `cmd/margin-guard` executes a governed query over ClickHouse `mv_placement_stats_hourly` (`SETTINGS max_execution_time=5`).
2. **Margin Evaluation**: Computes `profit = revenue - spend` and `roi = (profit / spend) * 100`. If placement ROI falls below `-30%` over a 50-click sample, an auto-pause action triggers.
3. **Mutation Execution**: Writes a `PAUSE_PLACEMENT` outbox event to PostgreSQL.
4. **Hot-Path Synchronization**: `OutboxWorker` reads the event and issues a `HSET` to Redis global keys (`blacklist:placement:{id}`). All tracker nodes absorb the updated blacklist within 5 seconds.

#### 3.3.5 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: ClickHouse node unavailability, high ClickHouse query lag (>5 min), Redis pub/sub drop.
- **Invariants**: Auto-pauser degrades gracefully when ClickHouse is delayed (`stale=true`); never issues false-positive pauses due to partial data.

---

### 3.4 CPA Affiliate Lead Status Sync Engine (`cmd/lead-status-sync`)

#### 3.4.1 Overview & Architecture
Periodically synchronizes conversion lifecycle updates (Lead $\rightarrow$ Approved / Rejected / Chargeback) from affiliate network APIs back into eSPX ledgers. Matrix Score: 11/18 (Standalone Cron Worker).

#### 3.4.2 Functional Capabilities
- **Affiliate Network Polling**: Connects to affiliate network APIs (HasOffers, Cake, Everflow, Tune, Voluum) using API keys.
- **Status Lifecycle Reconciliation**: Updates conversion status in PostgreSQL `postbacks` and `balance_ledger`:
  - *Approved Lead*: Releases held payout balance into active customer balance.
  - *Rejected Lead*: Cancels pending payout, preventing media buyer payout.
  - *Chargeback*: Records negative revenue adjustment in `balance_ledger`.
- **Downstream Postback Triggering**: Emits new outbox events when a lead status changes, instructing `cmd/postback-sender` to notify downstream traffic sources.

#### 3.4.3 Operator Control Interface
- **Affiliate Network Integration Hub**: Manage API keys and status mappings per affiliate network.
- **Reconciliation Report**: Summary of approved vs rejected leads, payout adjustments, and network latency.

---

### 3.5 Automated A/B Landing Page & Route Optimizer (`cmd/lander-optimizer`)

#### 3.5.1 Overview & Architecture
Dynamically adjusts traffic weights across landing pages and offers using reinforcement learning algorithms to maximize Earnings Per Click (EPC). Matrix Score: 10/18 (Standalone Worker).

#### 3.5.2 Functional Capabilities
- **Multi-Armed Bandit Algorithms**: Implements Thompson Sampling and Epsilon-Greedy algorithms to optimize traffic allocation.
- **Real-Time EPC/CR Tracking**: Reads real-time click and conversion rollups from ClickHouse per lander/offer variation.
- **Dynamic Weight Updates**: Periodically re-calculates optimal traffic distribution percentages and updates Redis campaign routing tables (`HSET campaign:{id}:routing_weights`).
- **Low-Performer Auto-Pruning**: Automatically pauses lander variants whose probability of being optimal falls below 5%.

#### 3.5.3 Operator Control Interface
- **A/B Experiment Dashboard**: Real-time visualization of conversion rate (CR), EPC, assigned traffic percentage, and statistical confidence intervals per variant.
- **Manual Weight Lock**: Ability to lock specific traffic splits (e.g. force 50/50 split) for manual testing.

---

### 3.6 Traffic Rejection & Botnet Analyzer (`cmd/botnet-analyzer`)

#### 3.6.1 Overview & Architecture
Analyzes click streams for time-series anomalies and IP concentration patterns to discover emerging botnets. Matrix Score: 12/18 (Standalone Worker).

#### 3.6.2 Functional Capabilities
- **Inter-Click Interval Variance Analysis**: Evaluates time differences ($\Delta t$) between consecutive clicks from identical IP subnets. Low variance ($\sigma \rightarrow 0$) indicates automated timer-driven bot scripts.
- **IP Subnet Concentration Scoring**: Flags /24 subnets that generate abnormally high click-to-impression ratios without conversion activity.
- **Automated Blacklist Generation**: Emits `BLOCK_IP` events to `cmd/management`, which updates eBPF kernel maps via `cmd/edge-bpf-sync`.

#### 3.6.3 Operator Control Interface
- **Threat Intelligence Dashboard**: Displays detected botnet clusters, IP subnet attack heatmaps, and confidence scores.
- **False-Positive Review Queue**: Allows operators to unblock subnets and adjust algorithm sensitivity thresholds.

---

### 3.7 Retargeting Audience & Pixel Sync Exporter (`cmd/audience-exporter`)

#### 3.7.1 Overview & Architecture
Streams high-intent users reaching specific funnel stages to external Data Management Platforms (DMPs) and ad network custom audience APIs. Matrix Score: 11/18 (Standalone Worker).

#### 3.7.2 Functional Capabilities
- **Real-Time Segment Builder**: Groups users into target audience lists based on actions (e.g. visited lander but did not convert, spent $>60$ seconds on offer page).
- **PII Hashing & GDPR Protection**: Hashes user identifiers (email, phone, device ID) with SHA-256 before egress transmission.
- **External Audience API Dispatch**: Syncs custom audience lists to Facebook Custom Audiences, Google Customer Match, and TikTok Audience API.

#### 3.7.3 Operator Control Interface
- **Audience Segment Manager**: Rule builder to define inclusion/exclusion criteria for audience lists.
- **Egress Sync Monitor**: Displays total synced identifiers per platform, match rates, and export logs.

---

### 3.8 AI/ML Smart Pacing & Conversion Rate Prediction Engine (`cmd/smart-pacer`)

#### 3.8.1 Overview & Architecture
Prevents early budget exhaustion ("front-loading spend") by predicting hourly traffic conversion rates ($pCR$) and dynamically calculating pacing multipliers ($K_{\text{bid}}$). Matrix Score: 11/18 (Standalone Cron Worker).

#### 3.8.2 Functional Capabilities
- **Probabilistic Token Bucket Pacing**: Computes hourly budget distribution slots over 24-hour windows based on historical 30-day traffic velocity.
- **Dynamic Bid Multipliers**: Calculates slot multipliers $K_{\text{bid}} \in [0.1, 2.0]$ per Geo, Device, and Hour.
- **Smooth Pacing Enforcement**: Slowly scales down multiplier keys in Redis when traffic velocity exceeds target spend rates without pausing campaigns completely.

#### 3.8.3 Operator Control Interface
- **Pacing Curve Visualizer**: Graphical interface displaying target vs. actual hourly spend curves.
- **Pacing Strategy Selector**: Configurable modes: Uniform Pacing, Conversion-Weighted Pacing, Aggressive Front-Loading.

---

### 3.9 Cookieless Identity Graph & Multi-Touch Attribution Engine (`cmd/identity-resolver`)

#### 3.9.1 Overview & Architecture
Constructs an anonymous First-Party Identity Graph to link user touchpoints across Privacy-First browsers (Apple ITP, Chrome Privacy Sandbox) without 3rd-party cookies. Matrix Score: 12/18 (Standalone Async Worker).

#### 3.9.2 Functional Capabilities
- **First-Party Identity Graph**: Links touchpoints using cryptographically salted vectors (First-Party Cookie Hash, IP /24 Subnet, Client Hints TLS, Screen Resolution Hash).
- **Multi-Touch Attribution Models**: Calculates attribution weighting across First-Touch, Last-Touch, Linear, Time-Decay, and Shapley Value models.
- **Cross-Device Household Bridging**: Groups mobile and desktop interactions originating from identical local networks into unified session graphs.

#### 3.9.3 Operator Control Interface
- **Attribution Model Comparison Tool**: Side-by-side ROI analysis across First-Touch, Last-Touch, and Shapley Value models.
- **Identity Graph Health Monitor**: Metrics on graph node resolution confidence and link expiration rates.

---

### 3.10 Fraud Ring & Collusion Graph Analyzer (`cmd/fraud-graph-analyzer`)

#### 3.10.1 Overview & Architecture
Identifies complex botnets and collusion rings by discovering hidden graph relationships across traffic streams. Matrix Score: 13/18 (Standalone Analytics Worker).

#### 3.10.2 Functional Capabilities
- **Connected Component Graph Mining**: Identifies collusion clusters sharing rare attribute combinations (payout wallet addresses, canvas fingerprint vectors, fixed click-to-lead latency deltas).
- **Graph Risk Scoring**: Computes a graph risk score $R_{\text{graph}} \in [0.0, 1.0]$ per publisher/SubID.
- **Automated Kernel Offloading**: Emits `BLOCK_SUBNET` outbox events when $R_{\text{graph}} > 0.9$, updating eBPF kernel maps via `cmd/edge-bpf-sync`.

#### 3.10.3 Operator Control Interface
- **Threat Graph Explorer**: Interactive node-graph visualization of detected botnet rings and shared attributes.
- **Manual Unblock Override**: Allows operators to whitelist false-positive clusters.

---

### 3.11 Automated Lander Mirroring & Sanitizer Engine (`cmd/lander-cloner`)

#### 3.11.1 Overview & Architecture
Clones, cleans, and hosts static landing page mirrors to protect against domain blocklists and host downtime. Matrix Score: 11/18 (Standalone Utility Service).

#### 3.11.2 Functional Capabilities
- **DOM Sanitizer Pipeline**: Parses landing page HTML, stripping competitor spy scripts, external tags, malware scripts, and unapproved redirects.
- **Media Optimization Pipeline**: Automatically converts heavy PNG/JPEG images into compressed WebP/AVIF assets (reducing page size by ~60%).
- **Anti-Spy Protection Injection**: Injects obfuscated JS guards to defeat automated spy tools (AdHeart, Anstrex).
- **Edge Deployment**: Pushes processed assets to local S3/MinIO storage for low-latency serving via Nginx `sendfile`.

#### 3.11.3 Operator Control Interface
- **Lander Mirror Console**: Interface to submit cloning URLs (`POST /api/v1/landers/clone`), view sanitization logs, and preview mirrored pages.

---

### 3.12 Multi-Currency Settlement & Payout Engine (`cmd/settlement-engine`)

#### 3.12.1 Overview & Architecture
Automates balance verification, hold period management, and multi-currency publisher payouts in crypto and fiat. Matrix Score: 14/14 (Standalone Financial Core).

#### 3.12.2 Functional Capabilities
- **Hold Period Governance**: Manages lead hold windows (e.g. 14-day hold) awaiting affiliate status confirmations (`cmd/lead-status-sync`).
- **Fraud Gate Check**: Verifies publisher risk scores with `cmd/botnet-analyzer` before releasing funds.
- **Multi-Provider Dispatches**: Dispatches crypto payouts (USDT TRC20/ERC20/TON via `cmd/crypto-gateway`) or fiat payments (SEPA/Wire/Stripe API).
- **Double-Entry Ledger Accounting**: Appends balanced debit/credit entries to `balance_ledger` in PostgreSQL, preventing financial drift.

#### 3.12.3 Operator Control Interface
- **Payout Approval Dashboard**: Interface for finance operators to audit pending payouts, hold balances, and transaction receipts.

---

### 3.13 Cold-Path Distributed Lock Manager (`cmd/dlm-orchestrator` / PostgreSQL & Redis DLM)

#### 3.13.1 Overview & Architecture
Provides cluster-wide mutual exclusion, single-leader election, and distributed coordination across replicated instances of cold-path background workers (`cmd/management`, `cmd/cost-sync`, `cmd/postback-sender`, `cmd/settlement-engine`, `cmd/margin-guard`). Matrix Score: 11/18 (Standalone Middleware Service).

#### 3.13.2 Functional Capabilities
- **Dual-Layer Lock Protocol**:
  1. *Transactional PostgreSQL Advisory Locks*: Executes `SELECT pg_try_advisory_xact_lock(key)` for database-bound transactional tasks (e.g. `cmd/settlement-engine`, `cmd/cost-sync`). Locks release automatically on transaction commit/rollback or TCP connection crash, eliminating orphan lock risks.
  2. *Redis Redlock with Monotonic Fencing Tokens*: Executes multi-master Redlock (`SET key uuid NX PX 30000` with Lua release scripts) for distributed cron jobs and cross-system tasks without open PostgreSQL transactions.
- **Monotonic Fencing Tokens**: Every acquired lock receives an auto-incrementing 64-bit fencing token (`lock_fence_token`). Downstream storage engines and API workers verify `write_token >= current_token`, rejecting writes from "zombie workers" stalled by long GC pauses or network partitions.
- **Single-Leader Election**: Elects a single active leader per task category (e.g., single leader executing hourly spend reconciliation or daily quota flushing), allowing passive standby replicas to maintain high availability without duplicate execution.
- **Lock Renewal & Heartbeat Goroutines**: Active lock holders execute background heartbeat goroutines (`RenewLock`) to extend lock TTLs while processing long-running jobs.

#### 3.13.3 Operator Control Interface
- **DLM Status Inspector**: Admin API endpoint (`GET /api/v1/dlm/locks`) displaying active locks, lock holders, fencing token values, and remaining TTLs.
- **Manual Emergency Lock Release**: Override API endpoint (`POST /api/v1/dlm/locks/release`) to force-release hung locks during infrastructure node failover drills.

#### 3.13.4 End-to-End Client-to-Disk Lifecycle & System Mechanics
1. **Lock Request**: Worker instance requests a named lock (e.g. `lock:cost_sync:hourly`).
2. **Dual-Layer Acquisition**:
   - DB Worker: Calls `pg_try_advisory_xact_lock(hash_key)` within its `pgx` transaction.
   - Non-DB Worker: Executes Redlock algorithm across Redis masters, receiving `lock_fence_token`.
3. **Execution & Fencing Check**: Worker executes task, attaching `lock_fence_token` to outbox events. Storage workers confirm token monotonicity before committing changes.
4. **Release**: Lock releases automatically on SQL transaction completion or via atomic Redis Lua script.

#### 3.13.5 Chaos Testing & Bottleneck Analysis
- **Fault Injection**: Worker process SIGKILL during active lock hold, network partition mid-execution, lock TTL expiration during long database reads.
- **Invariants Verified**: At most one worker instance executes a named task (`active_holders <= 1`); stale fencing tokens trigger write rejection (`chaos_proof fault=dlm_stale_fence_rejected`).

---

## 4. Cold-Path Analytics & Administrative Infrastructure

### 4.1 ClickHouse Materialized Views & Governance

#### 4.1.1 LSM-Tree Engine Storage Mechanics
- ClickHouse uses Log-Structured Merge-Tree (LSM-Tree) engines (`ReplacingMergeTree`, `SummingMergeTree`).
- High-frequency individual inserts cause **part fragmentation** and high disk I/O write amplification.
- `cmd/processor` buffers events in memory, writing micro-batches of 20,000 events or 5-second flushes.
- Background merge processes combine smaller data parts into larger compressed parts using ZSTD compression.

#### 4.1.2 Governed Query Engine (`chquery`)
Admin API reports use the `chquery` wrapper to enforce resource limits on analytical nodes:
- `max_memory_usage = 2GB`
- `max_execution_time = 10s`
- `readonly = 1`

Prevents out-of-memory (OOM) crashes on ClickHouse clusters during heavy operator dashboard queries.

---

## 5. Microservices Loose Coupling Strategies

### 5.1 Transactional Outbox Event Bus
All cross-service state propagation uses PostgreSQL as a reliable event broker:
- Services write state changes and `outbox_events` within the same database transaction.
- Background workers read events using `SELECT FOR UPDATE SKIP LOCKED` and dispatch them to Redis, gRPC, or external APIs.
- Guarantees **at-least-once delivery** without requiring external messaging infrastructure (e.g. Kafka/RabbitMQ).

### 5.2 Schema Boundaries & API Contracts
- Services communicate internally over gRPC using Protobuf contracts.
- External administrative clients interact via JSON REST endpoints (`internal/adminapi`).
- Database schemas are strictly partitioned by service ownership (`public`, `billing`, `licensing`). Direct cross-service database access is prohibited.

---

## 6. Legacy Tracker Problem Resolution Matrix

| Problem | Legacy Root Cause | eSPX Architectural Solution |
| :--- | :--- | :--- |
| **High p99 Latency under skew** | Single-threaded PHP/MySQL synchronous lock | gnet epoll event loop + client-sharded Redis Lua atomic debits |
| **Database Lock Contention** | Direct row updates per click | Async stream ingestion + 10s in-memory batch consolidation in processor |
| **Part Fragmentation (ClickHouse)** | Single-row HTTP inserts to CH | Memory-buffered batch writes (20k rows / 5s window) via `ClickHouseStore` |
| **Bot Traffic Server Overload** | User-space script evaluation | Line-rate eBPF/XDP filtering at kernel driver level (`XDP_DROP`) |
| **Domain Reputation Loss** | Static tracking links | Zero-allocation dynamic path obfuscator using `unsafe.String` |

---

## 7. Speculative Execution & Mathematical Models

### 7.1 Speculative Execution & Pre-Fetching Models

#### 7.1.1 Speculative Markov Chain Route Pre-Computation
To eliminate lookup latency on high-frequency campaign paths, tracker nodes use a first-order Markov Chain model to predict subsequent request transitions:

$$P(S_{t+1} = j \mid S_t = i) = \frac{N_{ij}}{\sum_k N_{ik}}$$

Where $S_t$ represents the current user funnel state (e.g. initial ad click $i$), and $S_{t+1}$ represents the target landing page or offer $j$. When transition probability $P(S_{t+1} \mid S_t) > 0.85$, the worker speculatively pre-warms campaign routing snapshots and Redis token keys in CPU L1/L2 cache lines before the client HTTP request completes.

#### 7.1.2 Speculative Zero-Copy Header Offset Parsing
Incoming TCP byte streams are speculatively parsed based on client network fingerprints:
- If the incoming connection originates from a known HTTP/1.1 Edge proxy, the parser speculatively projects header offsets at fixed byte positions (`data + 16`).
- Verified via a single 64-bit word equality comparison (`uint64`). On mismatch, the parser reverts to standard zero-copy DFA parsing without heap allocations.

---

### 7.2 Mathematical Formulations & Optimization Models

#### 7.2.1 Probabilistic Token Bucket Budget Pacing Model
The dynamic bid pacing multiplier $K_{\text{bid}}(t)$ in `cmd/smart-pacer` is governed by:

$$K_{\text{bid}}(t) = \min\left(2.0, \max\left(0.1, \frac{\text{Budget}_{\text{remaining}}(t)}{\text{Time}_{\text{remaining}}(t) \cdot \text{Target\_Velocity}}\right)\right)$$

Where $\text{Target\_Velocity}$ is the smoothed historical consumption rate:

$$\text{Target\_Velocity} \leftarrow \alpha \cdot \text{Velocity}_{\text{actual}} + (1 - \alpha) \cdot \text{Target\_Velocity}_{\text{prior}}$$

With exponential smoothing factor $\alpha = 0.15$ corresponding to a 6-hour half-life.

#### 7.2.2 Multi-Touch Attribution Shapley Value Model
In `cmd/identity-resolver`, credit allocation $\phi_i(v)$ for touchpoint $i$ across a user conversion journey $N$ is computed using cooperative game theory:

$$\phi_i(v) = \sum_{S \subseteq N \setminus \{i\}} \frac{|S|!(|N| - |S| - 1)!}{|N|!} \left( v(S \cup \{i\}) - v(S) \right)$$

Where $S$ is a subset of touchpoints excluding $i$, and $v(S)$ represents the characteristic conversion probability of sequence $S$.

#### 7.2.3 Inter-Click Interval Variance Botnet Identification
In `cmd/botnet-analyzer`, timer-driven bot traffic is flagged when inter-click arrival variance $\sigma^2$ falls below threshold $\epsilon$:

$$\sigma^2 = \frac{1}{N} \sum_{i=1}^N (\Delta t_i - \bar{\Delta t})^2, \quad \bar{\Delta t} = \frac{1}{N} \sum_{i=1}^N \Delta t_i$$

Where $\Delta t_i = t_i - t_{i-1}$ is the time delta between consecutive clicks from identical `/24` subnets. A subnet is flagged as an automated botnet when $\sigma^2 < 0.005\text{ s}^2$ over sample size $N \ge 30$.

#### 7.2.4 Thompson Sampling Multi-Armed Bandit Model
In `cmd/lander-optimizer`, optimal landing page variation $k^*$ is selected by sampling from Beta distributions:

$$\theta_k \sim \text{Beta}(\alpha_k + 1, \beta_k + 1)$$

$$k^* = \arg\max_{k \in \{1, \dots, K\}} (\theta_k)$$

Where $\alpha_k$ is the total number of recorded conversions for variant $k$, and $\beta_k$ is the number of non-converting clicks.

---

## 8. Recommended Reading & Architectural References

To gain a comprehensive technical understanding of the eSPX platform architecture, microservice boundaries, low-level execution mechanics, and domain protocols, refer to the following documentation sources:

### 8.1 Internal Repository Documentation
- **Microservices Boundary Matrix**: [GUIDE_IDEAS_MICROSERVICES.md](../GUIDE_IDEAS_MICROSERVICES.md) — 18-point rubric for evaluating standalone microservices vs. in-process modular monolith packages.
- **Low-Level System Mechanics**: [CONCEPTS.md](./CONCEPTS.md) — Detailed analysis of OS syscalls (`epoll`, `mmap`), memory hierarchy latency, cache line false sharing padding (`_ [56]byte`), and storage engine comparisons.
- **Go Hot-Path Execution Guidelines**: [GO.md](./GO.md) — Zero-allocation memory rules (`0 allocs/op`), compiler Bounds Check Elimination (BCE), and thread pinning (`runtime.LockOSThread()`).
- **Commercial & Engine Proposals**: [PROPOSALS.md](./PROPOSALS.md) — Hybrid volume licensing models, zero-latency edge lander localization, and line-rate eBPF/XDP filtering.
- **Open work & scale roadmap**: [GAPS.md](./GAPS.md) — Shard orchestrator, chaos backlog, RTB gaps
- **Data Persistence & Database Internals**: [DATABASE.md](./DATABASE.md) — PostgreSQL B+-tree schemas, idempotency catalogs, and ClickHouse LSM-tree batch insertion mechanics.
- **Management Control Plane & REST API**: [MANAGEMENT.md](./MANAGEMENT.md) — Cold-path control plane specifications, background workers, and admin HTTP REST API endpoints.
- **Chaos Engineering & Reliability Guide**: [GUIDE_CHAOS_RELIABILITY.md](../GUIDE_CHAOS_RELIABILITY.md) — Rules R1–R10 for chaos testing, fault injection mechanisms, steady-state metrics, and proof verification.
- **Code Style & Repository Layout**: [GUIDE_STYLE_CODE.md](../GUIDE_STYLE_CODE.md) — Repository organization, flat package conventions, error boundaries, and godoc standards.

### 8.2 External Domain Protocols & Technical Standards
- **Linux eBPF / XDP Architecture**: Kernel network interface driver packet hook (`XDP_DROP`), BPF hash maps (`BPF_MAP_TYPE_LPM_TRIE`), and driver-level packet filtering.
- **ClickHouse LSM-Tree Mechanics**: Documentation for `ReplacingMergeTree`, background part merges, ZSTD compression, and `chquery` resource governance.
- **PostgreSQL Transactional Outbox Pattern**: `SELECT FOR UPDATE SKIP LOCKED` concurrency semantics, Write-Ahead Log (WAL) persistence, and MVCC tuple vacuuming.
- **OpenRTB 2.5 / 3.0 Specifications**: Real-time bidding auction request/response schemas, bid response win/loss notifications, and `NoBidReason` error codes.
- **Third-Party Conversion APIs**: Facebook Conversions API (CAPI), Google Ads Offline Conversion Imports, TikTok Events API, and S2S postback URL macro substitution standards.
