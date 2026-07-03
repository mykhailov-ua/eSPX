# eSPX Microservices & Integration Ideas

This document outlines the strategy for expanding the eSPX ecosystem with new microservices, choosing appropriate communication protocols, and maintaining the system's strict performance SLAs.

## 1. Architectural Principles

eSPX operates with three distinct latency planes. Any new service must be categorized accordingly:

| Plane | Latency Target | Technology | Rule |
| :--- | :--- | :--- | :--- |
| **Hot Path** | < 100ms E2E | gnet, Redis Lua, In-process Go | **No synchronous RPC**. Data must be pre-synced to Redis or local memory. |
| **Warm Path** | 100ms - 1s | Edge Lua, BPF, Async Workers | Sidecar-based sync, timer-based shared dicts. |
| **Cold Path** | > 1s | gRPC, HTTP, Postgres/ClickHouse | Standard microservices, outbox pattern for side effects. |

---

## 2. Communication Protocols

### gRPC (TCP Localhost / Internal Network)
*   **Use for:** Internal control plane APIs (Auth, Payment, Settlement).
*   **Ports:** `5105x` range.
*   **Pattern:** Protobuf contracts in `api/*.proto`, `vtproto` for performance.
*   **Security:** `x-internal-token` metadata for service-to-service trust.

### UDS (Unix Domain Sockets)
*   **Use for:** High-performance sidecars co-located on the same host (e.g., Tracker sidecars).
*   **Benefit:** ~2x faster than loopback TCP, no port exposure, file-based permissions.
*   **Target:** Fraud scoring models, heavy RTB auction logic, log shippers.

### Redis-as-a-Bus (Async)
*   **Use for:** Cross-service side effects, event streaming, and global state replication.
*   **Patterns:**
    *   **Outbox:** PG -> Outbox Worker -> Redis (e.g., campaign updates).
    *   **Streams:** Redis Stream -> Processor (e.g., event settlement).
    *   **Global Replicate:** Write to all shards for edge local lookups (e.g., blacklists).

### HTTP
*   **Use for:** External webhooks (Stripe), Admin UI (HTMX), and Ops endpoints (`/metrics`, `/health`).

---

## 3. Service Ideas by Plane

### A. Hot Path & Ingestion (Sidecars)
*   **Fraud L1 Scorer (ML):** A UDS sidecar co-located with the Tracker. It receives raw headers/payloads and returns a probability score.
    *   *Integration:* Tracker calls via UDS (budget <2ms) or reads a score pre-calculated by Edge.
    *   *Hot Path:* **0 allocs/op**; use pre-allocated memory arenas for model inputs; zero-copy payload passing.
    *   *Chaos:* **Fail-Open** strategy (default score 0) if sidecar is unreachable or exceeds 2ms latency. Must include `chaos_proof fault=ml_sidecar_timeout`.
    *   *Implementation:*
        1.  Create `cmd/fraud-scorer` using a lightweight ML runtime (e.g., `onnxruntime-go` or `cgo` bindings to XGBoost).
        2.  Listen on a Unix Domain Socket (e.g., `/var/run/espx/fraud.sock`).
        3.  Implement a custom binary protocol or use `vtproto` with pooling.
        4.  In `internal/ads/handler.go`, add a `UDSClient` that sends a subset of `domain.Event` fields (IP, UA, Fingerprint).
        5.  Use a `CircuitBreaker` with a 2ms timeout. If it trips, default to `fraud_score=0`.

*   **RTB Auction Sidecar:** If auction logic becomes too heavy for the main Tracker process.
    *   *Integration:* UDS call from `processTrack()`. Result injected into `campaign_id`.
    *   *Hot Path:* `vtproto` with pooling for request/response; monotonic time (`nanotime`) for bid deadlines.
    *   *Chaos:* Strict **tmax** enforcement; fencing tokens if auction state is distributed across nodes.
    *   *Implementation:*
        1.  Create `cmd/rtb-sidecar` to isolate complex bidder selection and pricing logic.
        2.  Use `UDS` for communication to keep RTT < 1ms.
        3.  Implement `RunAuction(TargetingData) -> Winner` using a pre-loaded in-memory catalog of active bids.
        4.  The sidecar should refresh its catalog via Redis Pub/Sub (Shard 0) or a periodic poll from Management.
        5.  Tracker uses `RTB_MODE=sidecar` to delegate the auction.

*   **Geo/ASN Data Refresher:** A background daemon that updates MaxMind DBs or ASN maps in a shared volume.
    *   *Hot Path:* Zero-copy **mmap** for data files; no heap allocations during lookup.
    *   *Chaos:* Atomic file swaps; checksum verification before loading; must not block Tracker during reload.
    *   *Implementation:*
        1.  Create `cmd/geo-refresher`.
        2.  Periodically (e.g., weekly) check MaxMind/ASN providers for updates.
        3.  Download to `/deploy/geoip/new_db.mmdb`, verify SHA256.
        4.  Use `os.Rename` for an atomic swap to `/deploy/geoip/city.mmdb`.
        5.  Send `SIGHUP` to Tracker processes or update a Redis key `config:geoip_version` to trigger a re-mmap in `internal/ads/geo.go`.

### B. Warm Path (Edge & Perimeter)
*   **Edge Config Agent:** A daemon that polls Management/Redis and pushes updates directly into XDP/eBPF maps or Nginx `lua_shared_dict`.
    *   *Warm Path:* Timer-based sync (5s); no per-request blocking calls.
    *   *Chaos:* **Stale cache fallback** if Management is unreachable; circuit breaker on push failures.
    *   *Implementation:*
        1.  Create `cmd/edge-agent` running as a sidecar to Nginx.
        2.  Poll Redis Shard 0 for `config:values` and `blacklist:*`.
        3.  For Nginx: Expose a local-only HTTP endpoint in `nginx.conf` (e.g., `POST /internal/update_config`) that the agent calls to update `lua_shared_dict`.
        4.  For BPF: Use `cilium/ebpf` to update pinned maps at `/sys/fs/bpf/espx/`.

*   **Dynamic Rate-Limiter:** A service that adjusts campaign rate limits in real-time based on ClickHouse traffic analysis.
    *   *Warm Path:* `lua_shared_dict` counters; atomic increments.
    *   *Chaos:* Circuit breaker to prevent over-throttling if ClickHouse metrics are delayed or skewed.
    *   *Implementation:*
        1.  Add a `RateLimitController` to `cmd/processor` or a new `cmd/pacing-engine`.
        2.  Query ClickHouse every 30s: `SELECT campaign_id, count() FROM impressions WHERE timestamp > now() - 1m GROUP BY campaign_id`.
        3.  Compare against target RPS. If exceeding, calculate a new `rate_limit_per_min`.
        4.  Update Redis `config:values` via Management's `SyncSystemState` logic.

### C. Cold Path (Business Logic)
*   **IVT Anomaly Detector:** Analyzes ClickHouse logs for botnet clusters. [COMPLETED]
    *   *Integration:* Periodically runs -> Enqueues `blacklist:fraud` events via Management Outbox.
    *   *Cold Path:* Idiomatic Go; `pgx` batching; standard `net/http`.
    *   *Chaos:* **Exactly-Once** blocklist updates via `sync_idempotency`; backpressure if Outbox is full.
    *   *Implementation:*
        1.  Create `cmd/ivt-detector`.
        2.  Run complex SQL queries (e.g., finding IPs with high click-to-imp ratios or shared fingerprints).
        3.  For each suspicious IP, call `ManagementService.AddBlacklistEntry(ip, reason="ivt_detected")`.
        4.  Management handles the transactional outbox to ensure all Redis shards are eventually updated.

*   **Notification Hub:** Centralized service for Telegram, Slack, and Email alerts. [COMPLETED]
    *   *Integration:* Accepts HTTP/gRPC from Alertmanager or Management.
    *   *Cold Path:* Standard Go; retry with exponential backoff.
    *   *Chaos:* Circuit breaker for external providers; must not lose alerts on transient network failures.
    *   *Implementation:*
        1.  Create `cmd/notifier` with a gRPC interface (`api/notifier.proto`).
        2.  Implement providers for Telegram (reusing `cmd/telegram` logic), Slack, and SMTP.
        3.  Use a persistent queue (e.g., a simple PG table) to store pending notifications.
        4.  A background worker processes the queue with retries.

*   **Audit Log Evacuator:** Tails mmap log segments and ships them to long-term storage (S3/GCS). [COMPLETED]
    *   *Cold Path:* `io.Copy` with fixed 32KB buffers; no dynamic appends.
    *   *Chaos:* **Checkpoint persistence**; exactly-once delivery via S3 ETag/Multipart; must handle log rotation races.
    *   *Implementation:*
        1.  Create `cmd/log-evacuator`.
        2.  Use `fsnotify` to watch `/var/log/espx/` for `.log.zst.ready` files.
        3.  Upload to S3 using the AWS SDK Go v2.
        4.  Store the "last uploaded file" in a local SQLite or flat file to resume after crashes.

*   **Billing & Ledger Expansion:** Dedicated service for complex financial reporting, tax calculation, and invoice generation. [COMPLETED]
    *   *Cold Path:* Strict `pgx` transactions; **BIGINT micro-units** for all money columns.
    *   *Chaos:* **State Invariant Assertions** ($\sum Spend = Ledger$); must include `chaos_proof fault=ledger_drift_check`.
    *   *Implementation:*
        1.  Create `cmd/billing`.
        2.  Define a new schema `billing` in Postgres.
        3.  Implement `GenerateInvoice(customer_id, month)` which aggregates `ledger` entries.
        4.  Add a `TaxCalculator` component that applies VAT/Sales Tax based on customer metadata.
        5.  Expose an HTMX-based dashboard in Management by calling the Billing gRPC service.

---

## 4. Implementation Playbook

1.  **Define Contract:** Create `.proto` in `api/`. Use `vtproto` for hot-path consumers.
2.  **Service Skeleton:** Place in `cmd/<name>/`. Use `internal/config` for port assignment (`51054+`).
3.  **Database:** Use a separate schema or database if the domain is isolated (like `payment`).
4.  **Client Logic:** Implement a lazy-dialing client in `internal/management/` or `internal/ads/` with optional enablement via environment variables.
5.  **Side Effects:** Always use the **Transactional Outbox** pattern. Never perform dual-writes to PG and Redis/gRPC.
6.  **Observability:** Expose `/metrics` on a dedicated port. Add Grafana dashboard and Prometheus alerts.

---

## 5. Anti-Patterns to Avoid

*   **Synchronous gRPC on `/track`:** Even a 5ms RPC call can destroy p99 latency under high RPS.
*   **Per-request Redis from Nginx:** Use `lua_shared_dict` with timer-based sync instead.
*   **Sharing Sharding Logic:** Any service writing to Redis by `campaign_id` **must** use `StaticSlotSharder` to avoid key divergence.
*   **Reflection on Hot Path:** Stick to `vtproto` or manual byte-slice walking.

---

## 6. Reliability & Performance Standards

All new services must adhere to the following standards derived from `CHAOS.md` and project rules:

### Hot Path Requirements
*   **Zero Allocations:** Must achieve 0 allocs/op in benchmarks. Use `sync.Pool`, DFA scanners, and pre-allocated buffers.
*   **No Reflection:** Forbidden. Use code generation (`vtproto`) or manual parsing.
*   **Monotonic Time:** Use `nanotime` for all latency and timeout calculations.
*   **Concurrency:** Use lock-free atomics, MPSC rings, and avoid mutexes where possible.
*   **Memory:** Prefer SoA (Structure of Arrays) and stack-fixed arrays.

### Cold Path Requirements
*   **Idiomatic Go:** Follow standard library patterns.
*   **Database:** Use `pgx` pools and batching. All money must be in **BIGINT micro-units**.
*   **Integrity:** Use the **Transactional Outbox** pattern for all external side effects.

### Chaos Engineering (CHAOS.md)
*   **Steady State:** Define and monitor steady-state metrics (RPS, Latency p99, Error Rate).
*   **Blast Radius:** Services must be isolated (e.g., via sharding) to minimize the impact of failures.
*   **Chaos Proofs:** Integration tests must output `chaos_proof fault=<name>` to verify resilience in CI.
*   **Idempotency:** Mandatory for all write operations. Use `click_id` or `idempotency_key`.
*   **Fencing:** Use epoch-based fencing tokens for any distributed leader/state logic.
