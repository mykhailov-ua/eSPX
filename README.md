# eSPX (Event Stream Pacing)

High-throughput, real-time event ingestion and pacing pipeline.

## Core Features

- **Ingestion**: Event-driven network handler based on `github.com/panjf2000/gnet/v2` with `SO_REUSEPORT` and `TCP_NODELAY` socket configurations.
- **Validation**: Sharded Redis cluster utilizing client-side static hash slot mapping for budget, pacing, and frequency checks.
- **Anti-Fraud**: MaxMind DC/VPN/Proxy checks, Time-To-Click (TTC) velocity checks, and geo-targeting validation.
- **Persistence**: Transactional outbox pattern using PostgreSQL `LISTEN/NOTIFY` and asynchronous multi-row batch writers Claim-and-Persist groups.
- **Serialization**: Schema-optimized binary Protobuf formats utilizing zero-copy unmarshaling via `vtproto`.
- **Infrastructure**: Automated PostgreSQL partition rotation and Nginx dynamic blacklist sync workers.

---

## Ingestion Architecture

### Ingress (Tracker)
- **Networking**: Stateless replicas running in host network mode using `gnet/v2` multi-reactors locked to OS threads via `gnet.WithLockOSThread(true)`.
- **Memory Footprint**: Lock-free, zero-allocation connection-local pool (`connContext`) bound to connection lifetime.
- **Data Parsing**: Zero-copy DFA HTTP/1.1 request stream scanner mapping headers directly from socket ring buffers.

### Edge Caching & Routing
- **Sharding**: Client-side `StaticSlotSharder` executing O(1) constant-time lookups over 1024 virtual slots.
- **Filters**: Atomic multi-constraint Lua scripts evaluating budget allocation, click deduplication, and frequency caps.

### Settlement
- **Processor**: Consumers pulling batch streams from Redis Consumer Groups with integrated Circuit Breaker.
- **Postgres 16**: Strict ACID daily partitions with write idempotency tracking.
- **ClickHouse**: Raw analytics logging with partial failure deduplication state checks.

---

## Design Decisions

| Subsystem | Selected Pattern | Engineering Justification |
| :--- | :--- | :--- |
| **Serialization** | Protobuf (`bytes` fields) | Bypasses reflective marshalling; permits zero-allocation slicing directly from stream buffers. |
| **Networking** | `gnet` + OS Thread Lock | Eliminates goroutine-per-connection scheduling; locks reactors to logical CPU cores at engine boot. |
| **Sharding** | Static Slot Mapping | Bypasses O(ln N) JumpHash overhead. Achieves constant O(1) ~9.5 ns/op lookup via bitwise `key & 1023` masking. |
| **Memory** | Connection-Local Context | Eliminates global `sync.Pool` lock contention, interface boxing, and type assertion overhead. |
| **Budgeting** | Integer Scaling | Micro-unit integer representation (10^6) eliminating decimal/float parsing allocations. |
| **Outbox I/O** | `SKIP LOCKED` | Decouples PG transaction scope from Redis write operations, avoiding connection pool starvation. |

---

## Observability

- **Metrics**: End-to-end telemetry scraped by Prometheus.
- **Visuals**: Grafana dashboards monitoring throughput, memory, and database latencies.
- **Alerting**: Alertmanager routes anomalies to Telegram webhook gateway.
