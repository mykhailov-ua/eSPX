# AdPulse Event Processor Architecture Specification

Technical overview of the sharded ad event ingestion pipeline and storage architecture.

## System Design

The system utilizes a distributed, horizontally scaled architecture to isolate network-bound ingestion from compute-intensive persistence.
1.  **Tracker Pool (Ingress)**: Cluster of 4+ stateless Go replicas in `network_mode: host`.
2.  **Redis Shard Cluster (State)**: 6 independent Redis instances for atomic budget and frequency validation.
3.  **Processor Pool (Egress)**: Decoupled consumer workers reading from the shard cluster and sinking to PostgreSQL and ClickHouse.

## Ingestion Pipeline (Tracker)

### HTTP Ingress
*   **Networking**: Operates in **Host Network Mode** to eliminate Docker bridge overhead and NAT translation, significantly reducing `softirq` and context switching at 100k+ RPS.
*   **Protocol**: HTTP/1.1 with persistent keepalive connections.
*   **Format**: Priority support for **Protobuf** (`application/x-protobuf`). Zero-copy reading from request bodies with object pooling to minimize GC pressure.
*   **Load Balancing**: Nginx upstream distributes traffic across 4 local ports (8081-8084).

### Sharding & State Validation
*   **Consistent Hashing**: Trackers route events to one of 6 Redis shards based on `CampaignID`. This ensures that all events for a specific campaign are processed by the same shard, maintaining budget atomicity.
*   **Atomic LUA Filter**: A single LUA script execution per event handles:
    1.  ClickID Deduplication (45s window).
    2.  Real-time Budget Reservation.
    3.  Campaign Frequency Capping.
    4.  Append to local shard Redis Stream.
*   **GOMEMLIMIT Hardening**: Each Tracker replica is constrained with `GOMEMLIMIT=700MiB` and `GOGC=50` to ensure predictable memory behavior and prevent OOM-induced cascading failures.

### Message Backbone (Redis Streams)
*   **Sharded Streams**: Events are distributed across 6 streams (one per shard).
*   **Independent Consumers**: Processor workers scale linearly with the number of shards.

## Persistence Strategy (Processor)

### PostgreSQL Sink (`group_pg`)
*   **Data**: Transactional aggregates and budget state.
*   **Batching**: 20,000 events per write.
*   **Partitioning**: Daily time-based partitioning on `created_date` for efficient log rotation.

### ClickHouse Sink (`group_ch`)
*   **Data**: High-volume analytical logs.
*   **Batch Size**: 50,000 events.
*   **Memory Management**: ClickHouse server constrained to 4GB RAM to prevent competition with Redis/Trackers on the same host.

## Reliability and Fault Tolerance

### Circuit Breaker & DLQ
*   **Circuit Breaker**: Protects downstream databases by pausing consumption when failure thresholds are met.
*   **Dead Letter Queue (DLQ)**: Failed events after 5 retries are moved to a dedicated stream for manual recovery.

### Health Checks
Services implement active health checks that perform `Ping` operations on all critical dependencies (Postgres, ClickHouse, and all 6 Redis shards) before returning a `200 OK` status.

## Deployment Topology

### Single-Host Capacity
- **CPU**: 12 Cores.
- **RAM**: 16GB (32GB recommended for headroom).
- **Network**: Host networking required for 30k+ RPS on a single node.

### Horizontal Scaling Strategy
To achieve 100k+ RPS:
1.  **Scale Out**: Duplicate the stack across 3 physical hosts.
2.  **Edge Load Balancing**: Distribute traffic at the edge (Cloud LB or Hardware F5) to the host-networked Nginx instances.
3.  **Redis Sharding**: Increase the number of Redis shards if budget validation becomes a bottleneck (requires consistent hashing update).
