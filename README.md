# AdPulse Event Processor

High-throughput ad event ingestion and processing pipeline. Optimized for low-latency state validation and high-frequency data persistence.

## System Architecture

### Ingestion Layer (Tracker)
- **Horizontal Scaling**: 4 independent replicas behind Nginx load balancer.
- **Protobuf Ingestion**: Binary ingestion via `application/x-protobuf` to minimize CPU cycles spent on deserialization and heap allocation.
- **Stateless Execution**: Trackers do not maintain local state; all validation is offloaded to the sharded cache layer.
- **Host Networking**: Services operate in `network_mode: host` to eliminate Docker network bridge overhead (docker-proxy/NAT).

### State & Cache Layer (Sharded Redis)
- **Horizontal Sharding**: 6 independent Redis instances using consistent hashing by `CampaignID`.
- **Atomic Operations**: Business logic (budget validation, frequency capping, deduplication) is executed via Lua scripts to ensure atomicity and reduce round-trips.
- **Deduplication**: Bloom-filter-like behavior with short-term (45s) TTL for ClickIDs to prevent ingestion bursts from double-charging.

### Persistence Layer (Async Processor)
- **Stream Processing**: Decoupled consumer group reading from Redis Streams.
- **Dual Store**: 
  - **PostgreSQL**: Transactional storage for campaign state and budget aggregates.
  - **ClickHouse**: Analytical storage for raw event logs, optimized for high-volume writes (50k event batches).
- **Partitioning**: Automated PostgreSQL partitioning to maintain query performance over time.

## Performance Design Decisions

| Feature | Decision | Rationale |
|---------|----------|-----------|
| **Serialization** | Protobuf | Reduces payload size and CPU overhead by ~70% compared to JSON. |
| **Networking** | Host Mode | Minimizes softirq and context switching by bypassing the virtual network stack. |
| **Memory** | GOMEMLIMIT | Set to 700MiB/Tracker to force frequent, low-latency GC cycles, preventing OOM. |
| **Deduplication**| Redis TTL | 45s window balances memory occupancy and protection against high-frequency duplicate bursts. |
| **Sharding** | Redis Shards| Eliminates Redis single-thread bottleneck by distributing load across 6 cores. |

## Deployment & Configuration

### Prerequisites
- Docker Engine with Host Networking support.
- 16GB+ RAM (32GB recommended for full test load).

### Infrastructure Limits
- **ClickHouse**: Limited to 4GB RAM via `memory_limit.xml`.
- **Redis**: Each shard constrained to 768MB RAM.
- **Trackers**: GC tuned with `GOGC=50` for faster memory recycling.

## Observability & Health

### Monitoring
- **Grafana**: `http://localhost:3000` (Pre-provisioned dashboard: "AdPulse Ingestion Performance").
- **Prometheus**: Scrapes metrics from all 4 trackers and the processor via host-networked endpoints.

### Health Checks
- **Tracker**: `/health` checks active connectivity to Postgres and all 6 Redis shards.
- **Processor**: `/health` checks connectivity to Postgres, ClickHouse, and all 6 Redis shards.

## Scaling Beyond 100k RPS
To scale the system beyond current hardware limits:
1. Increase the number of Redis shards (update consistent hashing logic in `unified_filter.go`).
2. Deploy additional Tracker nodes on separate physical hosts and update Nginx upstream.
3. Scale ClickHouse horizontally using a distributed cluster.
