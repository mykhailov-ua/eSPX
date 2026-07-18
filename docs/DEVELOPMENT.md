# Development Guide

Environment setup, tooling, and operational procedures for the eSPX codebase.

---

## Requirements

*   Go 1.25+
*   Docker and Docker Compose
*   `buf` CLI (or `make proto`)

---

## Quick Start

```bash
cp .env.example .env
# Build and start the full infrastructure
bash scripts/local-dev/dev_stack.sh build
bash scripts/local-dev/dev_stack.sh full
# Readiness check
bash scripts/local-dev/dev_preflight.sh
```

`dev_stack.sh` launch modes:
*   `infra`: start databases (Postgres, Redis x6, ClickHouse).
*   `full`: start all services (trackers, processors, management, billing, etc.).
*   `sentinel`: start Redis in Sentinel mode.

---

## Working with K3s (Kubernetes)

### Cold Path (Local)
Application services deploy in the `espx` namespace. Infrastructure (databases) stays in Docker Compose.
1.  Install k3s: `bash scripts/k8s/install_k3s.sh`.
2.  Start: `bash scripts/k8s/k8s_cold_path_up.sh`. The script applies migrations, builds images, and runs Terraform.

### Hot Path (Ingestion)
Trackers and Nginx run with `hostNetwork` to minimize network latency.
Start: `bash scripts/k8s/k8s_hot_path_up.sh`.

---

## Code Generation

*   `make proto`: generate Protobuf (vtproto) in `internal/*/pb/*`.
*   `make gen`: generate SQL queries (sqlc) in `internal/*/db/*`.

---

## Management Scripts (`scripts/`)

*   **Local development.**
    *   `dev_stack.sh`: Compose lifecycle management.
    *   `check_deps.sh`: check port availability and database migrations.
    *   `smoke_local.sh`: health check for all running services.
*   **Performance.**
    *   `perf_gate_run.sh`: run performance benchmarks. Verifies zero allocations.
    *   `edge_nic_tune.sh`: network interface tuning (RX ring, IRQ).
*   **Fault tolerance testing.**
    *   `test_chaos.sh`: run Chaos tests in Docker. Requires at least 46 scenarios to pass.
    *   `test_sentinel_failover.sh`: verify Redis failover when a Master node fails.

---

## Ports and Services

| Service | Port | Protocol |
| :--- | :--- | :--- |
| **Nginx** | 8180 | HTTP (Ingress) |
| **Tracker** | 8181–8184 | HTTP (gnet) |
| **Processor** | 8186 | HTTP |
| **Management** | 8188 | HTTP (Admin) |
| **Auth** | 51051 | gRPC |
| **Payment** | 51052 | gRPC |
| **Billing** | 51054 | gRPC |
| **Redis Shards** | 6479–6482 | TCP |
| **PostgreSQL** | 5430 | TCP |
| **ClickHouse** | 9000 | TCP |

---

## Environment Variables (Key)

Cold-path durability and write-concurrency principles: `docs/CONCEPTS.md` §10.

Full list in `.env.example`.
*   `DB_DSN`: Postgres connection string.
*   `REDIS_ADDRS`: list of Redis shard addresses.
*   `FILTER_TIMEOUT_MS`: request processing timeout (100 ms for Prod).
*   `TTC_FAIL_CLOSED`: click-time check policy (Fail-Closed when `true`).
*   **Processor write path:** `PROCESSOR_PG_GATE_SLOTS`, `PROCESSOR_CH_GATE_SLOTS` (`0` = auto); `PROCESSOR_PG_STREAM_MAX_WORKERS`, `PROCESSOR_CH_STREAM_MAX_WORKERS` (`0` = inherit `MAX_WORKERS` / `CH_MAX_WORKERS`); `SYNC_WORKER_MAX_CONCURRENCY`; `CH_SPOOL_SEGMENT_MB`, `CH_SPOOL_MAX_SEGMENTS`.

---

## Anti-Fraud: Operational Procedures

### Emergency Shutdown
To disable the scoring system:
1.  Set `FRAUD_SCORING_ENABLED=false` in environment variables.
2.  Restart `fraud-scorer` or `ivt-detector` workers.

### Manual Corrections
*   **Reset campaign boost.** Used for false anti-fraud triggers. A management API command creates an `ML_SCORE_BOOST` event with a zero value.
*   **Unblock IP.** Remove the entry from `ip_blacklist` in Postgres and send an `UPDATE_BLACKLIST` event to Redis.

All manual actions are recorded in `audit_logs`.
