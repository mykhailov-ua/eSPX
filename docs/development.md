# Development Guide

Tooling, testing, and maintenance workflow for the `eSPX` sharded ingestion pipeline.

## Requirements

- Go 1.25+
- Docker & Docker Compose
- `buf` CLI

---

## Make Targets

| Target | Command | Purpose |
| :--- | :--- | :--- |
| `make fmt` | `go fmt ./...` | Code styling format. |
| `make proto` | `buf generate` | Compile Protobuf schemas. |
| `make test` | `go test -v ./...` | Execute unit and integration tests. |
| `make build` | `docker build ...` | Compile unified multi-stage Docker image. |

---

## Git Hooks & Local Quality Control

Local CI emulation is handled natively via **Lefthook** to execute lightweight checks without Docker/containers dependency.

- **Pre-commit**: Automatically executes code format checks and statical analysis:
  ```bash
  make lint
  ```
- **Pre-push**: Executes the test suite prior to remote push:
  ```bash
  make test
  ```

Install hooks using:
```bash
lefthook install
```

---

## Ports & Services

| Service | Port | Description |
| :--- | :--- | :--- |
| **Nginx** | 8180 | Edge Load Balancer |
| **Tracker** | 8181-8184 | Stateless Ingestion Instances (`cmd/tracker.go`) |
| **Processor** | 8186 | Async Stream Batch Settlement (`cmd/processor.go`) |
| **Management** | 8188 | Control Plane Gateway (`cmd/management.go`) |
| **Auth Server** | 51051 | gRPC Authentication (`cmd/auth.go`) |
| **Redis Shards** | 6479-6484 | Sharded In-Memory Edge Cache (0-5) |
| **PostgreSQL** | 5440 | Relational ACID database |
| **ClickHouse** | 9100, 8223 | Columnar analytics database |
| **Prometheus** | 9190 | Telemetry Scraper |
| **Alertmanager** | 9093 | Alert Routing |
| **Telegram Proxy** | 8222 | Telegram Alert Webhook (`cmd/telegram.go`) |
| **Grafana** | 3100 | Visualization Server |

---

## CLI Tools

### DLQ Management Utility (`cmd/dlq.go`)

Interacts with Dead Letter Queue stream in Redis.

*   **Archive events to disk**:
    ```bash
    go run cmd/dlq.go -action=archive -stream=ad:events:dlq -dest=dlq_archive.bin -batch=1000
    ```
    Extracts DLQ entries, wraps them into binary length-prefixed `AdDLQEvent` Protobuf segments, writes to disk, and acknowledges/purges entries from Redis.
*   **Restore events from disk**:
    ```bash
    go run cmd/dlq.go -action=restore -dest=dlq_archive.bin -stream=ad:events -batch=1000
    ```
    Parses length-prefixed binary segments and requeues them back into Redis ingestion stream (`ad:events`).
*   **Requeue directly**:
    ```bash
    go run cmd/dlq.go -action=requeue -stream=ad:events:dlq -dest=ad:events -batch=1000
    ```
    Pipes events directly from the DLQ stream back to the active ingestion queue in Redis.

---

## Performance Gate Setup

To prevent hot path latency and memory regressions, pull requests are validated on dedicated bare-metal runners.

### Gate Thresholds

- **Heap Allocations**: Must be exactly `0 allocs/op`.
- **Memory Consumption**: Must be exactly `0 B/op`.
- **Latency Regression**: Must not exceed `12.0%` with statistical significance (p < 0.05).

Local PR comparison can be emulated via the gate parser:
```bash
go run scripts/perf_gate.go baseline_bench.txt pr_bench.txt
```
