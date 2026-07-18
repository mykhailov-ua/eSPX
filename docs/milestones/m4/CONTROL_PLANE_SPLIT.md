# M4 — Control plane (`internal/management`)

**Decision (2026-07):** `internal/management` stays **one flat Go package**. Theme subpackages (`management/fraud/`, `*_compat.go`, `*_wire.go`) were reverted — too many packages for gopls, little gain for a single binary.

**Binary:** `cmd/management` — one process. No `management` → sibling domain explosion.

---

## Layout

```text
cmd/management/main.go

internal/
  management/              # package management — ALL control plane + HTMX admin
    service.go, handler.go, outbox_*.go
    service_fraud.go, handler_fraud.go, blacklist_*.go, …
    pb/                    # gRPC settlement protos only
  adminapi/                # /api/v1 JSON facets (no import management)
  licensing/               # shared with tracker
  database/                # Postgres + ClickHouse connect (no chquery split)
  clickhouse/migrate/      # analytics DDL only
  ingestion/, rtb/         # hot path — unchanged
```

**Navigation:** filename tag (`blacklist_*`, `service_campaigns.go`). IDE search `fraud_` / `service_fraud` — not folder tree.

---

## What stays in `management/`

| Concern | Files (representative) |
| :--- | :--- |
| Lifecycle | `service.go`, `handler.go`, `workers.go`, `errors.go` |
| Outbox | `outbox_worker.go`, `outbox_handlers.go` |
| Settlement gRPC | `settlement_handler.go` |
| Sharding / quota | `redis_global.go`, `quota_*.go`, `service_slot_*.go` |
| Domains | `service_fraud.go`, `handler_supply.go`, `service_consent.go`, … |
| Gateway | `middleware.go`, `http_auth.go`, `rbac.go` |

---

## Sibling packages (allowed)

| Package | Why sibling |
| :--- | :--- |
| `adminapi/` | Separate HTTP surface; PKG-IMP-01 (no import `management`) |
| `licensing/` | Tracker + management entitlements |
| `clickhouse/migrate/` | DDL tool, not runtime connect |

**Not allowed:** `management/privacy/`, `management/outbox/` as Go packages.

---

## Import rules

| ID | Rule |
| :--- | :--- |
| CP-IMP-01 | `ingestion/`, `rtb/`, `cmd/tracker` **must not** import `management` |
| CP-IMP-02 | `adminapi` facets **must not** import `management` |
| CP-IMP-03 | ClickHouse **connect** lives in `internal/database/` — not a separate query package unless a new binary needs it |

---

## CI

```bash
go build ./...
go test ./internal/management/ -short
```

See [INTERNAL_LAYOUT.md](./INTERNAL_LAYOUT.md), [README.md](./README.md).
