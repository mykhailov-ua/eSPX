# M4 — Layout by domain meaning (not layer tags)

## Problem

`internal/management` sorts files by **transport/business tag**:

```text
handler_campaigns.go    service_campaigns.go
handler_supply.go       service_supply.go
```

Layer-oriented naming: one feature = grep two prefixes.

## Target

**One flat package** per deployable binary. Navigate by **filename tag**, not subfolders.

```text
internal/management/          # package management — flat
  handler.go, service.go, outbox_worker.go
  blacklist_janitor.go, service_fraud.go, handler_delivery.go
  pb/                         # generated only

internal/adminapi/            # package adminapi — flat
  register.go, errors.go
  billing_handlers.go, ops_handlers.go, selfserve_handlers.go, …
  # no db/queries/migrations/pb today — SQL via billing/db, ingestion/sqlc; protos via billing/pb

internal/licensing/           # sibling — shared with tracker
```

Rules:

1. **Tag = first `_` segment** — `blacklist_janitor.go`; for `service_<theme>_*.go` use `<theme>`.
2. **No `management/<theme>/` subpackages** — kills gopls; one binary = one package.
3. **Colocate on touch** — merge `handler_fraud.go` + `service_fraud.go` → `fraud_config.go` when editing.
4. **Extract siblings** only for: `adminapi/` (JSON API), `licensing/` (multi-binary), hot-path packages (`ingestion/`, `rtb/`).
5. **Database** — Postgres + ClickHouse connect in `internal/database/`; `internal/clickhouse/migrate/` = DDL only.

Hot path stays **flat** per R1.

## Status

| Item | Status |
| :--- | :--- |
| `adminapi` flat (tag filenames) | **done** |
| `management` flat | **done** |
| Colocate `handler_*` + `service_*` on touch | ongoing |
| `licensing/` sibling | done |
