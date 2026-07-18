# Milestone 4 — Package Layout (index)

**Status:** `management/` and `adminapi/` = **flat** packages; navigate by filename tag.

## Reference layouts

| Path | Rule |
| :--- | :--- |
| `internal/management/` | flat, tags (`blacklist_*`, `service_fraud.go`), only `pb/` subdir |
| `internal/adminapi/` | flat, tags (`billing_handlers.go`, `ops_recon.go`); R1 `db/`/`queries/`/`migrations/`/`pb/` only when owned here (none today — billing/ingestion/management stores) |
| `internal/database/` | Postgres + ClickHouse connect |
| `internal/clickhouse/migrate/` | DDL only |

Plan: [CONTROL_PLANE_SPLIT.md](./CONTROL_PLANE_SPLIT.md), [INTERNAL_LAYOUT.md](./INTERNAL_LAYOUT.md).

## AdminAPI tags

| Tag | Example files | Status |
| :--- | :--- | :--- |
| `billing` | `billing_handlers.go`, `billing_balance.go` | done |
| `ops` | `ops_handlers.go`, `ops_recon.go` | done |
| `selfserve` | `selfserve_handlers.go` | done |
| `reports`, `export`, `licensing` | `reports_*.go`, `export_*.go` | done |
| `dashboards`, `views` | scaffold (M6) | |

## Backlog

- [x] Flat `management/` and `adminapi/` (no theme subpackages)
- [ ] Colocate `handler_*` + `service_*` pairs on touch
- [ ] Wire remaining JSON routes via `adminapi/register.go`
