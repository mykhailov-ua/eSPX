# Milestone 3 — Commercial Platform (layout index)

**Status:** core complete (licensing + subscriptions + hot-path gates). Admin report waves (W1–W6) deferred to Milestone 4 facets.

## Code layout

| Path | Role |
| :--- | :--- |
| `internal/licensing/` | Ed25519 verify, entitlements, `Effective()`, mmap `LicenseSpool`, JSON guard |
| `internal/adminapi/licensing/` | M3 JSON API facet (subscription, usage, quota, license status) |
| `cmd/license-server/` | Vendor-side license issue / renew / heartbeat |
| `internal/licensing/watcher.go` | Async `LicenseWatcher` + spool recovery |
| `internal/licensing/client.go` | Vendor license-server HTTP client |
| `internal/licensing/entitlement_sync_buffer.go` | Bounded entitlement refresh queue (OOM guard) |
| `internal/management/outbox_handlers.go` | `UPDATE_ENTITLEMENTS` → Redis + registry pub/sub |
| `internal/ingestion/filter_entitlements.go` | Hot-path license / RPD / feature gates |
| `internal/ingestion/registry.go` | Entitlements snapshot sync |
| `internal/billing/migrations/00002_add_subscriptions.sql` | Plans, subscriptions, meters, `license_status` |
| `internal/billing/migrations/00003_vendor_licensing.sql` | Vendor `licenses`, `deployments`, renewals |

## Tests and CI

| Path | Role |
| :--- | :--- |
| `tests/integration/licensing_subscription_test.go` | End-to-end licensing + billing + hot-path |
| `internal/billing/licensing_explain_test.go` | `EXPLAIN (ANALYZE)` for M3 SQL |
| `internal/licensing/licensing_chaos_test.go` | Spool / JSON / mmap chaos |
| `internal/management/licensing_chaos_test.go` | Outbox + entitlement buffer chaos |
| `scripts/sql-explain/licensing_explain.sql` | Query plan templates |
| `scripts/chaos-drills/m3/README.md` | M3 `chaos_proof` catalog |

## Deferred (M4+)

- `internal/adminapi/reports/`, `dashboards/`, `views/` — W1–W6 buyer/finance/adops UX
- `SUB-DAILY-FLUSH` worker (`DailyQuotaFlushWorker`)
- `espx-install license` wizard (M9)
