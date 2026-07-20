# M6 — Day-2 Operations & Analytics Pipeline: Technical Report

**Date:** 2026-07-20  
**Milestone:** M6 (Exec #6)  
**Status:** Shipped

---

## 1. Summary

M6 delivers production operability without tracker restart for config changes: incremental hot reload, split liveness/readiness probes, governed ClickHouse admin queries, CH partition retention, and pipeline backlog gates on processor readiness.

---

## 2. Components Delivered

| ID | Component | Path | Notes |
|:---|:---|:---|:---|
| HR-PUB | Incremental pub/sub reload | `internal/ingestion/registry.go` | `campaigns:update` → `UpdateAndWarmCampaign(id)`; no full `Sync()` |
| HR-REG | Lock-free registry | `internal/ingestion/registry.go` | `atomic.Value` COW; add/remove on status change |
| HR-BL | Blacklist fan-out + edge signal | `internal/management/outbox_worker.go` | All shards + `blacklist:update` pub on shard 0 |
| HR-KEYS | Redis hash tags | `internal/ingestion/redis_keys.go` | `{campaign_id}` prefix on Lua keys |
| HR-WARM | Budget warm metric | `internal/metrics/collectors.go` | `ad_registry_warm_duration_seconds` |
| HC-* | Health split | `internal/health/health.go` | `/healthz` (no I/O) + `/readyz` (cached probes) |
| CHG-* | CH query governance | `internal/database/chquery.go` | `readonly=1`, memory/exec caps |
| CHJ-* | CH partition janitor | `internal/database/ch_partition_janitor.go` | `CH_RAW_RETENTION_DAYS` (default 180) |
| PIPE-* | Processor readiness gates | `cmd/processor/main.go` | Spool segments, stream lag, XLEN metrics |

---

## 3. New / Updated Metrics

| Metric | Type | Purpose |
|:---|:---|:---|
| `ad_registry_warm_duration_seconds` | Histogram | Incremental warm latency (HR-WARM SLO < 2 s) |
| `ad_blacklist_replication_lag_seconds` | Histogram | Outbox → Redis blacklist fan-out (HR-BL SLO p99 < 5 s) |
| `ad_ch_spool_segments` | Gauge | Current spool segment count for ops/readyz |
| `ad_processor_stream_xlen` | Gauge | Per-shard `XLEN` on main stream (PIPE-*) |

Existing metrics retained: `ad_registry_sync_lag_seconds`, `ad_tracker_health_degraded`, `ad_processor_stream_lag_seconds`, `ad_ch_spool_*`.

### Environment knobs

| Variable | Default | Used by |
|:---|:---|:---|
| `CH_READONLY_DSN` | falls back to `CH_DSN` | `chquery` / admin reports |
| `CH_RAW_RETENTION_DAYS` | `180` | `CHPartitionJanitor` |
| `CH_SPOOL_MAX_SEGMENTS` | `8` | Processor `/readyz` gate |
| `PROCESSOR_STREAM_LAG_MAX_SEC` | `120` | Processor `/readyz` gate |

---

## 4. Health Endpoints

| Process | Liveness | Readiness | Backend check |
|:---|:---|:---|:---|
| **tracker** (gnet + metrics port) | `GET /healthz` — atomic counter only, 0 allocs | `GET /readyz` — cached PG + Redis atomics | `StartHealthProbe` every 2 s |
| **processor** | `GET /healthz` | `GET /readyz` | PG + CH + Redis + spool + stream lag |
| **management** | `GET /healthz` | `GET /readyz` | PG + Redis |

Legacy `GET /health` aliases `/readyz` on all services for backward compatibility.

K8s manifests updated: `livenessProbe` → `/healthz`, `readinessProbe` → `/readyz` in `deploy/k8s/hot-path/deployment-trackers.yaml.tpl`, `deploy/k8s/apps/deployment-processor.yaml.tpl`, `deploy/k8s/apps/deployment-management.yaml`.

---

## 5. Test Results

### M6.5 prescribed commands

```bash
go test ./internal/ingestion/... -run 'Registry|Health|Spool' -short
# ok   espx/internal/ingestion   1.855s

go test ./internal/database/... -run 'CHQuery|Partition' -short
# ok   espx/internal/database   0.028s

go test ./internal/adminapi/... -run 'Freshness' -short
# ok   espx/internal/adminapi   0.032s
```

### Criterion coverage

| Test | Criterion | Result |
|:---|:---|:---|
| `TestRegistry_StartWatch_IncrementalOnlyOneCampaign` | HR-PUB: no full catalog scan | **PASS** (logic); requires Redis for integration run |
| `TestTrackerReadyz_RedisDown_503` | HC-READY: Redis down → 503 | **PASS** |
| `TestHealthz_ZeroAllocs` | `/healthz` 0 allocs/op | **PASS** (0 allocs on gnet prebuilt response path) |
| `TestCampaignKeys_CrossSlotColocation` | HR-KEYS: same hash slot | **PASS** |
| `TestCHPartitionJanitor_DropOld` | CHJ-DROP: partition cutoff logic | **PASS** |
| `TestCHQuery_Freshness` | CHG freshness helper | **PASS** |
| `TestReports_FreshnessWithoutCH` | Admin freshness DTO | **PASS** |
| `TestChaos_*Spool*` | Chaos R10 spool recovery | **PASS** (short mode) |
| `TestCHQuery_HeavyGroupByKilled` | CHG-ERR OOM guard | **Skipped** (requires live CH integration) |

Integration tests requiring Docker (Redis/Postgres) were not executed in the CI sandbox; unit and short-mode suites pass.

### Build

```bash
go build ./...
# exit 0
```

---

## 6. Architecture Notes

### Hot reload path

```
management UPDATE → campaigns:update (UUID)
    → Registry.StartWatch
    → UpdateAndWarmCampaign(id)
        → GetCampaignFull (single row)
        → atomic map swap (add if ACTIVE, remove otherwise)
        → BudgetCacheWarmer.WarmOne
```

### Processor readiness

Background probe (2 s) sets `/readyz` false when:
- Any dependency ping fails
- `chStore.Spool().SegmentCount() > CH_SPOOL_MAX_SEGMENTS`
- `ProcessorStreamLagSec() > PROCESSOR_STREAM_LAG_MAX_SEC`

Probe also publishes `ad_processor_stream_xlen{shard}` and `ad_ch_spool_segments`.

### CH admin path

All `service_campaign_stats` hourly queries and `adminapi` report freshness now route through `database.CHQuery` with `SETTINGS readonly=1, max_memory_usage, max_execution_time`.

---

## 7. Known Gaps / Follow-ups

- **CHG-ERR integration test** — heavy `GROUP BY` kill test needs live ClickHouse in integration CI.
- **Chaos proofs** — `registry_incremental_reload lag_p99_lt_5s` and `clickhouse_outage_10m spool_recovered` scripted proofs remain in `scripts/chaos-drills`; underlying spool chaos tests pass in short mode.
- **Remaining scaffold reports** — 12+ `NOT_IMPLEMENTED` admin routes unchanged; placements/keywords use mock rows but real freshness when `CHQuery` is wired at registration time.

---

## 8. Files Changed (primary)

- `internal/health/health.go` (new)
- `internal/database/chquery.go`, `ch_partition_janitor.go` (new)
- `internal/ingestion/registry.go`, `redis_keys.go`, `processor_health.go`, `handler.go`
- `internal/management/ops.go`, `outbox_worker.go`, `service_campaign_stats.go`
- `internal/adminapi/reports_handlers.go`
- `cmd/tracker/main.go`, `cmd/processor/main.go`
- `internal/metrics/collectors.go`, `internal/config/env.go`
- `deploy/k8s/**` probe migration
- Tests: `health_test.go`, `registry_pubsub_test.go`, `redis_keys_test.go`, `chquery_test.go`, `ch_partition_janitor_test.go`, `reports_freshness_test.go`
