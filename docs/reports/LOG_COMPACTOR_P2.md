# Log Compactor P2 — Lag Metrics, Cold Tier, ClickHouse Rollups

## Scope

P2 extends the log-compactor pipeline with observability (lag gauges + Prometheus alerts) and a cold tier that aggregates warm `.compact.zst` segments into ClickHouse `audit_log_rollups`.

Pipeline: hot segments (`.log.zst.ready`) are compacted to warm (`.compact.zst`) by the compactor; the cold rolluper (second loop in the same binary) aggregates warm files into `audit_log_rollups`.

## Lag metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `log_compactor_hot_lag_seconds` | Gauge | Age of oldest uncompacted hot segment |
| `log_compactor_hot_pending_total` | Gauge | Hot segments not yet checkpointed |
| `log_compactor_cold_lag_seconds` | Gauge | Age of oldest warm segment pending CH rollup |
| `log_compactor_warm_pending_total` | Gauge | Warm segments not yet in cold checkpoint |
| `log_compactor_cold_rollups_total` | Counter | Warm segments successfully rolled up |
| `log_compactor_cold_rollup_rows_total` | Counter | Hourly rollup rows inserted |
| `log_compactor_cold_errors_total` | Counter | Cold rollup failures |

Hot lag is refreshed at the end of each `Compactor.RunOnce` pass by scanning all hot segments (`ListHot(now)`), not only age-eligible ones.

Prometheus alert rules (`prometheus-rules.yml`):

- **LogCompactorHotLagHigh** — hot lag > 48h for 30m
- **LogCompactorColdLagHigh** — cold lag > 7d for 1h
- **LogCompactorColdErrors** — any cold error rate over 15m

## Cold tier rollup

### Trigger

When `CH_DSN` is set, cold tier is enabled by default (`LOG_COMPACTOR_COLD_ENABLED=true`). A second goroutine in `cmd/log-compactor` runs `ColdRolluper.Run`.

### Eligibility

Warm files matching `*.compact.zst` with `mtime < now - LOG_COMPACTOR_COLD_WARM_MIN_AGE_D` (default 7 days).

### Aggregation key

`(campaign_id, rollup_hour UTC, event_type)` per warm segment:

- `event_count` — all records in bucket
- `fraud_event_count` — `FraudScore > 0`, `GhostEvent`, or non-empty `FraudReason`
- `billable_event_count` — same policy as warm compaction (`click`, `conversion`, fraud-marked)
- `sample_click_ids` — up to 5 unique click IDs per bucket (ops sampling)

### Idempotency

Separate cold checkpoint (`LOG_COMPACTOR_COLD_CHECKPOINT_PATH`) keyed by warm dest key + SHA256. Re-run skips already-ingested segments.

### ClickHouse schema

`deploy/clickhouse/audit_log_rollups.sql` + appended to `deploy/clickhouse/init.sql`:

```sql
ENGINE = SummingMergeTree()
ORDER BY (campaign_id, rollup_hour, event_type, source_segment, warm_dest_sha256)
TTL rollup_hour + INTERVAL 365 DAY
```

`source_segment` and `warm_dest_sha256` in ORDER BY keep per-segment rows for idempotency; cross-segment queries use `SUM(...) GROUP BY campaign_id, rollup_hour, event_type`.

### Config

| Env | Default | Description |
|-----|---------|-------------|
| `LOG_COMPACTOR_COLD_ENABLED` | `true` if `CH_DSN` set | Enable cold worker |
| `LOG_COMPACTOR_COLD_CHECKPOINT_PATH` | `/var/lib/espx/log-compactor-cold.checkpoint.jsonl` | Cold checkpoint |
| `LOG_COMPACTOR_COLD_WARM_MIN_AGE_D` | `7` | Min warm age before rollup |
| `LOG_COMPACTOR_COLD_WORK_INTERVAL_H` | `24` | Cold scan interval |
| `LOG_COMPACTOR_DELETE_WARM_AFTER_COLD` | `false` | Delete warm after CH insert |
| `CH_DSN` | — | Required when cold enabled |

## EXPLAIN baseline

Run against local ClickHouse:

```bash
clickhouse-client --multiquery < scripts/sql/log_compactor_ch_explain.sql
```

Seeds 100k rollup rows into `tmp_audit_log_rollups_seed` and runs `EXPLAIN indexes = 1` for:

1. Campaign hourly volume (7-day window)
2. Fraud rate by campaign (30-day window)
3. Top billable campaigns (24h)
4. Monthly partition-pruned aggregation

Expected: primary key `(campaign_id, rollup_hour, ...)` serves campaign+time filters; monthly partitions prune on range scans.

## Files touched

| Path | Change |
|------|--------|
| `internal/logcompactor/lag.go` | Hot/cold lag computation |
| `internal/logcompactor/cold_rollup.go` | Warm scan + aggregation |
| `internal/logcompactor/cold_rolluper.go` | Cold worker loop |
| `internal/logcompactor/cold_clickhouse.go` | CH batch inserter |
| `internal/logcompactor/metrics.go` | P2 gauges/counters |
| `internal/logcompactor/cold_test.go` | Unit tests |
| `cmd/log-compactor/main.go` | Cold goroutine + CH connect |
| `internal/config/log_compactor.go` | Cold env config |
| `deploy/clickhouse/init.sql` | `audit_log_rollups` table |
| `prometheus-rules.yml` | Lag/error alerts |
| `scripts/sql/log_compactor_ch_explain.sql` | CH EXPLAIN baseline |

## Test plan

```bash
# Unit tests (fast, -short)
go test ./internal/logcompactor/... -short -count=1 -timeout 30s

# ClickHouse integration + EXPLAIN baseline (testcontainers, no -short)
go test ./internal/logcompactor/... -run 'ClickHouse|Explain' -count=1 -timeout 120s -v
```

Integration tests boot `clickhouse/clickhouse-server:24.3-alpine` with production `init.sql`, insert cold rollups, and emit flat `chaos_proof` summaries for EXPLAIN plans (no tree dump).

Cold-tier integration tests use recent `rollup_hour` values (within TTL); historical fixture dates are dropped immediately by `TTL rollup_hour + INTERVAL 365 DAY`.

Manual EXPLAIN (requires running ClickHouse on `CH_DSN`):

```bash
clickhouse-client --multiquery < scripts/sql/log_compactor_ch_explain.sql
```

## Not in P2

- S3 warm tier cold rollup (local backend only)
- Leader election for multi-instance cold worker
- Postgres admin_audit rollup (separate track)
