# M13 — ClickHouse Lifecycle Advanced: Technical Report

**Date:** 2026-07-20  
**Milestone:** M13 (Exec #10)  
**Status:** Shipped

---

## 1. Summary

M13 extends the M6 `CHPartitionJanitor` in the processor binary with off-peak partition recompression (ZSTD codec + `OPTIMIZE … FINAL`), `system.parts` / `system.disks` monitoring, and an emergency drop policy when disk usage exceeds `CH_EMERGENCY_DROP_PERCENT`. Emergency drops notify operators via the existing notifier-backed `OpsAlerter`.

---

## 2. Components Delivered

| ID | Component | Path | Notes |
|:---|:---|:---|:---|
| CHJ-RET | Retention drop (M6) | `internal/database/ch_partition_janitor.go` | `CH_RAW_RETENTION_DAYS`; unchanged semantics |
| CHJ-ZSTD | ZSTD codec migration | `internal/clickhouse/migrate/migrations/00006_raw_zstd_codec.sql` | `payload CODEC(ZSTD(3))` on raw tables |
| CHJ-RC | Off-peak recompress | `internal/database/ch_partition_janitor.go` | `system.parts` HAVING `parts >= threshold` → `OPTIMIZE TABLE … PARTITION … FINAL` |
| CHJ-EMG | Emergency drop | `internal/database/ch_partition_janitor.go` | Oldest non-current-month partition when disk ≥ threshold |
| CHJ-OPS | Ops alert | `internal/management/ops_alerter.go` | `AlertCHEmergencyDrop` → broadcast Telegram/Slack |
| CHJ-WIRE | Processor wiring | `cmd/processor/main.go` | Notifier client + janitor options; graceful `Wait()` on shutdown |

---

## 3. Janitor Pass Order

```
Run()
  ├─ Query system.disks → ad_ch_disk_used_percent
  ├─ disk ≥ CH_EMERGENCY_DROP_PERCENT?
  │     └─ DROP oldest partition (< current YYYYMM) + ops alert + return
  ├─ Retention DROP (partitions older than CH_RAW_RETENTION_DAYS)
  └─ Off-peak UTC window?
        └─ OPTIMIZE FINAL on partition with most active parts (one per pass)
```

Off-peak default: **02:00–06:00 UTC**. One recompress candidate per daily pass keeps the 5-minute janitor timeout safe on large partitions.

---

## 4. Environment Knobs

| Variable | Default | Used by |
|:---|:---|:---|
| `CH_RAW_RETENTION_DAYS` | `180` | Retention drop cutoff |
| `CH_EMERGENCY_DROP_PERCENT` | `0` (disabled) | Trigger emergency drop + alert; set e.g. `90` in production |
| `CH_RECOMPRESS_PARTS_THRESHOLD` | `8` | Minimum active parts in a partition before `OPTIMIZE FINAL` |
| `CH_RECOMPRESS_OFFPEAK_START_UTC` | `2` | Off-peak window start hour (UTC) |
| `CH_RECOMPRESS_OFFPEAK_END_UTC` | `6` | Off-peak window end hour (UTC, exclusive) |
| `OPS_ALERTS_ENABLED` + notifier recipient | — | Required for emergency drop notifications |

---

## 5. New Metrics

| Metric | Type | Purpose |
|:---|:---|:---|
| `ad_ch_disk_used_percent` | Gauge | Disk utilization from `system.disks` |
| `ad_ch_janitor_retention_drop_total` | Counter | Partitions dropped by retention policy |
| `ad_ch_janitor_emergency_drop_total` | Counter | Partitions dropped under emergency policy |
| `ad_ch_janitor_recompress_total` | Counter | Partitions merged via off-peak `OPTIMIZE FINAL` |

---

## 6. Test Results

### M13.2 prescribed commands

```bash
go test ./internal/database/... -run 'Partition|OffPeak|Emergency' -short
# ok   espx/internal/database   0.067s

go test ./internal/management/... -run 'CHEmergency' -short
# ok   espx/internal/management   0.129s
```

### Integration (testcontainers ClickHouse 24.3)

```bash
go test ./internal/database/... -run 'CHPartitionJanitor' -timeout 10m
# ok   espx/internal/database   14.186s
```

| Test | Criterion | Result |
|:---|:---|:---|
| `TestCHPartitionJanitor_Recompress_RealCH` | Many parts → `OPTIMIZE FINAL` reduces part count | **PASS** |
| `TestCHPartitionJanitor_EmergencyDrop_RealCH` | Disk threshold triggers drop + alerter callback | **PASS** |
| `TestCHPartitionJanitor_RetentionDrop_RealCH` | Partition older than retention dropped | **PASS** |
| `TestOpsAlerter_AlertCHEmergencyDrop` | Notifier broadcast on emergency drop | **PASS** |
| `TestCHOffPeakUTC` | Off-peak window logic incl. midnight wrap | **PASS** |

### Build

```bash
go build ./...
# exit 0
```

---

## 7. Architecture Notes

### Emergency vs retention

- **Retention** drops partitions with `YYYYMM < cutoff` derived from `CH_RAW_RETENTION_DAYS`.
- **Emergency** bypasses retention timing: when disk is critical, the janitor drops the **oldest** partition still below the current month, one partition per pass, and skips retention/recompress that cycle to avoid compounding I/O.

### ZSTD

Migration `00006` sets `CODEC(ZSTD(3))` on `payload` for `impressions`, `clicks`, `conversions`, and `fraud_events`. Off-peak `OPTIMIZE FINAL` merges fragmented parts and applies the codec on rewrite.

### Ops alerts

Processor dials notifier when `OPS_ALERTS_ENABLED` and a recipient are configured (same path as management/margin-guard). `AlertCHEmergencyDrop` uses broadcast delivery and 5-minute dedup cooldown per partition key.

---

## 8. Known Gaps / Follow-ups

- **Single partition per pass** — large backlogs of fragmented partitions drain over multiple daily ticks; consider a `CH_JANITOR_MAX_RECOMPRESS_PER_RUN` knob if ops need faster catch-up.
- **Disk on multi-volume CH** — `system.disks` sums all volumes; dedicated `CH_EMERGENCY_DISK_NAME` filter is not implemented.
- **Prometheus alert rule** — add `ad_ch_disk_used_percent` / `ad_ch_janitor_emergency_drop_total` rules in `deploy/monitoring/prometheus.rules.yaml` (ops follow-up).

---

## 9. Files Changed (primary)

- `internal/database/ch_partition_janitor.go` (extended)
- `internal/database/ch_partition_janitor_test.go`, `ch_partition_janitor_integration_test.go` (new/updated)
- `internal/clickhouse/migrate/migrations/00006_raw_zstd_codec.sql` (new)
- `internal/config/env.go`
- `internal/metrics/collectors.go`
- `internal/management/ops_alerter.go`, `ops_alerts_extended_test.go`
- `cmd/processor/main.go`
