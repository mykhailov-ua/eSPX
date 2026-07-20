# M3-T — Commercial PU Packaging: Technical Report

**Date:** 2026-07-20  
**Milestone:** M3-T (Exec #7)  
**Status:** Shipped

---

## 1. Summary

M3-T delivers hybrid volume licensing (ESPX-LP-2026-V1): JWT volume bands S/M/L, subsystem module flags, weighted billable-event metering, ingress protection gates, and zero-allocation hot-path license enforcement. Cold-path workers roll up ClickHouse audit data into `billing.usage_meters` for invoice overage lines.

---

## 2. Components Delivered

| ID | Component | Path | Notes |
|:---|:---|:---|:---|
| PU-BAND | Volume bands S/M/L + PU matrix | `internal/licensing/volume.go` | `BandIncludedEvents`, `BasePU`, `ModuleCoefficients`, `MonthlyPU()` |
| PU-FEAT | Module feature flags | `internal/licensing/features.go`, `entitlements.go` | `openrtb_engine`, `ivt_ml_detector`, `ebpf_xdp_edge`; `rtb_live` alias |
| PU-JWT | JWT `volume_band` claim | `internal/licensing/jwt_claims.go` | Parsed via `ParseVolumeBand()` |
| PU-HOT | Hot-path license filter | `internal/ingestion/filter_license.go` | Rejects `EXPIRED`/`REVOKED` only; grace continues |
| PU-METER | Hourly CH rollup worker | `internal/management/volume_meter_worker.go` | `audit_log_rollups` → weighted units → `usage_meters` |
| PU-DAILY | Optional daily flush | `internal/management/usage_daily_worker.go` | `USAGE_DAILY_FLUSH_ENABLED=1` |
| PU-UDP | JWT RPS → UDP quotas | `internal/management/udp_control_server.go` | Splits `max_rps` across shards from `license_status` |
| PU-GATE | Deployment module gates | `internal/licensing/deployment_gate.go` | PG snapshot for cold-path workers |
| PU-WATCH | License watcher Redis fields | `internal/licensing/watcher.go` | Publishes module flags + `volume_band` |

---

## 3. Weighted Billable Events

| Category | Weight | Event types (examples) |
|:---|---:|:---|
| Accepted | 1.0 | `click`, `impression`, `conversion`, `bid` |
| Dedup reject | 0.1 | `duplicate`, `dedup`, `freq`, `fcap` |
| eBPF drop | 0.0 | `ebpf_drop`, `l3_blocklist`, `xdp_drop` |

Formula: `billable_units = Σ (count[type] × weight[type])`

---

## 4. Module Enforcement

| Binary | Flag | Behavior when disabled |
|:---|:---|:---|
| `tracker` | `openrtb_engine` | `bid`/`rtb` events → `filterRejectLicenseExpired` |
| `processor` | `ml_fraud_boost` | Fraud micro-batcher not started |
| `ivt-detector` | `ivt_ml_detector` | Process exits at startup |
| `edge-bpf-sync` | `ebpf_xdp_edge` | Sync loop idle (fail-open if Redis key missing) |

---

## 5. Environment Knobs

| Variable | Default | Used by |
|:---|:---|:---|
| `ESPX_LICENSE_PUBLIC_KEY` | — | Enables `LicenseWatcher` in management |
| `VOLUME_METER_ENABLED` | on (when CH enabled) | Hourly rollup |
| `VOLUME_METER_INTERVAL` | `1h` | Rollup tick |
| `USAGE_DAILY_FLUSH_ENABLED` | `0` | Daily `usage_daily` snapshot |
| `USAGE_DAILY_FLUSH_INTERVAL` | `24h` | Flush tick |

---

## 6. Test Results

```bash
go test ./internal/licensing/... -run 'Volume|Grace|Chaos_LicenseServer|FeatureSet' -short
# ok   espx/internal/licensing   0.004s

go test ./internal/ingestion/... -run 'License|FilterLicense' -short
# ok   espx/internal/ingestion   0.028s

go test ./internal/management/... -run 'Volume|Weighted' -short
# ok   espx/internal/management   0.039s

go test ./internal/ingestion/... -bench=BenchmarkFilterLicense -benchmem -run=^$
# BenchmarkFilterLicense-12   159156097   7.724 ns/op   0 B/op   0 allocs/op
```

### Criterion coverage

| Test | Criterion | Result |
|:---|:---|:---|
| `TestWeightedBillableUnits_goldenFixture` | Weighted rollup math | **PASS** |
| `TestComputeWeightedUnitsFromRows_goldenFixture` | CH row → customer units | **PASS** |
| `TestLicenseFilter_graceAllowsIngest` | Grace → ingest continues | **PASS** |
| `TestChaos_LicenseGraceIngestContinues` | Chaos R10 grace proof | **PASS** |
| `TestChaos_LicenseServerUnreachableUsesLastKnownGood` | Chaos R10 last-known-good JWT | **PASS** |
| `BenchmarkFilterLicense` | 0 allocs/op hot path | **PASS** (0 allocs) |

### Build

```bash
go build ./cmd/management/... ./cmd/tracker/... ./cmd/processor/... ./cmd/ivt-detector/... ./cmd/edge-bpf-sync/...
# success
```

---

## 7. Architecture

```text
[Vendor license-server]
        │ JWT (volume_band, module flags, limits)
        ▼
[management LicenseWatcher] ──► billing.license_status + Redis entitlement:deployment
        │
        ├── VolumeMeterWorker ──► CH audit_log_rollups ──► billing.usage_meters
        ├── UDPControlServer ──► tracker ingress RPS epochs
        └── registry SyncEntitlements ◄── campaigns:update pub/sub

[tracker]
  LicenseFilter (atomic snapshot, 0 allocs)
  EntitlementsFilter (RPD ingress:day:*, OpenRTB gate)
```

---

## 8. Documentation

- [LICENSING.md](./LICENSING.md) §14 — PU packaging runtime contract
- [PROPOSALS.md](./PROPOSALS.md) §2 — marked shipped (M3-T)
- [MILESTONE.md](./MILESTONE.md) M3-T DoD checklist — complete
- [scripts/chaos-drills/m3/README.md](../scripts/chaos-drills/m3/README.md) — new chaos proofs

---

## 9. Out of Scope / Follow-ups

- Live ClickHouse integration test for `VolumeMeterWorker.RunHour` (requires Docker CH fixture).
- Invoice line item explicitly labeled "PU overage" (current billing uses `events` meter vs `max_events_per_month`).
- `cmd/fraud-scorer` standalone module gate (shares `ml_fraud_boost` with processor; deferred).
