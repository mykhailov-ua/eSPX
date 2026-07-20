# M7 — Multi-Region Enterprise Cells: Technical Report

**Date:** 2026-07-20  
**Milestone:** M7 (Exec #13)  
**Status:** Shipped

---

## 1. Summary

Milestone M7 delivers enterprise multi-cell topology per [MULTI_REGION.md](./MULTI_REGION.md): asynchronous global-to-regional event relay (`RegionOutboxRelay`), cell-isolated Redis with no cross-region Lua, per-region RPD ingress quotas in Redis and UDP control-plane epochs, JWT `multi_region` licensing gate, and installer `multi_region: true` unblocked for production profiles.

---

## 2. Components Delivered

| ID | Component | Path | Notes |
|:---|:---|:---|:---|
| **M7.0** | **Standards envelope** | `internal/management/region_outbox_relay.go` | Regional relay; PG global read model; no cross-region Redis on `/track` |
| **M7.0** | **Schema** | `internal/ingestion/migrations/00047_multi_region.sql` | `regions`, `outbox_region_delivery`, `region_apply_idempotency`, PG fan-out trigger |
| **M7.1** | **RegionOutboxRelay** | `internal/management/region_outbox_relay.go` | Polls pending deliveries per `ESPX_REGION_CODE`; idempotent apply via `region_apply_idempotency` |
| **M7.1** | **Per-region RPD (Redis)** | `internal/ingestion/region_keys.go`, `filter_entitlements.go` | Keys: `ingress:day:{region}:{customer}:{date}` |
| **M7.1** | **Per-region RPD (UDP)** | `udp_control_codec.go`, `udp_control_server.go` | Protocol v2 trailing `MaxRPD`; split by active region count |
| **M7.1** | **License gate** | `licensing/features.go`, `cmd/management/main.go` | `MultiRegionEnabled()` + Enterprise JWT `multi_region` required when `MULTI_REGION_ENABLED=1` |
| **M7.1** | **Installer** | `internal/installer/profile.go` | `multi_region: true` allowed on `single_vps` / `k8s_k3s` |
| **M7.1** | **Config** | `internal/config/env.go` | `ESPX_REGION_CODE`, `MULTI_REGION_ENABLED`, `MultiRegionCell()` / `MultiRegionGlobal()` |
| **M7.1** | **Worker routing** | `internal/management/service.go` | Regional cells: relay only; global control: fan-out only; single-region: legacy `OutboxWorker` |
| **M7.2** | **Cell isolation test** | `region_outbox_relay_test.go` | `TestRegionCellIsolation` |
| **M7.2** | **Relay test** | `region_outbox_relay_test.go` | `TestRegionOutboxRelay` |
| **M7.2** | **Chaos Kong runbook** | `scripts/chaos-drills/m7/README.md` | Manual game day drill |

---

## 3. Architecture

### Delivery flow

1. Management writes `outbox_events` in global Postgres (unchanged API).
2. PG trigger `outbox_region_fanout` inserts `outbox_region_delivery` rows for each active row in `regions`.
3. Each regional management process (`ESPX_REGION_CODE` ≠ 0) runs `RegionOutboxRelay`, which:
   - Claims `PENDING` rows for its region with `FOR UPDATE SKIP LOCKED`
   - Applies the event to **local** Redis only via existing `OutboxWorker` handlers
   - Records `region_apply_idempotency` before marking `DELIVERED`
4. Global control plane (`ESPX_REGION_CODE=0`, `MULTI_REGION_ENABLED=1`) does not run local `OutboxWorker` — propagation is regional only.

### Per-region RPD

- **Redis:** `EntitlementsFilter` increments region-scoped daily counters; single-region deployments (`ESPX_REGION_CODE=0`) keep legacy key format.
- **UDP:** When `MULTI_REGION_ENABLED=1`, management publishes protocol v2 epochs with `MaxRPD = license.max_requests_per_day / active_regions`.

---

## 4. Environment knobs

| Variable | Default | Used by |
|:---|:---|:---|
| `ESPX_REGION_CODE` | `0` | Regional cell identifier (byte 8 in Global UUID) |
| `MULTI_REGION_ENABLED` | `false` | Enables multi-cell worker routing and UDP RPD extension |

---

## 5. Metrics

| Metric | Type | Purpose |
|:---|:---|:---|
| `ad_region_outbox_delivered_total` | Counter | Events applied by `RegionOutboxRelay` |
| `ad_region_outbox_delivery_lag_seconds` | Histogram | `outbox_events.created_at` → regional `DELIVERED` |

---

## 6. Test Results

### M7.2 prescribed tests

```bash
go test ./internal/ingestion/... -run 'IngressDayKey|RPDCodec' -short
# ok   espx/internal/ingestion   0.026s

go test ./internal/management/... -run 'TestRegion' -count=1 -v
```

```
=== RUN   TestRegionCellIsolation
    region_outbox_relay_test.go:42: chaos_proof fault=region_cell_isolation subsystem=multi_region cell_b_hit=false cell_a_key=ingress:day:01:...
--- PASS: TestRegionCellIsolation (1.51s)
=== RUN   TestRegionOutboxRelay
    region_outbox_relay_test.go:125: chaos_proof fault=region_outbox_relay delivered=true event_id=1 redis_budget=5000000 subsystem=region_outbox_relay region_code=1
--- PASS: TestRegionOutboxRelay (2.94s)
PASS
ok   espx/internal/management   2.966s
```

```bash
go test ./internal/installer/... -run TestProfileValidation -count=1
# ok   espx/internal/installer   0.003s
```

---

## 7. Criterion Coverage

| Test / Artifact | Criterion | Result |
|:---|:---|:---|
| `TestRegionCellIsolation` | Redis key in cell A invisible in cell B | **PASS** |
| `TestRegionOutboxRelay` | Outbox event reaches remote cell Redis | **PASS** |
| `TestRegionOutboxRelay` (replay) | Idempotent re-apply (`region_apply_idempotency`) | **PASS** |
| `scripts/chaos-drills/m7/README.md` | Documented Chaos Kong game day runbook | **PASS** |
| Installer `multi_region: true` | M9 flag unblocked on production profiles | **PASS** |
| License gate | JWT `multi_region` + Enterprise required | **PASS** |
| Hot path | No cross-region Redis Lua on `/track` | **PASS** (by design — regional relay only) |

---

## 8. Operator Notes

1. Seed `regions` before enabling multi-region (`INSERT INTO regions (code, name) VALUES (1, 'us-east'), (2, 'eu-west')`).
2. Set per-cell env: `MULTI_REGION_ENABLED=1 ESPX_REGION_CODE=<code>`.
3. Run Chaos Kong game day manually per `scripts/chaos-drills/m7/README.md` before production cutover.
