# M1 — Slot Migration and Redis Key Catalog — Technical Report

**Date:** 2026-07-23  
**Scope:** M1-01..M1-07, M1-09 (M1-08 dual-write deferred to phase 2)  
**Artifacts:** `internal/ingestion/redis_key_catalog.go`, `redis_migrate.go`, `slot_migration_rewarm.go`, `internal/management/service_slot_migration.go`, `slot_migration_orchestrator.go`, `internal/metrics/collectors.go`

---

## 1. Summary

M1 closes the slot-migration consistency gaps: a unified `CampaignRedisKeyCatalog` drives COPY/DRAIN with hash-tagged keys, PG re-warm is the authoritative cutover for budget counters, activation is gated by `EXISTS` after re-warm, production defaults enable migration fences, and chaos tests LUA-10 / SO-02 validate fence + invariant behavior under testcontainers.

| ID | Status | Notes |
| :--- | :--- | :--- |
| M1-01 | Done | `CampaignRedisKeyCatalog` — fixed keys, prefix patterns, activation-required keys |
| M1-02 | Done | Migrator uses `{uuid}budget:campaign:{uuid}` and hash-tagged dedup/idempotency/rl/imp_ts/quota/fcap |
| M1-03 | Done | `VerifyRequiredKeysExist` blocks activation (`ErrSlotMigrationKeysMissing`); post-COPY verify when source had key |
| M1-04 | Done | `RewarmCampaignBudgetKeys` at activation; drain on old shard; R5 verified in chaos |
| M1-05 | Done | `MIGRATION_FENCE_ENABLED` default `true` when `ENV=production`; `BumpFencesForPendingMigrations` on orchestrator start |
| M1-06 | Done | PG re-warm authoritative for budget; COPY for ephemeral keys — documented in `DEVELOPMENT.md` |
| M1-07 | Done | Rollback playbook in `DEVELOPMENT.md` |
| M1-08 | Deferred | Dual-write / lag catch-up — metrics registered, implementation phase 2 |
| M1-09 | Done | `ad_slot_migration_lag_messages`, `ad_slot_migration_dual_write_total`, `ad_slot_migration_cutover_blocked_total` |

---

## 2. Architecture

### Cutover flow

```text
EnsureSlotMigrationJobs
  → BumpMigrationFences (source, when MIGRATION_FENCE_ENABLED)
  → COPY (CampaignKeyMigrator + catalog, DUMP/RESTORE)
  → post-COPY EXISTS (if source had budget key)
  → state = copied

ActivateSlotMapVersionWithMigration
  → RewarmCampaignBudgetKeys (target, from Postgres)
  → EXISTS gate (activation-required keys)
  → TX: set DRAINING, bump active_version
  → afterSlotMapActivated (StaticSlotSharder reload + broker publish)

DrainMigratingSlots
  → DrainCampaignKeys on source shard
  → state = done
```

### Key catalog

| Category | Examples | Hash tag |
| :--- | :--- | :--- |
| Fixed (COPY/DRAIN) | `budget:campaign`, `budget:quota`, `budget:sync:campaign`, `blacklist:placement` | Yes |
| Fixed (cold-path sync) | `budget:inflight:campaign`, `budget:lock:campaign`, `campaign:settings` | No |
| Source-only (DRAIN) | `budget:migration_fence` | No |
| Prefix SCAN | `dup:`, `idempotency:click:`, `rl:ip:`, `imp_ts:`, `fcap:c:`, `budget:daily_spent:` | Yes |

---

## 3. Metrics

| Metric | Type | Labels | When incremented |
| :--- | :--- | :--- | :--- |
| `ad_slot_migration_lag_messages` | Gauge | `slot`, `version` | Reserved for M1-08 dual-write |
| `ad_slot_migration_dual_write_total` | Counter | `slot`, `result` | Reserved for M1-08 dual-write |
| `ad_slot_migration_cutover_blocked_total` | Counter | `reason` | Activation blocked (`missing_keys`) |

**Alert (M1-09):** `ad_slot_migration_lag_messages > 0` for 30 s during active migration (Grafana rule to wire on deploy).

---

## 4. Test results

**Harness:** testcontainers-go (`postgres:16-alpine`, `redis:7-alpine`), Linux 6.17, Go 1.25.12

### Unit / integration (`internal/ingestion`)

```bash
go test ./internal/ingestion/... -run 'Migrate|Slot|RedisKey|Rewarm|Fence' -count=1
ok   espx/internal/ingestion   22.199s
```

| Test | Result |
| :--- | :--- |
| `TestCampaignRedisKeyCatalog_HashTaggedKeys` | PASS |
| `TestCampaignRedisKeyCatalog_SourceOnlyFence` | PASS |
| `TestCampaignKeyMigrator_MigrateAndDrain` | PASS (hash-tagged budget + dup prefix) |
| `TestRewarmCampaignBudgetKeys_FromPostgres` | PASS (PG 1M − 250K spend → 750K remaining) |
| `TestBumpMigrationFences_setsRedisAndPG` | PASS |
| `TestChaos_MigrationFenceConcurrentDebit` | PASS (32/32 fenced) |

### Management chaos (`internal/management`)

```bash
go test ./internal/management/... -run 'SlotMigration|LUA10|SO02|MigrationFence' -count=1
ok   espx/internal/management   34.052s
```

| Test | Scenario | Result |
| :--- | :--- | :--- |
| `TestSlotMigration_CopyAndActivate` | End-to-end COPY → activate → drain | PASS |
| `TestChaos_LUA10_DebitFencedDuringSlotCopy` | Debit during fence returns code 11 | PASS |
| `TestChaos_SO02_SlotMigrationPGRewarmCutover` | PG re-warm cutover + R5 invariant | PASS |
| `TestChaos_SlotMigrationPGRewarmColdStart` | No source Redis; PG re-warm at activation | PASS |
| `TestChaos_SlotMigrationCopyRedisPartition` | DUMP failure → failed state | PASS |
| `TestChaos_SlotMigrationRollbackAfterActivate` | Operator rollback | PASS |
| `TestChaos_SlotMigrationCutoverInvariant` | R5 after copy | PASS |

### Chaos proof lines (CI grep)

```
chaos_proof fault=lua10_migration_fence_copy fenced=true budget_unchanged=true code=11 subsystem=ads_lua fault=debit_during_copy
chaos_proof fault=so02_slot_migration_pg_rewarm subsystem=slot_migration fault=hot_slot_cutover r5_ok=true pg_rewarm=true campaign_id=...
```

---

## 5. Configuration

| Setting | Development default | Production default |
| :--- | :--- | :--- |
| `MIGRATION_FENCE_ENABLED` | `false` | `true` (via `ENV=production`) |
| `SLOT_MIGRATION_ENABLED` | `true` | `true` |

Staging k8s configmaps explicitly set `MIGRATION_FENCE_ENABLED: "true"`.

---

## 6. Verification commands

```bash
go test ./internal/ingestion/... -run 'Migrate|Slot|RedisKey' -short
go test ./internal/management/... -run SlotMigration -short
./scripts/chaos-drills/test_chaos.sh   # includes LUA-10, SO-02 chaos_proof lines
```

---

## 7. Deferred (M1-08)

Zero-downtime dual-write cutover with `replication_lag_messages < ε` gate and `SwapSnapshot` — metrics registered; implementation tracked as optional phase 2 in `MILESTONE.md`.

---

## 8. Files changed

| File | Change |
| :--- | :--- |
| `internal/ingestion/redis_key_catalog.go` | New — unified key catalog |
| `internal/ingestion/redis_migrate.go` | Catalog-driven COPY/DRAIN with hash tags |
| `internal/ingestion/slot_migration_rewarm.go` | PG re-warm for cutover |
| `internal/ingestion/budget_invariant_assert.go` | Hash-tagged key reads |
| `internal/management/service_slot_migration.go` | EXISTS gate, PG re-warm, fence bump |
| `internal/management/slot_migration_orchestrator.go` | Fence bump on start |
| `internal/metrics/collectors.go` | M1 migration metrics |
| `internal/config/env.go` | Production fence default |
| `docs/DEVELOPMENT.md` | Delta policy + rollback playbook |
| `docs/REDIS.md` | Catalog reference |
