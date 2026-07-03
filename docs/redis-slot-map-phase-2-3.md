# Fixed Slot Map — Shard Migration (Phase 2.3)

Cross-shard data copy, cutover, and drain for Postgres-controlled slot map resize (`REDIS.md` Phase 2.3).

## Architecture

```
Draft map (MIGRATING slots)          Active map (still live)
        │                                      │
        └─► SlotMigrationOrchestrator          └─► trackers read active_version
               ├─ list campaigns by slot = crc32(id) & 1023
               ├─ COPY keys source_shard → target_shard (idempotent DUMP/RESTORE)
               └─ after activate: DRAIN old keys on source_shard
```

**Spend safety (2.3.3):** trackers and edge keep routing via **active** map until `activate`. New spend hits the **old** shard during copy; target shard receives a snapshot copy. After cutover, routing switches atomically (Phase 2.2 reload). No dual-write on the hot path.

---

## 2.3.1 — Orchestrator (batch by slot)

| Component | Detail |
| :--- | :--- |
| Worker | `SlotMigrationOrchestrator` in management (`SLOT_MIGRATION_ENABLED`, default 30s tick) |
| Trigger | Highest draft version `> active_version` with `MIGRATING` slots |
| Campaign list | `ListCampaignIDs` from Postgres → `FilterCampaignIDsBySlot(ids, slot)` |
| Progress | `redis_slot_migration` table (migration `00029`) |

Slot index: `CampaignSlotIndex(id) = crc32Castagnoli(id) & 1023` — matches `StaticSlotSharder.GetShard` slot lookup.

---

## 2.3.2 — Key copy (idempotent)

Go cold path: `CampaignKeyMigrator` (`internal/ads/redis_migrate.go`).

| Key pattern | Notes |
| :--- | :--- |
| `budget:campaign:{id}` | Legacy budget |
| `budget:quota:{id}` | Phase 1 quota |
| `budget:refill_lock:{id}` | Quota refill lock |
| `budget:sync/inflight/lock/txid:campaign:{id}` | Sync worker |
| `campaign:settings:{id}` | Settings hash |
| `budget:daily_spent:campaign:{id}:*` | SCAN |
| `fcap:c:{id}:u:*` | SCAN |

Shell: `scripts/redis-migrate-campaign.sh` extended with the same keys (manual / recovery).

Copy uses `DUMP` + `RESTORE ... REPLACE` — safe to re-run.

---

## 2.3.3 — Read-from-old until cutover

| Phase | Routing | Spend destination |
| :--- | :--- | :--- |
| Copy in progress | `active_version` (old map) | Old shard |
| After `activate` | New `active_version` | Target shard |
| Drain | New map (`DRAINING` metadata) | Target shard; old keys deleted |

**Ops:** prefer low-traffic window; optional pause heavy campaigns in migrating slots before copy.

---

## 2.3.4 — Cutover and drain

1. Orchestrator finishes copy → migration state `copied`
2. `POST /admin/shards/slot-map/versions/{v}/activate` (or CLI `slot-map activate`):
   - Validates all `MIGRATING` slots are `copied`
   - Sets slots to `DRAINING` in map (routing `shard_id` = target)
   - Updates `active_version`, audit log, broker `shards:reload`
3. Orchestrator drain tick: delete campaign keys on **source_shard** → state `done`, slot → `ACTIVE`

Verify: `scripts/redis-reconcile-post-deploy.sh` PASS after drain.

---

## 2.3.5 — Rollback

`POST /admin/shards/slot-map/rollback` body: `{"previous_version": N}`

- Sets `active_version` back to N (must be `<` current)
- Broadcasts `shards:reload`
- Audit: `SLOT_MAP_ROLLBACK`

**Staging test:** create v2 with MIGRATING → copy → activate → rollback to v1 → confirm trackers reload.

---

## Admin API

| Method | Path | Permission |
| :--- | :--- | :--- |
| GET | `/admin/shards/slot-map/versions/{v}/migrations` | `shards:read` |
| POST | `/admin/shards/slot-map/versions/{v}/copy` | `shards:write` |
| POST | `/admin/shards/slot-map/versions/{v}/activate` | `shards:write` |
| POST | `/admin/shards/slot-map/rollback` | `shards:write` |

CLI: `admin slot-map copy|migrations|activate|rollback`.

---

## Migration state machine

```
pending → copying → copied → (activate) → draining → done
                └→ failed
```

---

## Files

| File | Role |
| :--- | :--- |
| `internal/ads/slot_index.go` | `CampaignSlotIndex`, `FilterCampaignIDsBySlot` |
| `internal/ads/redis_migrate.go` | DUMP/RESTORE copy + drain |
| `internal/ads/slot_migration_repo.go` | PG progress |
| `internal/management/slot_migration_orchestrator.go` | Background worker |
| `internal/management/service_slot_migration.go` | Copy / activate / drain / rollback |
| `internal/ads/migrations/00029_redis_slot_migration.sql` | Progress schema |
| `scripts/redis-migrate-campaign.sh` | Manual idempotent copy |

---

## Tests

```bash
go test ./internal/ads/... -run TestCampaignKeyMigrator -count=1
go test ./internal/management/... -run TestSlotMigration -count=1
```

`TestSlotMigration_CopyAndActivate`: copy → activate → drain end-to-end with testcontainers.
