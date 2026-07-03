# Fixed Slot Map — Control Plane (Phase 2.1)

Postgres control plane for dynamic Redis resize without changing `slot % N` on the fly (`REDIS.md` Phase 2.1). Trackers load `active_version` at startup (Phase 2.2); this document covers schema, Admin API, ACID guarantees, RBAC, and query plans.

## Architecture

```
Admin / CLI ──► Management HTTP API ──► Postgres
                    │                    ├── redis_slot_map (1024 rows × version)
                    │                    └── redis_slot_map_meta (active_version singleton)
                    └── admin_audit_log (RBAC actions)
```

**Routing invariant (unchanged):** `slot = crc32(campaign_id) & 1023`; `shard_id` comes from the slot map row for that slot index.

---

## 1. Schema (2.1.1, 2.1.2)

Migration: `internal/ads/migrations/00028_redis_slot_map.sql`

| Column | Type | Notes |
| :--- | :--- | :--- |
| `version` | INT | Monotonic map version; clone creates `max(version)+1` |
| `slot` | SMALLINT | `0..1023`, unique per version |
| `shard_id` | SMALLINT | Target Redis shard index |
| `state` | ENUM | `ACTIVE`, `MIGRATING`, `DRAINING` |

**Slot lifecycle:**

| State | Meaning |
| :--- | :--- |
| `ACTIVE` | Steady-state; trackers route to `shard_id` |
| `MIGRATING` | Slot scheduled for cross-shard copy (Phase 2.3 orchestrator) |
| `DRAINING` | Old shard draining after cutover |

Seed: **version 1** with `slot % 4` (default `ExpectedRedisShardCount`).

---

## 2. Admin API (2.1.3)

| Method | Path | Permission | Action |
| :--- | :--- | :--- | :--- |
| GET | `/admin/shards/slot-map?version=&include_slots=` | `shards:read` | Summary or full map |
| POST | `/admin/shards/slot-map/versions` | `shards:write` | Clone → new version + overrides |
| POST | `/admin/shards/slot-map/versions/{v}/migrate` | `shards:write` | Mark slots `MIGRATING` |
| POST | `/admin/shards/slot-map/versions/{v}/activate` | `shards:write` | Set `active_version` |

All mutating endpoints write `admin_audit_log` in the **same transaction** as the map change.

### RBAC

| Role | `shards:read` | `shards:write` |
| :--- | :---: | :---: |
| Admin (`A`) | ✓ | ✓ |
| Manager (`M`) | ✗ | ✗ |
| User (`U`) | ✗ | ✗ |

Permissions: `internal/management/permissions.go`, `rbac.go`.

---

## 3. ACID and transaction isolation

All control-plane mutations use **explicit `BEGIN … COMMIT`** with row-level locks:

| Operation | Lock order | Isolation effect |
| :--- | :--- | :--- |
| `CreateNextVersion` | `redis_slot_map_meta FOR UPDATE` → bulk copy → per-slot updates | Serializes concurrent version creation; only one draft at a time per meta lock |
| `MarkSlotsMigrating` | Per-slot `redis_slot_map FOR UPDATE` | Prevents lost updates on overlapping slot batches |
| `ActivateVersion` | `redis_slot_map_meta FOR UPDATE` → count validation → meta update | Atomic pointer swap; incomplete maps rejected |

**Financial / routing safety:**

- `active_version` is **not** switched on clone — operators must explicitly `activate`.
- Activation requires **exactly 1024 rows** (`ErrSlotMapIncomplete` otherwise).
- Failed HTTP handler → transaction rollback → no partial map + audit divergence.

Default Postgres isolation: **READ COMMITTED**; correctness relies on `FOR UPDATE` + single-transaction commit (same pattern as `QuotaRepo.ReserveChunk`).

---

## 4. EXPLAIN ANALYZE (reference plans, PG 16)

Run via CLI (rolls back copy probe):

```bash
go run ./cmd/admin slot-map explain --env-path .env
```

Or manually:

```sql
BEGIN;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT active_version FROM redis_slot_map_meta WHERE id = 1 FOR UPDATE;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT COUNT(*) FROM redis_slot_map WHERE version = 1;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT version, slot, shard_id, state
FROM redis_slot_map WHERE version = 1 AND slot = 42 FOR UPDATE;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT version, slot, shard_id, state
FROM redis_slot_map WHERE version = 1 AND state = 'MIGRATING' ORDER BY slot;

EXPLAIN (ANALYZE, BUFFERS, WAL)
INSERT INTO redis_slot_map (version, slot, shard_id, state)
SELECT 999, slot, shard_id, state FROM redis_slot_map WHERE version = 1;

ROLLBACK;
```

### Expected plan shape

| Statement | Access method | Notes |
| :--- | :--- | :--- |
| `meta FOR UPDATE` | Index Scan on `redis_slot_map_meta_pkey` | Single-row singleton; ~1 buffer |
| `COUNT(version)` | Index Only Scan or Aggregate on PK `(version, slot)` | 1024 rows; ~4–8 buffers warm |
| `slot FOR UPDATE` | Index Scan on PK `(version, slot)` | Point lookup; row lock |
| `state = MIGRATING` | Index Scan on `idx_redis_slot_map_version_state` | Filter `(version, state)` |
| `CopySlotMapVersion` | Insert + Index Scan on source version | ~1024 heap inserts + WAL; cold path only |

Typical meta lock + single slot update: **< 1 ms** warm. Full version clone (1024 rows): **~2–8 ms** SSD depending on `synchronous_commit`.

**No sequential scans** at steady state: PK `(version, slot)`, partial index on `(version, state)`.

---

## 5. CLI (operator path)

```bash
# Show active map summary
go run ./cmd/admin slot-map show

# Create version 2 with two migrating slots
go run ./cmd/admin slot-map create-version --override 0:2:MIGRATING --override 1:2:MIGRATING

# Mark batch migrating on draft version
go run ./cmd/admin slot-map mark-migrating --version 2 --slots 10,11,12 --target-shard 3

# Cutover pointer (after Phase 2.3 data copy)
go run ./cmd/admin slot-map activate --version 2
```

---

## 6. Files

| Component | Path |
| :--- | :--- |
| Migration | `internal/ads/migrations/00028_redis_slot_map.sql` |
| SQLc queries | `internal/ads/queries/slot_map.sql` |
| Repository | `internal/ads/slot_map_repo.go` |
| Service + audit | `internal/management/service_slot_map.go` |
| HTTP handlers | `internal/management/handler_slot_map.go` |
| CLI | `cmd/admin/cmd/slot_map.go` |
| Tests | `internal/ads/slot_map_repo_test.go`, `internal/management/slot_map_test.go` |

---

## 7. Next steps (Phase 2.2+)

- Tracker startup: load `active_version` → `StaticSlotSharder.StoreSlotMap`
- gRPC/broker `shards:reload` broadcast
- Orchestrator for `MIGRATING` slots (Phase 2.3)
