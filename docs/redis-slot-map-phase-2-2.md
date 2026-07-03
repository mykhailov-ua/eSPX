# Fixed Slot Map — Tracker Reload (Phase 2.2)

Hot-path reload of Postgres `active_version` into `StaticSlotSharder` without Redis Pub/Sub on shard 0 (`REDIS.md` Phase 2.2).

## Architecture

```
Postgres (active_version + 1024 rows)
        │
        ├─► Tracker startup: LoadActiveSlotMap → atomic.Value swap
        │
        ├─► SlotMapWatcher (cold path)
        │      ├─ poll /ops/shards/slot-map meta via PG (10s default)
        │      └─ broker topic shards:reload (per-tracker consumer group)
        │
        ├─► Management activate: PG commit → local reload → broker publish
        │
        └─► Nginx edge-slot-map.lua: poll GET /ops/shards/slot-map (10s)
```

**Hot path invariant:** `GetShard` = `atomic.Load` + index — **0 allocs/op, no mutex** (unchanged from Phase 1.2).

---

## 2.2.1 — Startup load (Tracker + Management)

On boot, both services call `ads.LoadActiveSlotMap`:

1. Read `redis_slot_map_meta.active_version`
2. Load 1024 rows for that version
3. `StaticSlotSharder.StoreSlotMap(table)` — atomic pointer swap
4. `SetActiveVersion(version)` for change detection

**Fallback:** if migration/schema missing → `ReloadFromModulo(len(REDIS_ADDRS))`, log warning.

Files: `cmd/tracker/main.go`, `cmd/management/main.go`, `internal/ads/slot_map_loader.go`.

---

## 2.2.2 — Reload signal (broker, not Redis Pub/Sub)

| Component | Mechanism |
| :--- | :--- |
| **Management** | After `ActivateSlotMapVersion` commit → `PublishSlotMapReload` to broker topic `shards:reload` |
| **Tracker** | `SlotMapWatcher.brokerLoop` — unique consumer group `slotmap-{hostname}`, `Fetch` + `CommitOffset` |
| **Safety net** | PG poll every `SLOT_MAP_POLL_INTERVAL_MS` (default 10s) compares `active_version` |

**Why not Redis Pub/Sub on shard 0:** avoids SPOF and hot-shard coordination (`REDIS.md` Phase 2.2).

Config: `SLOT_MAP_RELOAD_TOPIC`, `BROKER_URL`, `BROKER_REDIS_URL`.

---

## 2.2.3 — Atomic swap

`StoreSlotMap` copies `[1024]uint16` onto stack, stores pointer in `atomic.Value`. In-flight requests observe **entire old or new table** — no torn reads.

Verified: `TestStaticSlotSharder_StoreSlotMap_concurrent`, `TestStaticSlotSharder_ZeroAllocs`.

---

## 2.2.4 — Nginx edge sync

| Item | Detail |
| :--- | :--- |
| Module | `deploy/nginx/lua/edge-slot-map.lua` |
| Source | `GET {MANAGEMENT_URL}/ops/shards/slot-map` |
| Cache | `lua_shared_dict slot_map 4m` — keys `s:0..1023`, `version` |
| Routing | `crc32c(uuid_bytes) & 1023` → `dict["s:"..slot]` |
| Timer | `init-worker.lua` — worker 0, every `SLOT_MAP_SYNC_INTERVAL_SEC` (10s) |

Edge `get_shard(campaign_id)` matches Go `StaticSlotSharder.GetShard` for the same map version.

---

## Ops endpoint

`GET /ops/shards/slot-map` (unauthenticated, internal network only):

```json
{
  "version": 1,
  "active_version": 1,
  "slots": [0,1,2,3,0,1,...]
}
```

1024 integers — `shard_id` per slot index. Used by nginx and observability tooling.

---

## Performance

| Path | Cost | Notes |
| :--- | :--- | :--- |
| `GetShard` (hot) | **0 ns/op, 0 B/op, 0 allocs/op** | Unchanged after Phase 2.2 |
| `LoadActiveSlotMap` (cold) | ~2–8 ms | 1024-row PK scan + swap |
| Broker reload signal | ~1 Produce + 1 Fetch | Control plane only |
| Edge sync | ~1 HTTP GET / 10s | Worker 0 timer, not per-request |

---

## Config (.env)

```bash
SLOT_MAP_RELOAD_TOPIC=shards:reload
SLOT_MAP_POLL_INTERVAL_MS=10000
MANAGEMENT_URL=http://127.0.0.1:8188
SLOT_MAP_SYNC_INTERVAL_SEC=10
```

---

## Files

| Component | Path |
| :--- | :--- |
| Loader | `internal/ads/slot_map_loader.go` |
| Watcher | `internal/ads/slot_map_watcher.go` |
| Broker message | `internal/ads/slot_map_reload.go` |
| Ops route | `internal/management/ops.go` |
| Activate broadcast | `internal/management/service_slot_map.go` |
| Tracker wiring | `cmd/tracker/main.go` |
| Edge Lua | `deploy/nginx/lua/edge-slot-map.lua` |
| Tests | `internal/ads/slot_map_loader_test.go`, `sharding_test.go` |

---

## Cutover checklist

1. Deploy migration `00028` (Phase 2.1) if not applied
2. Deploy management + tracker with Phase 2.2 binaries
3. Verify `GET /ops/shards/slot-map` returns 1024 slots
4. Activate new map version via admin API
5. Confirm tracker logs `slot map reloaded source=broker|poll version=N`
6. Confirm nginx `edge slot map sync` without warnings in error log

Next: **Phase 2.3** — orchestrator for `MIGRATING` slots + data copy.
