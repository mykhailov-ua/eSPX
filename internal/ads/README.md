# Ads (`internal/ads`)

Hot-path tracker, filters, processor, and shared data layer. Subpackages:

| Package | Role |
|---------|------|
| `catalog/` | In-memory campaign registry, settings watcher, budget warmer |
| `clock/` | Cached wall clock and fast UUID generation |
| `filter/` | Filter engine, unified Redis Lua script (`unified.lua`) |
| `ingest/` | gnet HTTP handler, track parse, hybrid balancer |
| `processor/` | Redis stream consumer, Postgres/ClickHouse stores |
| `rtbbridge/` | RTB catalog sync and budget authority bridge |
| `sharding/` | Slot map, static/jump hash sharders |
| `repo/` | Campaign/customer repos, protobuf pool helpers |
| `sync/` | Budget sync worker, PG/CH reconciliation |
| `broker/` | mmap broker stream consumer |
| `db/` | sqlc-generated queries |
| `pb/` | track/event protobuf types |

Migrations and sqlc queries remain at `internal/ads/migrations/` and `internal/ads/queries/`.
