# Edge chaos tests in CI

OpenResty phase-1/phase-2 enforcement runs in `deploy/nginx/lua/`. A full `docker compose` nginx stack per PR is too heavy for the default chaos gate.

## CI harness

`internal/edge/perimeter` mirrors the production contract:

| Production (Lua) | CI (Go) |
| :--- | :--- |
| `edge-blacklist-sync.lua` SMEMBERS → `blacklist_cache` | `BlacklistCache.SyncFromRedis` |
| `access-check.lua` `phase1_blacklist` | `BlacklistCache.Phase1Check` |
| `espx_edge_blocked_ip_total` / `espx_edge_body_read_total` | `perimeter.Metrics` |

Chaos tests use **real Redis** (testcontainers) and assert:

1. Blacklisted IP → phase-1 `403` equivalent with `body_read_total == 0`.
2. `SADD blacklist:manual` propagates into the cache within the **5s** worker sync interval (`init-worker.lua` `BLACKLIST_SYNC_INTERVAL`).

## Known limitation

These tests do **not** start nginx/OpenResty. They validate blacklist sync + phase-1 ordering semantics that production Lua implements. Full stack validation remains `scripts/edge-phase-bench.sh` against a running edge node.

## Running

```bash
go test -count=1 -v -run TestChaos_Edge ./internal/edge/perimeter/...
make test-chaos   # includes perimeter package
```
