# Redis: 4-shard topology

Production ad Redis uses **4 standalone masters** (`redis-0` … `redis-3`, host ports **6479–6482**). Routing is `StaticSlotSharder` in `internal/ads/sharding.go` (`slot = crc32(campaign_id) & 1023`, `shard = slot % N`).

`config.ExpectedRedisShardCount = 4`. `ENV=production` rejects `len(REDIS_ADDRS) != 4`.

## Why 4 instead of 6

- Fewer Redis processes, replicas, Sentinel monitors, and connection pools.
- Trade-off: changing N remaps **~67%** of campaign shard indices (`TestStaticSlotSharder_MigrateSixToFour`); shards 4–5 are removed entirely.

## Cold start (dev / new env)

1. Update `.env`:
   ```
   REDIS_ADDRS=127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481,127.0.0.1:6482
   BROKER_PARTITION_COUNT=4
   ```
2. `docker compose up -d` (only four shard services).
3. `bash scripts/verify-redis-topology.sh`
4. `bash scripts/redis-reconcile-post-deploy.sh` after management is up.

## Migrating from 6 shards (existing data)

**Requires downtime or campaign-by-campaign copy** — `slot % 6` ≠ `slot % 4` for ~67% of campaigns; all data on former shards 4–5 must move.

1. Stop trackers and processor (pause writes).
2. For each campaign UUID:
   ```bash
   bash scripts/redis-migrate-campaign.sh <campaign_uuid>
   ```
   Script uses `REDIS_SHARD_COUNT=4` and copies shard-local keys.
3. Deploy stack with 4 shards only; remove `redis-4` / `redis-5` volumes when confident.
4. Run `scripts/redis-reconcile-post-deploy.sh` to replicate global keys (blacklist, config) to all 4 shards.

**Dev shortcut:** wipe shard volumes and re-seed from Postgres (`management` budget warmer / campaign sync).

## Shard CLI helper

```bash
go run ./scripts/campaign_shard.go <campaign_uuid> 4
```

## Observability (Phase 0 — hot-shard)

Tracker exposes per-shard Redis load and sampled per-campaign breakdown for ops triage **before** considering shard resize or Distributed Quotas (Phase 1).

### Metrics

| Metric | Labels | Purpose |
| :--- | :--- | :--- |
| `ad_redis_ops_total` | `shard` | Unified-filter EvalSha round trips per shard (every `/track` filter, including budget-miss retries) |
| `ad_redis_campaign_ops_sampled_total` | `shard`, `campaign_id` | Downsampled per-campaign Redis ops for top-N dashboards |
| `ad_tracker_campaign_spend_micro_sampled_total` | `shard`, `campaign_id` | Downsampled accepted spend (micro-units) per campaign |

Sampling uses `METRICS_HISTOGRAM_SAMPLE_MASK` (default `127` → ~1/128 requests). Set `-1` to sample every request in dev only — high cardinality in production.

Existing shard health metrics: `ad_redis_lua_duration_seconds{shard}`, `ad_redis_breaker_state{shard}`, `ad_tracker_redis_shard_healthy{shard}`.

### Grafana queries

Per-shard Redis RPS:

```promql
sum by (shard) (rate(ad_redis_ops_total[5m]))
```

Top campaigns on a shard (sampled ops):

```promql
topk(10, sum by (campaign_id) (rate(ad_redis_campaign_ops_sampled_total{shard="0"}[5m])))
```

Top campaigns by spend rate (micro-units/s, sampled):

```promql
topk(10, sum by (campaign_id) (rate(ad_tracker_campaign_spend_micro_sampled_total{shard="0"}[5m])))
```

Shard skew alert (also in `prometheus-rules.yml` as `RedisShardOpsSkew`):

```promql
max(sum by (shard) (rate(ad_redis_ops_total[5m])))
/ avg(sum by (shard) (rate(ad_redis_ops_total[5m])))
```

Threshold: **>3×** for 5 minutes → investigate hot campaign before resharding.

## Hot campaign without resharding

When one shard shows elevated `ad_redis_ops_total` or a single `campaign_id` dominates sampled counters:

1. **Confirm skew** — compare `sum by (shard) (rate(ad_redis_ops_total[5m]))` across shards 0–3. Skew alone does not require adding shards.
2. **Identify campaign** — `topk(10, …)` on `ad_redis_campaign_ops_sampled_total` and `ad_tracker_campaign_spend_micro_sampled_total` for the hot shard.
3. **Check Redis shard health** — `ad_redis_lua_duration_seconds` p99 per shard, Redis CPU/memory on that master, `ad_redis_breaker_state`.
4. **Scale ingestion, not shards** — add tracker replicas behind Nginx; edge rate limit / circuit breaker already shed load before Redis. Hot campaign traffic stays on one shard by design (`StaticSlotSharder`).
5. **Campaign-level knobs** — lower bid/pacing, pause campaign via management, emergency breaker (`config:values`), or edge block for fraud-tier traffic.
6. **When to resize** — only if **all** shards are near capacity (CPU >80%, Lua p99 >15–20 ms, >50–70k ops/shard sustained). That is Phase 2 (Fixed Slot Map), not `slot % N` on the fly. See `REDIS.md` ROADMAP.

**Do not** change `N` or `REDIS_ADDRS` count to fix a single hot campaign — ~67% of keys remap and require migration.

## Related

- `internal/config/env.go` — `ExpectedRedisShardCount`
- `docker-compose.yml` — `REDIS_SHARD_COUNT=4`, sentinel entrypoint
- `.env.example` — `REDIS_ADDRS` template
- `docs/redis-quota-lifecycle.md` — Distributed Quotas request lifecycle (Phase 1)
