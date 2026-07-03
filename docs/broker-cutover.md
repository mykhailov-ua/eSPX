# Broker primary ingest cutover runbook

Broker carries full `AdStreamEvent` records (IP, UA, payload, fraud, user_id) from the tracker mmap audit log via log-shipper. Redis Stream remains the settlement source until cutover is validated.

## Preconditions

- `docker-compose` stack includes `broker-redis`, `broker`, `log-shipper`, processor with `BROKER_*` env.
- `BROKER_SHADOW_MODE=1` (default): broker consumers parse and audit, but do **not** write PG/CH.
- Grafana/Prometheus scrape broker `:8081/metrics` and processor metrics.

## Phase 1 — Shadow validation (7–14 days)

1. Deploy with `BROKER_SHADOW_MODE=1`.
2. Watch metrics:
   - `ad_broker_ingest_divergence_messages` — sum Redis `XLEN` (all shards) vs broker committed offsets (all partitions). Target: stable, below `BROKER_DIVERGENCE_THRESHOLD`.
   - `ad_broker_consumer_lag_messages` — broker HWM minus committed per partition. Target: bounded under load.
   - `ad_broker_shadow_messages_total` — non-zero confirms broker path is live.
3. Spot-check audit log payloads decode as `AdStreamEvent` (not legacy `AdLogRecord`).
4. Compare PG/CH row counts from Redis path only; broker shadow must not affect billing.

## Phase 2 — Dual-write (CH first)

1. Set `BROKER_SHADOW_MODE=0` on **ClickHouse broker consumer only** (temporary env split requires two processor replicas or code flag — recommended: cut over CH group first in staging).
2. Run recon: CH event counts from broker vs Redis for 48h. Ledger (PG) still Redis-only.
3. Roll back to shadow if divergence or duplicate-key spikes appear.

## Phase 3 — PG cutover

1. `BROKER_SHADOW_MODE=0` for Postgres broker consumer.
2. Monitor ledger reconciliation and campaign stats vs management recon jobs.
3. Keep Redis stream consumers running (dual-read) for one retention window.

## Phase 4 — Redis stream decommission

1. Confirm broker consumer lag ≈ 0 on all 6 partitions for both `_pg_broker` and `_ch_broker` groups.
2. Disable per-shard `StreamConsumer` in processor (config change / feature flag).
3. Remove Redis `XADD` from `unified-filter.lua` only after broker RPO/RTO sign-off.

## Go / no-go checklist

| Check | Pass criteria |
|-------|----------------|
| Divergence | `ad_broker_ingest_divergence_high == 0` for 7d |
| Consumer lag | p99 lag < 10k messages per partition |
| Failover | HA drill: kill broker, leader re-elects < 30s, no committed offset regression |
| Payload | Sample events have `user_id`, `ip`, `payload` populated in CH |
| Billing | PG ledger totals match Redis baseline within 0.01% |

## Rollback

1. Set `BROKER_SHADOW_MODE=1` immediately.
2. Re-enable Redis `StreamConsumer` if disabled.
3. Broker offsets are preserved; no need to truncate broker log on rollback.

## Partition routing

- Partitions: `BROKER_PARTITION_COUNT=6` (matches Redis shard count).
- Routing: `crc32(campaign_id) & 1023 % N` — same slot function as `StaticSlotSharder`.
- log-shipper routes produce by campaign_id from `AdStreamEvent`.

## HA production notes

- Default compose runs single broker node. For HA use `deploy/broker-ha/` (2 nodes + HAProxy + Sentinel).
- Coord Redis (`6490`) must stay separate from ad shards (`6479–6482`).
