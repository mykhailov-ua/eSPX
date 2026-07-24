# M14 Technical Report — Shard-0 Survival (GAP-SHARD-04)

**Date:** 2026-07-24  
**Scope:** M14-01 .. M14-05 (shard-0 survival — now documented in [CAPABILITIES.md](./CAPABILITIES.md#m14--shard-0-survival-ingress-hardening-quanta-lifecycle))  
**Status:** complete · closes [GAP-SHARD-04](./BACKLOG.md)

---

## Summary

Shard 0 was a control-plane SPOF: `campaigns:update` pub/sub, default outbox notify target, and (historically) preferred read for global keys. Campaign debit on shards 1–3 already survived shard-0 loss (`TestChaos_Shard0Outage`); this work makes failure modes explicit, extends global key fan-out/read locality, adds registry stale-serve, optional broker fallback, and fail-fast / triplet reroute for shard-0-homed ingest.

Local-quanta budget-boundary overspend was **not** in scope (already atomic via `local-quota-refill.lua`).

---

## Deliverables

### M14-01 — Global key fan-out

| Change | Detail |
| :--- | :--- |
| `internal/management/redis_global.go` | Helpers: `syncGlobalStringToAllShards`, `deleteGlobalKeyFromAllShards`, `syncGlobalSetMemberToAllShards`, `syncGlobalHashFieldToAllShards` (plus existing config fan-out) |
| Outbox | Blacklist / ML boost / placement pause use helpers (all masters) |
| Tracker reads | `SettingsWatcher` prefers shards 1..N for `ml:score:boost:*`; `FraudBlacklistFilter` / `PlacementBlacklistFilter` use `pickLocalGlobalShard` |
| Docs | Placement key name clarified as `{uuid}blacklist:placement:{uuid}` (not `placement:blocklist:*`) |

### M14-02 — Registry stale-serve

| Change | Detail |
| :--- | :--- |
| `registry_stale.go` | `ConfigureStaleMode`, `MarkPubSubOK`, `IsStaleMode`; gauge `ad_registry_stale_mode` + `ad_shard0_pubsub_unreachable` |
| `StartWatch` | Reconnect loop + periodic `PING`; quiet > `REGISTRY_STALE_TTL` (default 30 s) → stale |
| Reject path | Unknown campaign → `503 registry_stale` (geo/schedule/unified); known-in-RAM campaigns keep serving |
| Env | `REGISTRY_STALE_TTL` (seconds) |

### M14-03 — Campaign-update fallback

| Change | Detail |
| :--- | :--- |
| Opt-in | `CAMPAIGN_UPDATE_BROKER_FALLBACK=true` |
| Management | `publishCampaignUpdate` also `Produce`s campaign ID on broker topic (default `campaigns:update`) when Redis pub/sub fails or alongside it |
| Tracker | `CampaignUpdateWatcher` consumes broker → `UpdateAndWarmCampaign` + `MarkPubSubOK` |
| HTTP long-poll | Not shipped; broker path is the implemented secondary |

### M14-04 — Shard-0 ingest reroute

| Change | Detail |
| :--- | :--- |
| `ConnectRedisShards` | Returns `[]*RedisBreaker` with clients |
| `shard_resolve.go` | `resolveDebitShard`: if chosen shard breaker open → try triplet reserve/A/B; else `ErrShardUnavailable` |
| HTTP | `503` body `shard_unavailable` (never silent accept without debit) |
| Docs | Blast radius table in [DATA.md](./DATA.md) Part I §2 |

### M14-05 — Ops

| Change | Detail |
| :--- | :--- |
| Alert | `Shard0PubSubUnreachable` in `deploy/monitoring/prometheus.rules.yaml` |
| Runbook | [DEVELOPMENT.md](./DEVELOPMENT.md) §Shard-0 outage |
| Drill | `scripts/chaos-drills/m14_shard0_failure.sh` |
| Chaos | `TestChaos_Shard0Outage` extended: explicit 503 bodies, stale-serve, `AssertBudgetInvariant`, dual `chaos_proof` lines |

---

## Acceptance (verification matrix)

| Check | Result |
| :--- | :--- |
| Shards 1–3 accept during redis-0 stop | Covered by extended `TestChaos_Shard0Outage` |
| Shard-0 campaigns explicit 503 | `shard_unavailable` body asserted |
| Unknown ID in stale mode | `registry_stale` body asserted |
| Outbox PENDING → PROCESSED after recovery | Unchanged + still asserted |
| Budget invariant | `AssertBudgetInvariant` on shards 1–3 |
| Unit | `TestRegistryStaleMode_TTL`, `TestResolveDebitShard_*` |

```bash
go test ./internal/ingestion/ -run 'RegistryStale|ResolveDebitShard' -count=1
go test ./tests/chaos/ -run TestChaos_Shard0Outage -timeout 15m
bash scripts/chaos-drills/m14_shard0_failure.sh
```

---

## Residual risk

| Residual | Mitigation |
| :--- | :--- |
| Auth lockout / consent watch still prefer shard 0 | Out of M14-01..05; fail-open / separate backlog |
| Brand creatives pub/sub notify still shard-0 Publish | Data already fan-out; notify SPOF until Sentinel |
| Broker fallback opt-in | Default off; enable when broker HA is production-ready |
| Triplet reroute only when `HasTriplet` | Non-triplet shard-0 campaigns stay blocked until failover (by design) |

---

## Key files

| Path | Role |
| :--- | :--- |
| `internal/management/redis_global.go` | Fan-out helpers |
| `internal/ingestion/registry_stale.go` | Stale-serve |
| `internal/ingestion/shard_resolve.go` | Debit shard selection / reroute |
| `internal/ingestion/campaign_update_watcher.go` | Broker fallback consumer + produce helper |
| `internal/database/redis_shards.go` | Breaker export |
| `scripts/chaos-drills/m14_shard0_failure.sh` | Ops drill |
| `docs/M14_SHARD0_TECHNICAL_REPORT.md` | This report |
