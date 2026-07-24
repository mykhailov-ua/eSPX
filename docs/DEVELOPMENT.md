# Development

Local environment, CI gates, and operational runbooks. Hot-path rules: [GO.md](./GO.md). Code style: [STYLE.md](./STYLE.md). Chaos: [CHAOS.md](./CHAOS.md).

---

## Requirements

- Go 1.25+
- Docker Compose
- `buf` (or `make proto`)

---

## Quick start

```bash
cp .env.example .env
bash scripts/local-dev/dev_stack.sh build
bash scripts/local-dev/dev_stack.sh full
bash scripts/local-dev/dev_preflight.sh
```

| `dev_stack.sh` mode | Contents |
| :--- | :--- |
| `infra` | Postgres, Redis ×6, ClickHouse |
| `full` | All services |
| `sentinel` | Redis Sentinel |

---

## CI merge gates

```bash
go test ./... -short
make lint
bash scripts/ci/check_comments.sh
bash scripts/chaos-drills/test_chaos.sh
bash scripts/perf-gate/perf_gate_run.sh    # when ingestion/rtb touched
make test-alloc-gate
bash scripts/ci/check_compliance.sh
```

Chaos steady-state: `/track` p99 < 80 ms; error rate < 0.1% (excluding valid rejects). See [CHAOS.md](./CHAOS.md) R1.

---

## Slot migration (M1)

### Default path

1. **COPY** — `CampaignKeyMigrator` `DUMP`/`RESTORE` per `CampaignRedisKeyCatalog`.
2. **Fence** — `MIGRATION_FENCE_ENABLED=true` → Lua code `11`.
3. **PG re-warm** — `RewarmCampaignBudgetKeys` on target from `budget_limit - current_spend`.
4. **EXISTS gate** — reject activation if required keys missing on target.
5. **Epoch bump** — `ActivateSlotMapVersionWithMigration`; broker reload.
6. **Drain** — delete keys on source shard.

### Dual-write (opt-in)

`SLOT_MIGRATION_DUAL_WRITE_ENABLED=true`: COPY → `dual_writing` → `slot_migration:delta` stream → lag catch-up → cutover when `ad_slot_migration_lag_messages ≤ SLOT_MIGRATION_LAG_EPSILON`.

| Env | Default |
| :--- | :--- |
| `SLOT_MIGRATION_LAG_EPSILON` | `0` |
| `SLOT_MIGRATION_LAG_THRESHOLD` | `1000` |

### Rollback

1. `RollbackSlotMapVersion(ctx, adminID, previousVersion)`
2. Optional `DrainCampaignKeys` on failed target
3. PG re-warm source if drained
4. `VerifySlotMigrationR5`, `AssertBudgetInvariant`
5. Clear `budget:migration_fence:{uuid}`

Chaos: `TestChaos_SlotMigrationRollbackAfterActivate`, `TestChaos_SO02_SlotMigrationPGRewarmCutover`, `TestChaos_LUA10_DebitFencedDuringSlotCopy`.

Elastic sharding (M2): [DATA.md](./DATA.md) Part I §7.

---

## Shard-0 outage (M14 / GAP-SHARD-04)

Shard 0 holds `campaigns:update` pub/sub, auth lockout, and is the default outbox notify target. Shards 1–3 hold ~75% of campaign debit keys. Sentinel promote typically recovers shard 0 in ~10–15 s.

### Expected behavior while redis-0 is down

| Surface | Behavior |
| :--- | :--- |
| Track shards 1–3 | Continue accepting; p99 stays within SLA |
| Track shard-0 campaigns | Explicit `503 shard_unavailable` (or debit via `campaign_routing` reserve when M2 triplet present) — never silent accept |
| Unknown campaign IDs | After `REGISTRY_STALE_TTL` (default 30 s) without pub/sub: `503 registry_stale` (not 404) |
| Global keys | `config:values`, `blacklist:*`, `ml:score:boost:*`, placement pause hashes already fan-out to all masters; tracker reads local copy |
| Management outbox | Events needing shard-0 write/notify stay `PENDING` until recovery |
| Metric / alert | `ad_registry_stale_mode`, `ad_shard0_pubsub_unreachable` → alert `Shard0PubSubUnreachable` |

### Operator steps

1. Confirm Sentinel: `redis-cli -p <sentinel> SENTINEL masters` — wait for promote.
2. Watch `ad_redis_breaker_state{shard="0"}` and `ad_registry_stale_mode`.
3. After master is up: outbox worker drains PENDING; shard-0 track returns 202.
4. Optional: enable `CAMPAIGN_UPDATE_BROKER_FALLBACK=true` so trackers reconcile via broker topic `campaigns:update` without shard-0 Redis.

| Env | Default | Purpose |
| :--- | :--- | :--- |
| `REGISTRY_STALE_TTL` | `30` (seconds) | Pub/sub quiet → stale-serve |
| `CAMPAIGN_UPDATE_BROKER_FALLBACK` | `false` | Broker secondary notify path |
| `CAMPAIGN_UPDATE_BROKER_TOPIC` | `campaigns:update` | Broker topic name |

Chaos: `TestChaos_Shard0Outage`, `scripts/chaos-drills/m14_shard0_failure.sh`.

Blast radius: [DATA.md](./DATA.md) Part I §2.

---

## Kubernetes

```bash
bash scripts/k8s/install_k3s.sh
bash scripts/k8s/k8s_cold_path_up.sh   # cold path namespace
bash scripts/k8s/k8s_hot_path_up.sh    # trackers + nginx, hostNetwork
```

---

## Code generation

| Command | Output |
| :--- | :--- |
| `make proto` | vtproto in `internal/*/pb/` |
| `make gen` | sqlc in `internal/*/sqlc/` |

---

## Scripts

| Path | Purpose |
| :--- | :--- |
| `scripts/local-dev/dev_stack.sh` | Compose lifecycle |
| `scripts/perf-gate/perf_gate_run.sh` | Benchmark gate |
| `scripts/chaos-drills/test_chaos.sh` | Fault injection |
| `scripts/edge-tuning/edge_nic_tune.sh` | NIC tuning |
| `scripts/redis-ops/` | Shard ops |
| `scripts/load-test/` | Load tests |

---

## Ports

| Service | Port |
| :--- | :--- |
| Nginx | 8180 |
| Tracker | 8181–8184 |
| Processor | 8186 |
| Management HTTP / gRPC | 8188 / 51053 |
| UDP control | 8190 → 8191 |
| Auth / Payment / Billing | 51051 / 51052, 8187 / 51054 |
| Redis shards | 6479–6482 |
| PostgreSQL / ClickHouse | 5430 / 9000 |

---

## Key environment variables

Full list: `.env.example`.

| Variable | Role |
| :--- | :--- |
| `FILTER_TIMEOUT_MS` | Filter deadline (≤ 100 prod) |
| `TRACKER_PG_FALLBACK` | `0` in production |
| `RTB_MODE` | `off` / `shadow` / `live` |
| `PROCESSOR_PG_GATE_SLOTS` | PG write concurrency |
| `CH_SPOOL_SEGMENT_MB` | CH outage spool |
| `LOCAL_QUOTA_MODE` | `live` for M8 local quanta |

---

## Testing

```bash
go test ./... -short
go test ./internal/ingestion/ -run 'TestChaos_' -timeout 15m
go test ./tests/e2e/... -count=1
EXPLAIN_AUDIT=1 go test ./internal/database/... -run TestExplainAudit
```

Hot-path change: `make test-alloc-gate`; chaos if write path changed ([CHAOS.md](./CHAOS.md) R10).

---

## Anti-fraud operations

- Disable ML workers: `FRAUD_SCORING_ENABLED=false`; restart `fraud-scorer`, `ivt-detector`.
- Reset boost: management API → `ML_SCORE_BOOST` outbox.
- Unblock IP: remove `ip_blacklist` + `UPDATE_BLACKLIST` outbox.

Actions logged in `audit_logs`.
