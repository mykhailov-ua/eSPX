# M7 — Chaos Kong Game Day (Manual)

**Milestone:** M7 Multi-Region (`Exec #13`)  
**Fault class:** Chaos Kong (full regional cell outage) per [GUIDE_CHAOS_RELIABILITY.md](../../GUIDE_CHAOS_RELIABILITY.md)  
**Automation:** Manual drill only — do **not** run in CI.

## Steady-state hypothesis

Before the drill, confirm:

1. Global Postgres is reachable from the control plane.
2. Each regional cell reports `GET /readyz` healthy on management and tracker.
3. `ad_region_outbox_delivery_lag_seconds` p99 < 30 s per cell.
4. Budget invariant holds on a canary campaign in each cell (`current_spend ≤ budget_limit`).

## Topology

| Cell | `ESPX_REGION_CODE` | Redis | Tracker | Management relay |
| :--- | :--- | :--- | :--- | :--- |
| Global control | `0` | none (PG only) | none | fan-out via `outbox_region_delivery` trigger |
| Region A | `1` | 4 shards (isolated) | yes | `RegionOutboxRelay` |
| Region B | `2` | 4 shards (isolated) | yes | `RegionOutboxRelay` |

License: Enterprise plan with JWT `multi_region: true`. Installer: `multi_region: true` in `install.yaml`.

## Drill steps

### 1. Baseline (15 min)

```bash
# Per cell — replace REGION with 1 or 2
export ESPX_REGION_CODE=1 MULTI_REGION_ENABLED=1
curl -sf http://127.0.0.1:8081/readyz
curl -sf http://127.0.0.1:8080/readyz
```

Record Prometheus snapshots: `ad_http_request_duration_seconds`, `ad_region_outbox_delivered_total`, `ad_tracker_local_quota_block_total`.

### 2. Inject Kong — isolate Region A (10 min)

Stop **all** Region A services (tracker, processor, management, Redis) simultaneously. Do **not** stop global Postgres or Region B.

```bash
# Example: compose profile region-a
docker compose -f scripts/local-dev/docker-compose.yml stop tracker-a processor-a management-a redis-a-0 redis-a-1 redis-a-2 redis-a-3
```

**Expected:**

- GeoDNS/Anycast routes new traffic to Region B only.
- Region B continues processing; no cross-region Redis Lua.
- Global PG outbox rows fan out; Region B deliveries reach `DELIVERED`; Region A stays `PENDING`.
- No budget overrun on canary campaigns (±1μ invariant).

### 3. Recovery (15 min)

Bring Region A back online. Verify `RegionOutboxRelay` drains pending deliveries:

```bash
# Watch lag drain
watch -n2 'curl -s localhost:9090/metrics | grep ad_region_outbox_delivery_lag'
```

**Expected:**

- Pending `outbox_region_delivery` rows for region_code=1 transition to `DELIVERED`.
- `region_apply_idempotency` prevents duplicate Redis writes on replay.
- Steady-state latency returns to p95 < 50 ms within 5 min.

### 4. Rollback criteria (abort drill)

Abort and escalate if:

- Global Postgres unavailable > 60 s.
- Region B p99 > 80 ms for 30 s sustained.
- Budget invariant violation on any canary campaign.
- Cross-cell Redis key visible (cell isolation failure).

## Proof line

After a successful drill, log:

```text
chaos_proof fault=chaos_kong_region_outage subsystem=multi_region cell_a_down=true cell_b_ok=true relay_catchup=true budget_invariant=true
```

## Related tests (automated)

```bash
go test ./internal/management/... -run 'TestRegion' -short
go test ./internal/ingestion/... -run 'IngressDayKey|RPDCodec' -short
```
