# RTB in-process auction cutover runbook

In-process RTB (`internal/rtb` + `internal/ads/rtb_*`) selects campaign winners on the tracker hot path before Redis filters. Budget authority can remain on Lua during early live rollout, then move to `CheckAndSpendAll` when shadow divergence is acceptable.

## Preconditions

- Tracker built with RTB catalog sync (`REGISTRY_SYNC_INTERVAL_MS` matches management publish cadence).
- Prometheus scrapes tracker `/metrics` (sidecar or management proxy).
- Default production: `RTB_MODE=off`. Shadow requires explicit env change.
- Optional: `RTB_SNAPSHOT_PATH` for campaign budget persistence across tracker restarts.

## Env reference

| Variable | Values | Role |
|----------|--------|------|
| `RTB_MODE` | `off` / `shadow` / `live` | Shadow eval-only; live replaces `campaign_id` on win |
| `RTB_BUDGET_AUTHORITY` | `redis` (default) / `rtb` | Who debits budget in live mode |
| `RTB_CLEARING_MODE` | `second` / `first` | Clearing price policy |
| `RTB_TARGETING_INDEX` | `true` / `false` (default) | Geo+device+category inverted index (**staging only**) |
| `RTB_SNAPSHOT_PATH` | file path | Persist campaign budgets (not daily/customer spend) |

RTB auction metrics (`ad_rtb_*`) are enabled automatically when `RTB_MODE≠off`.

## Phase 1 — Shadow validation (7–14 days)

1. Deploy tracker with `RTB_MODE=shadow`, `RTB_BUDGET_AUTHORITY=redis` (default).
2. Client `campaign_id` is **not** changed; Lua budget remains authoritative.
3. Watch metrics:
   - `ad_rtb_shadow_winner_mismatch_total` — RTB eval winner ≠ client `campaign_id`. Target: stable rate **< 5%** of track volume (`RtbShadowWinnerMismatchHigh` alert).
   - `ad_rtb_shadow_no_bid_total{reason}` — no-bid breakdown while client sent a campaign.
   - `ad_rtb_shadow_price_delta_micro` — sampled clearing vs payload `bid_micro` on shadow wins.
   - `ad_rtb_auction_no_bid_total{reason}` — fill rate by reason (pacing, daily, spend_failed).
   - `ad_rtb_auction_duration_seconds` — in-process p99 **< 15µs** (`RtbAuctionLatencyHigh`).
   - `ad_rtb_auction_candidates_scanned` — dense shard warning (`RtbAuctionCandidatesScannedHigh`).
4. Tracker SLA (`.cursorrules`): p95 < 50ms, p99 < 80ms on `ad_http_request_duration_seconds` (`TrackerLatencyP95Warning` / `TrackerLatencyP99Critical`).
5. Redis Lua p99 < 15ms per shard (`RedisLuaLatencyHigh`).

Shadow does **not** change billing. Compare audit samples only.

## Phase 2 — Live + Redis budget authority

1. Set `RTB_MODE=live`, keep `RTB_BUDGET_AUTHORITY=redis`.
2. RTB winner replaces `campaign_id` before `FilterEngine.Check`; Lua still debits budget.
3. Monitor:
   - `ad_rtb_auction_win_total` vs track accept rate.
   - `ad_filter_decisions{bid_floor,campaign_not_found}` — unexpected reject spikes.
   - Financial recon unchanged (Redis budget path).
4. Roll back to `shadow` if accept rate or revenue diverges from baseline.

## Phase 3 — Live + RTB budget authority

1. Set `RTB_MODE=live`, `RTB_BUDGET_AUTHORITY=rtb`.
2. Lua `unified-filter.lua` skips budget debit (`skip_budget`); dedup, fcap, rate, stream remain.
3. Monitor:
   - `ad_rtb_budget_spend_rejected_total` — CAS failures after winner selection.
   - `ad_rtb_budget_reconcile_high` — Redis vs RTB drift (should stay 0).
   - `ad_rtb_budget_reconcile_divergence_micro` — sample delta histogram.
4. After restart: campaign budgets restore from snapshot; daily/customer spend resync from Redis on catalog tick.

### Staging soak (dev/stage cluster)

1. Copy or merge `.env.rtb-staging` into `.env`, **or** use the compose overlay:
   ```bash
   docker compose -f docker-compose.yml -f deploy/rtb-staging/docker-compose.override.yml up -d tracker-0 tracker-1 tracker-2 tracker-3
   ```
2. Open Grafana dashboard **RTB Cutover** (`uid=rtb-cutover`) — reconcile stat, shadow mismatch, spend rejections.
3. Run integration gate locally: `go test -count=1 -run TestE2E_RtbLiveBudgetAuthority ./tests/` (requires Docker).
4. Hold ≥ 48h with `ad_rtb_budget_reconcile_high == 0` and nominal tracker p95/p99 before prod Phase 3.
5. **Phase 5 (staging):** `RTB_TARGETING_INDEX=true` — validate auction no-bid/latency vs geo-only path before prod enable.

## Go / no-go checklist

| Check | Pass criteria |
|-------|----------------|
| Shadow duration | ≥ 7d with stable traffic |
| Winner mismatch | `rate(mismatch)/rate(requests)` < 5% for 7d |
| Tracker p95 / p99 | < 50ms / < 80ms under nominal load |
| RTB in-process p99 | < 15µs |
| Redis Lua p99 | < 10ms per shard |
| Corrupt catalog | `ad_rtb_auction_no_bid_total{reason="corrupt_catalog"}` == 0 |
| Chaos / race | `go test -race ./internal/rtb/...` green on release candidate |
| Perf gate | `make test-alloc-gate` + `perf_gate` ±5% on hot benches |

## Rollback

1. `RTB_MODE=shadow` — immediate; client campaign restored as authority for selection (eval only).
2. `RTB_MODE=off` — disable auction entirely; pure client `campaign_id` + Lua path.
3. If `RTB_BUDGET_AUTHORITY=rtb` was active: revert to `redis` **before** or together with `live` off to avoid double-authority confusion.
4. No snapshot truncation required on rollback.

## Known limitations (pre–Phase 2)

- Per-campaign bid/CTR still default from `CLICK_AMOUNT` until management publishes hybrid metadata per campaign.
- Customer pool = sum of campaign remainings (approximation until Redis customer limit sync).
- Geo lookup runs once per request (deduped); MaxMind still allocates country string once.

## Related docs

- `docs/rtb-tech-report.md` — implementation and benchmarks
- `docs/rtb-chaos-plan.md` — fault injection matrix
- `.cursorrules` — Tracker latency SLA
- `docs/broker-cutover.md` — parallel ingest cutover (independent track)
