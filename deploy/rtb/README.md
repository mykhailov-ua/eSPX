# RTB compose overlay

Default `docker-compose.yml` + `.env` ship with **`RTB_MODE=off`** (client `campaign_id` path).

Use this overlay to run a **live RTB soak** on all tracker replicas (budget authority in-process, optional targeting index).

```bash
docker compose -f docker-compose.yml -f deploy/rtb/docker-compose.override.yml up -d tracker-0 tracker-1 tracker-2 tracker-3
```

Or merge keys from `deploy/rtb/env.example` into `.env` and restart trackers.

## Cutover gates (Grafana: RTB Cutover, `uid=rtb-cutover`)

- `ad_rtb_budget_reconcile_high == 0`
- `rate(ad_rtb_budget_spend_rejected_total[5m])` near zero
- Tracker p95/p99 within SLA

Set `RTB_TARGETING_INDEX=true` only after inverted-index soak passes (default in overlay is `false`).
