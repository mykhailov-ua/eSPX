# RTB staging soak

Staging runs **live + RTB budget authority + targeting inverted index**.

## Quick start

```bash
# Option A: merge env
cat .env.rtb-staging >> .env   # or copy keys manually

# Option B: compose overlay
docker compose -f docker-compose.yml -f deploy/rtb-staging/docker-compose.override.yml up -d tracker-0
```

## Env (see `.env.rtb-staging`)

| Variable | Staging value |
|----------|----------------|
| `RTB_MODE` | `live` |
| `RTB_BUDGET_AUTHORITY` | `rtb` |
| `RTB_TARGETING_INDEX` | `true` |
| `RTB_SNAPSHOT_PATH` | `/var/lib/espx/rtb_snapshot.bin` |

Grafana: dashboard **RTB Cutover** (`uid=rtb-cutover`).

Production uses `.env.rtb-prod` stub — see `deploy/rtb-prod/README.md`.
