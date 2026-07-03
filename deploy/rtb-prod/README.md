# RTB production stubs

Production keeps RTB **disabled** until the cutover checklist in `docs/rtb-cutover.md` is signed off.

## Files

| File | Purpose |
|------|---------|
| [`.env.rtb-prod`](../../.env.rtb-prod) | Env stub: `RTB_MODE=off`, `RTB_TARGETING_INDEX=false` |
| `docker-compose.stub.yml` | No-op overlay — documents that prod compose is unchanged |

## Usage

Do **not** apply staging overlays (`deploy/rtb-staging/`) to production.

Default `docker-compose.yml` + `.env` already ship `RTB_MODE=off`.

When starting prod shadow (Phase 1), set only:

```bash
RTB_MODE=shadow
RTB_BUDGET_AUTHORITY=redis
```

Leave `RTB_TARGETING_INDEX=false` until staging Phase 5 soak validates the inverted index.

## Staging vs prod

| Setting | Staging (`.env.rtb-staging`) | Prod (`.env.rtb-prod`) |
|---------|------------------------------|-------------------------|
| `RTB_MODE` | `live` | `off` |
| `RTB_BUDGET_AUTHORITY` | `rtb` | `redis` |
| `RTB_TARGETING_INDEX` | `true` | `false` |

See `docs/rtb-cutover.md` for phased rollout.
