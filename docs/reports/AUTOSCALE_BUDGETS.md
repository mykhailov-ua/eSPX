# AutoscaleBudgets (M0.2) — Technical Report

Date: 2026-07-05  
Status: Implemented

## Summary

CTR-based budget shifting moves micro-units from low-performing campaigns to high-performing siblings under the same customer. Each transfer records `RELEASE`/`FREEZE` ledger rows, updates `campaigns.budget_limit`, and enqueues `CREATE_CAMPAIGN` outbox events for Redis propagation.

## Runtime

| Env | Default | Meaning |
|-----|---------|---------|
| `AUTOSCALE_INTERVAL_MS` | `0` | Worker off; set e.g. `300000` for 5 min ticks |
| `AUTOSCALE_HIGH_CTR_THRESHOLD` | `0.015` | Donor must exceed this CTR |
| `AUTOSCALE_LOW_CTR_THRESHOLD` | `0.005` | Recipient must be below this CTR |
| `AUTOSCALE_MIN_IMPRESSIONS` | `100` | Minimum impressions for high-CTR candidate |
| `AUTOSCALE_SHIFT_AMOUNT` | `$10` µ | Micro-units moved per transfer |
| `AUTOSCALE_MIN_REMAINING_BUDGET` | `$20` µ | Donor must retain at least this headroom |

Worker: `AutoscaleBudgetWorker` in `cmd/management/main.go` when `AUTOSCALE_INTERVAL_MS > 0`. Each tick calls `syncWorkers[].SyncAll` before `AutoscaleBudgets`.

## Transfer flow

1. Group active campaigns by `customer_id` (`GetAllActiveCampaignsWithStats`).
2. Per customer: `pg_advisory_xact_lock` + pick worst/best CTR pair.
3. Idempotency key `autoscale-transfer:{worst}:{best}:{stats}` — skip if FREEZE row exists.
4. `RELEASE` on donor + credit customer balance; `FREEZE` on recipient + debit balance (net zero).
5. Update both `budget_limit` values; enqueue two `CREATE_CAMPAIGN` outbox rows.

## Chaos tests

| Test | `chaos_proof fault=` | Guard |
|------|----------------------|-------|
| `TestChaos_AutoscaleNoDoubleFreeze` | `autoscale_no_double_freeze` | 24 concurrent ticks → one FREEZE/RELEASE pair |
| `TestChaos_AutoscaleInsufficientTotalBudget` | `autoscale_insufficient_budget` | Skip when spend would exceed reduced limit |

Unit: `TestSmartBudgetAutoscaling` — sync flush, limits, ledger rows, balance unchanged.

## Test results (2026-07-05)

```bash
go test ./internal/management/... -run 'Autoscale|autoscale|SmartBudget' -count=1 -timeout 15m -v
```

All tests PASS.
