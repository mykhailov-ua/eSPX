# Chaos M0 — Validation Report

Date: 2026-07-05  
Status: All five M0 chaos proofs implemented and passing on testcontainers (Postgres 16 + Redis 7)

## Summary

Milestone M0 chaos suite per `docs/MILESTONE.md:73`. Each test follows `GUIDE_CHAOS_RELIABILITY_RU.md`: one **steady-state hypothesis**, one **injected fault** (R2), one **measurable invariant**, and one grep-able `chaos_proof fault=...` line. No `sqlmock`; money/outbox paths use real PG + Redis containers.

## Results (2026-07-05 run)

| `chaos_proof fault=` | Test | Result | Duration | Invariant (must hold after fault) |
|----------------------|------|--------|----------|-----------------------------------|
| `quota_dead_shard_release` | `TestChaos_QuotaDeadShardRelease` | **PASS** | 6.86s | `campaign_quotas.reserved_amount` → 0 on dead shard |
| `autoscale_no_double_freeze` | `TestChaos_AutoscaleNoDoubleFreeze` | **PASS** | 3.53s | Exactly 1 FREEZE + 1 RELEASE ledger row; customer balance unchanged |
| `outbox_priority_lanes` | `TestChaos_OutboxPriorityLanes` | **PASS** | 2.95s | `UPDATE_BLACKLIST` PROCESSED before 500 pacing rows |
| `financial_recon_ops_alert` | `TestChaos_FinancialReconOpsAlert` | **PASS** | 3.98s | Notifier receives 1 alert with `MISSING_LEDGER_TOPUP` |
| `settlement_failed_notifier` | `TestChaos_SettlementFailedNotifier` | **PASS** | 2.66s | Outbox `DEAD` + notifier dedup key `payment-settlement-failed:{intent_id}` |

### Captured `chaos_proof` lines (verbatim from `go test -v`)

```
chaos_proof fault=quota_dead_shard_release released_micro=2000000 subsystem=management_quota_recon shard=0 reserved_after=0 baseline_ok=true fault_verify=redis_container_stopped

chaos_proof fault=autoscale_no_double_freeze autoscale_release=1 limit_low=90000000 limit_high=110000000 subsystem=management_autoscale baseline_ok=true fault_type=concurrency_stress workers=24 autoscale_freeze=1

chaos_proof fault=outbox_priority_lanes subsystem=management_outbox pacing_backlog=500 blacklist_first=true baseline_ok=true fault_type=priority_inversion

chaos_proof fault=financial_recon_ops_alert findings=1 subsystem=payment_financial_recon notified=true baseline_ok=true fault_type=missing_topup

chaos_proof fault=settlement_failed_notifier dedup_key=payment-settlement-failed:019f3330-a6b4-7ea0-8ce9-d439f3366e66 subsystem=payment_outbox notified=true baseline_ok=true fault_type=missing_customer intent_id=019f3330-a6b4-7ea0-8ce9-d439f3366e66
```

## Why this is real chaos (not mock-only)

| Test | Fault injection | Real infra | What would fail if code regressed |
|------|-----------------|------------|-----------------------------------|
| `quota_dead_shard_release` | `stopMgmtContainer` on Redis testcontainer; `requireMgmtFaultActive` asserts `PING` fails | `postgres:16-alpine` + `redis:7-alpine` via testcontainers | Stuck `reserved_amount` stays 2M after shard outage |
| `autoscale_no_double_freeze` | **24 concurrent** `AutoscaleBudgets` goroutines racing same stats fingerprint | Real PG + Redis; `pg_advisory_xact_lock` path | Double FREEZE/RELEASE or balance drift |
| `outbox_priority_lanes` | **500** `UPDATE_CAMPAIGN_PACING` rows then 1 newer `UPDATE_BLACKLIST` | Bulk `generate_series` insert + live outbox worker | FIFO would process pacing first; blacklist stays PENDING |
| `financial_recon_ops_alert` | Succeeded payment intent **without** ledger TOPUP (real PG drift) | Payment + ads schema on testcontainers; live `ReconService.Run` | Silent recon with no notifier enqueue |
| `settlement_failed_notifier` | `DELETE FROM customers` then settlement gRPC → **`NotFound`** | Live settlement gRPC server in `setupPaymentChaosInfra`; log shows real RPC error | Outbox not `DEAD` or no ops alert |

### Runtime evidence (not synthetic assertions only)

**Container fault (`quota_dead_shard_release`):** testcontainers stops Redis; proof includes `fault_verify=redis_container_stopped`.

**gRPC fault (`settlement_failed_notifier`):** worker log during run:

```
ERROR failed to handle outbox outboxEventent id=1 error="management SettlementService call failed: rpc error: code = NotFound desc = customer not found"
```

**Concurrency (`autoscale_no_double_freeze`):** `workers=24` in proof; test counts ledger rows `WHERE type IN ('FREEZE','RELEASE')` and asserts `balance_before == balance_after`.

**Backlog inversion (`outbox_priority_lanes`):** `ProcessOutboxWithCount(ctx, 1)` returns 1; SQL asserts 500 pacing rows still `PENDING` and Redis `SISMEMBER blacklist:manual` is true.

## Reproduce

```bash
# Management M0 (3 tests)
go test ./internal/management/... \
  -run 'TestChaos_QuotaDeadShardRelease|TestChaos_AutoscaleNoDoubleFreeze|TestChaos_OutboxPriorityLanes' \
  -count=1 -timeout 15m -v 2>&1 | grep chaos_proof

# Payment M0 (2 tests)
go test ./internal/payment/... \
  -run 'TestChaos_FinancialReconOpsAlert|TestChaos_SettlementFailedNotifier' \
  -count=1 -timeout 15m -v 2>&1 | grep chaos_proof
```

Requires Docker (testcontainers). Do not use `-short` (tests skip).

## Source files

| Fault | File |
|-------|------|
| `quota_dead_shard_release` | `internal/management/quota_chaos_test.go` |
| `autoscale_no_double_freeze` | `internal/management/autoscale_chaos_test.go` |
| `outbox_priority_lanes` | `internal/management/outbox_priority_chaos_test.go` |
| `financial_recon_ops_alert` | `internal/payment/recon_chaos_test.go` |
| `settlement_failed_notifier` | `internal/payment/settlement_failed_chaos_test.go` |

## Related reports

- [QUOTA_MANAGER.md](./QUOTA_MANAGER.md) — quota refill + dead shard release
- [AUTOSCALE_BUDGETS.md](./AUTOSCALE_BUDGETS.md) — autoscale ledger idempotency
- [PAYMENT_FINANCIAL_RECON.md](./PAYMENT_FINANCIAL_RECON.md) — recon findings + ops alert
- [MANAGEMENT_OPS_ALERTS.md](./MANAGEMENT_OPS_ALERTS.md) — notifier wiring
