# Milestone 3 chaos proofs

Catalog of `chaos_proof` lines emitted by M3 licensing/subscription tests.
Counted by `scripts/chaos-drills/test_chaos.sh` (`CHAOS_MIN_PROOFS`, default 52).

| fault | Package | Scenario |
| :--- | :--- | :--- |
| `license_spool_buffer_overflow` | `internal/licensing` | mmap WAL segment full → `ErrLicenseSpoolFull` |
| `license_spool_concurrent_append` | `internal/licensing` | 24 goroutines append; latest token recovered |
| `entitlement_buffer_oom_guard` | `internal/management` | Bounded channel rejects overload without panic |
| `entitlement_buffer_recovery` | `internal/management` | `Recover()` replays customer IDs after restart |
| `update_entitlements_redis_recovery` | `internal/management` | Redis outage during outbox handler, self-heal |
| `update_entitlements_idempotent` | `internal/management` | 5× replay of `UPDATE_ENTITLEMENTS` handler |

Run locally:

```bash
go test ./internal/licensing/... -run 'TestChaos_License|TestLicenseSpool'
go test ./internal/management/... -run 'TestChaos_Entitlement|TestChaos_Update|TestChaos_Subscription'
```
