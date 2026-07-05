# Log Compactor P3 — S3 TierStore + Leader Election

## Scope

P3 completes the cloud path and multi-instance safety:

1. **S3 TierStore** — replaces MVP stub with scratch-sync + upload
2. **Leader election** — POSIX `flock` single-writer lock for HA deployments

## S3 TierStore

Hybrid model: S3 is source of truth for hot/warm objects; compaction runs on local scratch (same lifecycle as `LocalTierStore`).

Flow: sync hot prefix from S3 to `scratch/hot`, compact locally (ClaimHot/Rollback), write `scratch/warm` (`.compact.zst` + `.meta.json`), upload to S3 warm prefix.

### API

`NewS3TierStore(ctx, S3Config)` — requires `Region`, `Bucket`, optional `HotPrefix`, `WarmPrefix`, `ScratchDir`.

| Method | Behaviour |
|--------|-----------|
| `ListHot` | ListObjectsV2, download missing hot segments to scratch, then local `ListHot` |
| `WriteWarmFromFile` | Local 2-phase warm write, then PutObject warm + meta to S3 |
| `RemoveCompacting` | Delete scratch + S3 hot object |
| `LocalScratch()` | Exposes scratch `LocalTierStore` for cold-tier rollup |

### Test double

`MemoryS3TierStore` + `MemoryObjectStore` — in-process S3 for unit/chaos tests without AWS.

## Leader election

`FileLeaderLock` uses non-blocking `flock(LOCK_EX|LOCK_NB)` on `LOG_COMPACTOR_LEADER_LOCK_PATH`.

- Metric: `log_compactor_leader` (0/1)
- Standby instances skip `RunOnce` until lock acquired
- Optional via `LOG_COMPACTOR_LEADER_ELECTION=true` (default false)

Integrated via `WithLeaderLock()` compactor option in `cmd/log-compactor/main.go`.

## Config

| Env | Default | Description |
|-----|---------|-------------|
| `LOG_COMPACTOR_LEADER_ELECTION` | `false` | Enable flock leader election |
| `LOG_COMPACTOR_LEADER_LOCK_PATH` | `/var/lib/espx/log-compactor.leader.lock` | Lock file path |
| `LOG_COMPACTOR_S3_SCRATCH_DIR` | `/var/lib/espx/log-compactor/scratch` | Local staging for S3 backend |

S3 backend additionally requires `LOG_COMPACTOR_S3_BUCKET`, `LOG_COMPACTOR_S3_REGION`.

## Chaos tests (GUIDE_CHAOS_RELIABILITY)

| Test | Fault | Guard |
|------|-------|-------|
| `TestChaos_logCompactorS3TierExactlyOnce` | `log_compactor_s3_tier_exactly_once` | Idempotent warm upload, no duplicate segments |
| `TestChaos_logCompactorS3WarmUploadRetry` | `log_compactor_s3_warm_upload_retry` | Transient upload failure, retry, checkpoint |
| `TestChaos_logCompactorLeaderElectionSingleWriter` | `log_compactor_leader_election` | 2 instances, 10 segments, no duplicates |

Prior P0/P1 chaos tests unchanged:

- `log_compactor_checkpoint_crash_recovery`
- `log_compactor_compacting_recovery`
- `log_compactor_warm_write_rollback`
- `log_compactor_concurrent_stress`
- `log_compactor_pipeline_marker`

## Files

| Path | Change |
|------|--------|
| `internal/logcompactor/s3_tier_store.go` | AWS S3 tier implementation |
| `internal/logcompactor/memory_tier_store.go` | In-memory S3 test double |
| `internal/logcompactor/leader.go` | FileLeaderLock |
| `internal/logcompactor/p3_chaos_test.go` | P3 chaos tests |
| `internal/logcompactor/compactor.go` | Leader-gated run loop |
| `cmd/log-compactor/main.go` | S3 ctx init, leader option, S3 cold scratch |
| `internal/config/log_compactor.go` | P3 env vars |

## Test plan

```bash
go test ./internal/logcompactor/... -count=1 -timeout 120s -v
go test ./internal/logcompactor/... -run Chaos -count=1 -timeout 120s -v | grep chaos_proof
```
