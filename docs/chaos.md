# Chaos engineering tests

Chaos tests inject **real** faults (container terminate, `chmod 0000`, concurrent races) and assert quantitative degradation plus data invariants. They are **not** run under `-short`.

## Run

```bash
make test-chaos
```

Requires Docker (testcontainers). Output is tee'd to `/tmp/espx-chaos.log`; the Makefile fails if fewer than 5 `chaos_proof` lines are emitted.

## Proof format

Each chaos test logs a grep-friendly line:

```text
chaos_proof fault=<name> subsystem=... baseline_ok=true fault_verify=... <metrics>
```

## Catalog

| Package | Test | Fault | Expected effect |
|---------|------|-------|-----------------|
| `tests` | `TestChaos_RedisTerminateStopsIngest` | Redis container kill | ≥80% `/track` non-202 |
| `tests` | `TestChaos_PostgresKillOpensConsumerCircuit` | PG kill | Consumer circuit open, stream retains messages |
| `tests` | `TestChaos_StreamBacklogUnderPostgresOutage` | PG kill | Redis stream grows for accepted events |
| `internal/auth` | `TestChaos_AuthRedisTerminateFailClosedVerify` | Redis kill | VerifyToken fail-closed |
| `internal/auth` | `TestChaos_AuthPGTerminateBlocksLogin` | PG kill | Login fails, `pool.Ping` errors |
| `internal/auth` | `TestChaos_AuthRefreshReuseDetection` | Refresh replay | `ErrSessionBlocked`, one active session |
| `internal/auth` | `TestChaos_AuthBlockUserRevokesInFlight` | `BlockUser` | `revoked:user` in Redis, verify denied |
| `pkg/broker/server` | `TestChaos_ReadonlyDataDirHealthz` | `chmod 0000` data dir | healthz 503 then recovery |
| `internal/management` | `TestChaos_*` | Deadlock, worker races, slow Redis | Exactly-once / balance invariants |

## Anti-slop checklist

1. Real fault (container/OS), not only interface mock
2. Baseline green before fault
3. Independent fault verification (`Ping` fails, `probeDisk` false)
4. Quantitative effect threshold
5. Data invariant (sessions, balances, outbox)
6. `chaos_proof` log line
7. `t.Skip` when `-short`

Mock-only fault injection belongs in `*FaultInjection*` or `*Integration*` tests, not `TestChaos_*`.
