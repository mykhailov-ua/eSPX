# Shipped Platform Capabilities

This document records **delivered capabilities** for closed milestones M1–M3 (core), M2, and M5. **Open issues:** [GAPS.md](./GAPS.md). **Active roadmap (easy → hard):** [MILESTONE.md §2](./MILESTONE.md#2-roadmap-overview--execution-order-easy--hard).

**Navigation:** [ARCHITECTURE.md](./ARCHITECTURE.md) · [REDIS.md](./REDIS.md) · [EDGE.md](./EDGE.md) · [MANAGEMENT.md](./MANAGEMENT.md) · [LICENSING.md](./LICENSING.md) · [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md) · [REMEDIATION.md](./REMEDIATION.md)

---

## Services and binaries (production stack)

| Binary | Port(s) | Role | Milestone |
| :--- | :--- | :--- | :--- |
| `tracker` | 8181–8184 | gnet ingest, Go filters, tiered Lua, registry, UDP ingress gate | M1 |
| `processor` | 8186 | Per-shard Redis streams → PG/CH; SyncWorker; CH mmap spool; PG/CH gates | M1 |
| `management` | 8188, 51053 | Admin HTTP, settlement gRPC, outbox, recon, quota, slot migration, UDP control :8190 | M1–M2 |
| `auth` | 51051 | gRPC: Argon2id, PASETO, sessions, API keys; Redis shard 0 lockout | M1 |
| `payment` | 51052, 8187 | Payment intents gRPC, Stripe webhooks, settlement outbox | M1 |
| `billing` | 51054 | Invoice generation gRPC (`balance_ledger` only) | M2 |
| `notifier` | 8085 | Async notifications (Telegram, Slack, SMTP, SMS) | M2 |
| `license-server` | vendor | Issue/renew/revoke license JWT (vendor DC only) | M3 |
| `ivt-detector` | — | CH batch → fraud blacklist via management | M1 |
| `fraud-scorer` | compose profile | ML scoring → management outbox | M1 |
| `edge-xdp`, `edge-bpf-sync` | — | Defensive XDP + BPF map sync from Redis | M5 |
| `admin`, `dlq` | — | Operator CLIs | M2 |

**Library packages (not separate deployables):** `internal/adminapi` (JSON `/api/v1`), `internal/licensing`, `internal/billing`, `internal/edge/{allowlist,blocklist}`.

---

## Milestone 1 — Architectural remediation (single-site)

**Status:** closed (except INST-P1 installer → M9 backlog).

### Durability and write path

| Capability | Implementation | Doc |
| :--- | :--- | :--- |
| CH ack after durable write | `XAck` only after PG commit or CH spool `fsync` (D0/D1, D2) | [DATABASE.md](./DATABASE.md), [REMEDIATION.md](./REMEDIATION.md) |
| CH mmap spool | Rotating `events.wal.NNNN`, `CH_SPOOL_*` env | REMEDIATION §D2 |
| Processor write gates | `ProcessorPgGate`, `ProcessorChGate`; stream backpressure | REMEDIATION §7, MILESTONE §1.7 |
| PEL retention on PG outage | Retriable errors; no premature DLQ | `write_path_chaos_integration_test.go` |
| Graceful shutdown | E2E: HTTP 202 → row in `events` | M1 D6 |

### Budget and settlement

| Capability | Implementation |
| :--- | :--- |
| Single writer `current_spend` | `SyncWorker` + H1 mutex |
| Budget invariant | `AssertBudgetInvariant`; chaos A/C/F |
| Idempotency | `idempotency:click:*` Lua; `sync_idempotency` PG |
| Payment → management credit | Payment outbox → settlement gRPC (S1) |
| Tracker PG fallback off in prod | `TRACKER_PG_FALLBACK=0` (B2) |

### Security and perimeter

| Capability | Implementation |
| :--- | :--- |
| Auth client IP | gRPC metadata real IP (AUTH-P0) |
| Argon2 semaphore | `cryptoSem` on login/password (AUTH-P0) |
| Edge login rate limit | Nginx on `/admin/login` (EDGE-P1) |

### Redis / Lua (current production model)

| Capability | Implementation | Doc |
| :--- | :--- | :--- |
| Client sharding N=4 | `StaticSlotSharder`, Sentinel | [REDIS.md](./REDIS.md), README |
| Slot map + migration | PG `redis_slot_map`, fence, orchestrator worker | REDIS.md, management |
| Tier B/C Lua | `budget-fast.lua`, `unified-filter.lua` | REDIS.md |
| UDP ingress control | `:8190→:8191`, `IngressQuotaCell` | [EDGE.md](./EDGE.md) III |
| QuotaManager (cold) | PG chunk → `budget:quota` | REMEDIATION Q1 |

**Deferred (documented):** L1 full Lua/XADD split — [REMEDIATION.md](./REMEDIATION.md) §6.

---

## Milestone 2 — Invoicing and server-side admin API

**Status:** closed.

### `internal/adminapi`

Flat package; `register.go` mounts routes on management `:8188`. **No** `internal/ingestion` import on hot admin paths.

| Area | Routes / components |
| :--- | :--- |
| Billing JSON | `/api/v1/billing/*`, statements, preview, wallet, invariant, forecast, exports |
| Ops fan-out | `/api/v1/ops/incidents`, `shards`, `outbox`, `dlq`, cursor pagination |
| `FanOutCollector` | Parallel Redis/PG sources; `partial: true` |
| Export | `export_*.go` async jobs |

### `cmd/billing`

gRPC invoice generation from `balance_ledger` only; `PlaceholderProvider` (no Stripe in billing binary).

### Integration

InvoiceWorker → PDF → notifier delivery; `CheckLedgerBalanceInvariant` alert.

**Detail:** [ADMINISTRATIVE.md](./ADMINISTRATIVE.md), [MANAGEMENT.md](./MANAGEMENT.md).

---

## Milestone 3 — Commercial platform (core)

**Status:** core closed; admin report waves W1–W6 → deferred to M6.

### Licensing (`internal/licensing`, `cmd/license-server`)

| Capability | Doc |
| :--- | :--- |
| Ed25519 JWT, `LicenseWatcher`, grace/EXPIRED | [LICENSING.md](./LICENSING.md) |
| Hot path: `filterRejectLicenseExpired` | LICENSING §2 |
| `GET /api/v1/license/status` | `adminapi/licensing_*` |

### Subscriptions

| Capability | Doc |
| :--- | :--- |
| Plans basic / pro / enterprise | [SUBSCRIPTIONS.md](./SUBSCRIPTIONS.md) |
| `UPDATE_ENTITLEMENTS` outbox → Redis | SUBSCRIPTIONS |
| RPD gate `ingress:day:{customer}:{date}` | SUBSCRIPTIONS §21, MANAGEMENT |
| UDP `max_rps` from entitlements | SUBSCRIPTIONS |
| `usage_meters`, invoice overage | billing schema |

### Chaos

`scripts/chaos-drills/m3/` — license grace, spool, outbox proofs.

---

## Milestone 5 — Regulatory compliance (core)

**Status:** closed (optional CMP-DEF-03 tarpit deferred).

| Capability | Implementation | Doc |
| :--- | :--- | :--- |
| Defensive XDP | `cmd/edge-xdp`, `edge_filter.c` | [GUIDE_COMPLIANCE.md](../GUIDE_COMPLIANCE.md) |
| Allowlist gate | `allowlist.IsProtected` before block/BPF | M5 CMP-EBPF-* |
| Blocklist sync | `blocklist.Store.ApplyDiff`; skip protected | `internal/edge/blocklist` |
| Audit | `edge_block_audit` PG + same txn as blacklist | M5 |
| TLS impersonation worker | `TLSImpersonationWorker` (passive metadata) | management |
| CI compliance grep | `scripts/ci/check_compliance.sh` | mandatory |
| No ebpf in tracker/management | CMP-FORB-04 | CI |

**Detail:** [EDGE.md](./EDGE.md) Part V, MILESTONE M5 checklist.

---

## Current Redis sharding (until M4)

Production uses **fixed client-side sharding**, not Redis Cluster:

```
slot  = crc32_castagnoli(campaign_id) & 1023
shard = slot_table[slot]   // StaticSlotSharder, default slot % 4
```

| Mechanism | Role |
| :--- | :--- |
| Outbox | Global keys on all 4 shards |
| Slot migration | COPY + `migration_fence` + activate |
| Autoscale (basic) | `AutoscaleShards` — INFO metrics, 16 slots/event |
| Shard 0 convention | pub/sub `campaigns:update`, auth lockout |

**Target architecture (M4, exec #12 — after Arbitrage Ops pack):** [Shard Orchestrator](./MILESTONE.md#m4--shard-orchestrator--elastic-triplets-tier-xl-exec-12).

---

## Observability and CI (cross-cutting)

| Gate | Command / metric |
| :--- | :--- |
| Chaos proofs | `./scripts/chaos-drills/test_chaos.sh`, `CHAOS_MIN_PROOFS≥52` |
| Perf / alloc | `scripts/perf-gate/`, `make test-alloc-gate` |
| Compliance | `scripts/ci/check_compliance.sh` |
| Steady state | p99 < 80 ms, budget invariant (GUIDE_CHAOS R1) |
