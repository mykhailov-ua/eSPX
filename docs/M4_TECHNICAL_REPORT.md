# M4 — Multi-Region Dedup Key Adapter (D3 v2) — Technical Report

**Date:** 2026-07-23  
**Scope:** M4-01..M4-15 (customer sync prefix boundary documented; quota/IVT remain prefix-keyed)  
**Artifacts:** `pkg/dedupkey`, `internal/dedup`, `internal/ingestion/migrations/00049_dedup_key_proposals.sql`, `dedup_claim_confirm` PG function

---

## 1. Summary

M4 replaces non-deterministic `uuid.New()` sync idempotency with **D3 v2**: a stable scope (SSID) plus two-factor proof (`factor_u` payload hash, `factor_d` PG receipt). `dedup_claim_confirm` runs claim+confirm in one round-trip before ledger/Redis/outbox side-effects. Crash recovery replays via `already_confirmed` + missing `sync_idempotency` row.

| ID | Status | Notes |
| :--- | :--- | :--- |
| M4-01 | Done | `pkg/dedupkey` — Scope, FormatCanonical, FactorU, golden vectors |
| M4-02 | Done | `dedup_key_proposals` table + unique SSID constraint |
| M4-03 | Done | `dedup_claim_confirm` + `dedup_format_key` — Go == PG golden vector test |
| M4-04 | Done | `DedupAdapter` + `SyncWorker.SetDedupAdapter` |
| M4-05 | Done | `RegionOutboxRelay` per-event SSID = `outbox_event_id` |
| M4-06 | Done | Optional `SET NX dedup/v2:{dedup_key}`; catalog prefix |
| M4-07 | Done | Pending TTL 24 h in PG function + `RejectStaleDedupProposals` |
| M4-08 | Done | `ad_dedup_proposal_total`, `ad_dedup_mismatch_total`, `ad_dedup_confirm_latency_seconds` |
| M4-09 | Done | `TestChaos_DedupCrashRecovery`, `TestChaos_DedupResumeApply` |
| M4-10 | Done | `hash_mismatch` never overwrites confirmed; table-driven PG test |
| M4-11 | Done | `source_epoch` from `control_plane_epochs` via `LoadRoutingEpoch` |
| M4-13 | Done | DATABASE.md §Idempotency scope boundary (customer/quota/IVT) |
| M4-14 | Done | Relay PG claim before `handleOutboxEvent` |
| M4-15 | Done | Broker PG consumer batch SSID from partition offsets |

---

## 2. Deduplication physics

Cross-region and async paths are **at-least-once**. Correctness requires idempotency keys that are **identical on retry** for the same logical unit of work.

### 2.1 Problem with UUID v4 per prepare cycle

`SyncWorker.prepareBudgetEntity` minted `uuid.New()` when `budget:txid:*` was empty. A worker restart mid-batch could produce a new `txID`, bypass `ON CONFLICT DO NOTHING` on `sync_idempotency`, and double-apply ledger rows.

### 2.2 D3 v2 model

```text
  USERSPACE                          POSTGRES
  ---------                          --------
  Build Scope (SSID)                 dedup_key_proposals
    region_id  ← REGION_CODE           UNIQUE(scope)
    source_id  ← worker lane           status: pending|confirmed|rejected
    source_epoch ← routing_epoch
    seq_start/end ← inflight gen,
                    outbox_event_id,
                    broker offsets
  factor_u ← SHA-256(canonical payload)
       │
       ▼
  dedup_claim_confirm(scope, factor_u) ──► confirmed + factor_d (UUID v4)
       │                                   already_confirmed
       │                                   hash_mismatch → reject
       ▼
  Apply side-effect (ledger / Redis / outbox handler)
       │
       ▼
  INSERT sync_idempotency(id = dedup_key) ON CONFLICT DO NOTHING
```

**Why two factors?**

- **factor_u** binds the *content* of the batch. Same SSID + different amounts → `hash_mismatch` (R4-01).
- **factor_d** is a *receipt* minted only after Postgres accepts the proposal. Userspace cannot forge a confirmed key without the DB row.

**Crash windows:**

| Crash point | Replay behavior |
| :--- | :--- |
| Before `dedup_claim_confirm` | Re-claim → `confirmed` |
| After confirm, before apply | `already_confirmed` → `NeedsResumeApply` → resume apply |
| After apply + `sync_idempotency`, before Redis commit | `already_confirmed` + idem exists → skip PG, `commitRollupRedis` |
| After full ack | `already_confirmed` → no-op |

### 2.3 Canonical key format

```text
v2|{region_id}|{source_id}|{source_epoch}|{seq_start}|{seq_end}|{factor_u}|{factor_d}
```

Go `FormatCanonical` and PG `dedup_format_key` produce identical strings (golden-vector tested).

---

## 3. Integration points

| Component | SSID | Claim timing |
| :--- | :--- | :--- |
| `SyncWorker` | `(shard, campaign, inflight_gen)` | Before `UpdateSpendBatch` |
| `RegionOutboxRelay` | `outbox_event_id` | Before `handleOutboxEvent` + optional Redis NX |
| `BrokerStreamConsumer` (PG) | `(partition, offset_start, offset_end)` | Before `StoreBatch` |

**Forbidden:** `pkg/dedupkey` / `internal/dedup` are not imported from `/track` or `FilterEngine`.

**Out of scope (M4-13):** customer sync, quota chunk reserve, IVT — keep existing prefix `sync_idempotency` keys.

---

## 4. Chaos test results

**Harness:** testcontainers-go (`postgres:16-alpine`, `redis:7-alpine`), Go 1.25, Linux 6.17

```bash
go test ./pkg/dedupkey/... -count=1
# ok   espx/pkg/dedupkey   0.003s

go test ./internal/dedup/... -count=1
# ok   espx/internal/dedup   0.027s

go test ./internal/ingestion/... -run 'Dedup|Sync' -short -count=1
go test ./internal/ingestion/... -run 'Chaos_Dedup' -count=1
# ok   espx/internal/ingestion   6.124s

go test ./internal/management/... -run 'RegionOutbox|Dedup' -short -count=1
go test ./internal/management/... -run 'Chaos_Dedup' -count=1
# ok   espx/internal/management   3.567s
```

| Test | Scenario | Result |
| :--- | :--- | :--- |
| `TestFormatCanonical_GoldenVector` | Same batch → same SSID + factor_u | PASS |
| `TestDedupClaimConfirm_GoMatchesPGFormat` | Go canonical == PG `dedup_format_key` | PASS |
| `TestDedupClaimConfirm_GoMatchesPGFormat` | Replay → `already_confirmed`; bad payload → `hash_mismatch` | PASS |
| `TestChaos_DedupCrashRecovery` | Replay sync after successful flush | PASS — spend unchanged, 1 idem row |
| `TestChaos_DedupResumeApply` | Confirm without idem → apply → replay no double spend | PASS |
| `TestChaos_DedupMultiRegionDuplicate` | 3× relay delivery of same outbox event | PASS — budget 8M, 1 proposal row |

### Chaos proof lines

```
chaos_proof fault=dedup_crash_recovery subsystem=sync_worker spend_unchanged=true sync_idem_rows=1 baseline_ok=true
chaos_proof fault=dedup_resume_apply subsystem=sync_worker spend_micro=125000 baseline_ok=true
chaos_proof fault=dedup_multi_region_duplicate subsystem=region_outbox_relay deliveries=3 proposal_rows=1 baseline_ok=true
```

---

## 5. Metrics

| Metric | Type | Labels | When |
| :--- | :--- | :--- | :--- |
| `ad_dedup_proposal_total` | Counter | `status` | Each `dedup_claim_confirm` outcome |
| `ad_dedup_mismatch_total` | Counter | — | `hash_mismatch` |
| `ad_dedup_confirm_latency_seconds` | Histogram | — | PG round-trip |

**Alert:** `ad_dedup_mismatch_total` increase > 0 — investigate corruption or topic reuse (R4-02).

---

## 6. Verification commands

```bash
go test ./pkg/dedupkey/... -count=1
go test ./internal/ingestion/... -run 'Dedup|Sync' -short
go test ./internal/management/... -run 'RegionOutbox|Dedup' -short
go test ./internal/ingestion/... ./internal/management/... -run 'Chaos_Dedup' -count=1
```

---

## 7. Files changed

| File | Change |
| :--- | :--- |
| `pkg/dedupkey/*.go` | D3 v2 scope, canonical format, payload hashing |
| `internal/dedup/adapter.go` | PG claim/confirm adapter + metrics |
| `internal/ingestion/migrations/00049_dedup_key_proposals.sql` | Table + `dedup_claim_confirm` |
| `internal/ingestion/dedup_sync.go` | SyncWorker integration |
| `internal/ingestion/sync_worker.go` | Dedup before batch flush |
| `internal/management/region_outbox_relay.go` | Claim-before-apply + Redis NX |
| `internal/ingestion/broker_consumer.go` | PG broker batch dedup |
| `cmd/management/main.go`, `cmd/processor/main.go` | Wire `DedupAdapter` |
| `docs/MULTI_REGION.md`, `docs/DATABASE.md` | Idempotency documentation |
