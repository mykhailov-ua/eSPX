# Distributed Quotas — request lifecycle (Phase 1)

End-to-end path for budget enforcement with **Postgres control plane** + **Redis local quota** (`REDIS.md` Phase 1). Phase **1.1** (this doc’s PG reserve path) is implemented; tracker Lua refill (1.3+) and QuotaManager (1.4) are the next wiring steps.

## Architecture snapshot

```
Client → Nginx (:8180) → Tracker (:8181+) → Redis shard (EvalSha unified-filter.lua)
                              ↑ cold                    ↑ budget:quota:{cid}
                              │                         │
                         QuotaManager (management) ──────┘
                              │
                         Postgres (campaigns + campaign_quotas)
```

**Routing invariant:** `shard = StaticSlotSharder(campaign_id)` only — no `user_id` shard routing for money.

---

## 1. Happy path — accepted `/track` (hot path)

| Step | Component | Action | Typical latency |
| :---: | :--- | :--- | :--- |
| 1 | Client → Nginx | TLS terminate, edge phase-1/2 (IP, RL, DFA parse) | 0.5–2 ms |
| 2 | Nginx → Tracker | HTTP/1.1 POST `/track` (protobuf) | 0.1–0.5 ms LAN |
| 3 | Tracker (gnet) | Parse, Go filters (geo, schedule, fraud, L3) | p99 < 20 ms |
| 4 | Tracker → Redis | **1× EvalSha** `unified-filter.lua` on campaign shard | p99 < 15 ms |
| 5 | Lua | `DECRBY budget:quota:{cid}` (Phase 1.3+), dedup, fcap, stream XADD | in-script |
| 6 | Tracker → Client | 302 / protobuf response | — |

**Network overhead (hot path):** one Redis round trip per accepted/rejected event. No Postgres on the tracker SLA path (`/track` p99 < 80 ms).

**Disk (hot path):** none on tracker or Redis master for steady-state quota debit (in-memory Redis + AOF async).

---

## 2. Refill path — quota low (cold path)

Triggered when Lua sees `budget:quota:{cid}` below refill threshold (Phase 1.3):

| Step | Component | Action |
| :---: | :--- | :--- |
| A | Lua | `SET budget:refill_lock:{cid} 1 NX EX 10`; on OK → `SADD budget:refill_needed {cid}` |
| B | QuotaManager | Poll refill queue / outbox |
| C | QuotaManager → Postgres | **`QuotaRepo.ReserveChunk`** (transaction below) |
| D | QuotaManager → Redis | `INCRBY budget:quota:{cid}` on campaign shard; `DEL budget:refill_lock:{cid}` |
| E | SyncWorker (async) | Flush spend → `campaigns.current_spend`; decrease `campaign_quotas.reserved_amount` (Phase 1.5) |

**Thundering herd:** only the Lua caller that wins `refill_lock` enqueues one refill task per ~10 s window.

---

## 3. Postgres reserve transaction (Phase 1.1)

Implemented in `internal/ads/quota_repo.go` → `ReserveChunk`.

### 3.1 Transaction steps

```text
BEGIN;
  INSERT INTO sync_idempotency (id) VALUES ('quota:' || $idempotency_key)
    ON CONFLICT DO NOTHING;
  -- if conflict → COMMIT early (idempotent retry)

  SELECT budget_limit, current_spend FROM campaigns WHERE id = $cid FOR UPDATE;
  SELECT reserved_amount FROM campaign_quotas
    WHERE shard_id = $shard AND campaign_id = $cid FOR UPDATE;

  IF current_spend + reserved + chunk > budget_limit → ROLLBACK (ErrQuotaBudgetExceeded);

  INSERT ... OR UPDATE campaign_quotas SET reserved_amount += chunk;
COMMIT;
```

**Financial invariant:**

$$\text{current\_spend} + \text{reserved\_amount} + \text{chunk} \le \text{budget\_limit}$$

`reserved_amount` = micro-units issued to Redis but not yet in `current_spend` (async sync lag).

### 3.2 EXPLAIN ANALYZE (reference plan, PG 16)

Run on a seeded campaign (replace UUIDs):

```sql
BEGIN;

EXPLAIN (ANALYZE, BUFFERS, WAL)
INSERT INTO sync_idempotency (id) VALUES ('quota:bench-1') ON CONFLICT DO NOTHING;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT budget_limit, current_spend FROM campaigns
WHERE id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid FOR UPDATE;

EXPLAIN (ANALYZE, BUFFERS, WAL)
SELECT shard_id, campaign_id, reserved_amount, chunk_size, updated_at
FROM campaign_quotas
WHERE shard_id = 2 AND campaign_id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid
FOR UPDATE;

EXPLAIN (ANALYZE, BUFFERS, WAL)
INSERT INTO campaign_quotas (shard_id, campaign_id, reserved_amount, chunk_size)
VALUES (2, 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid, 5000000, 5000000);

ROLLBACK;
```

**Expected plan shape:**

| Statement | Access method | Notes |
| :--- | :--- | :--- |
| `sync_idempotency` INSERT | Index Unique Scan on `sync_idempotency_pkey` | PK conflict check; ~1 page |
| `campaigns` FOR UPDATE | Index Scan on `campaigns_pkey` | Single heap tuple; row lock |
| `campaign_quotas` FOR UPDATE | Index Scan on `campaign_quotas_pkey (shard_id, campaign_id)` | Or Index Only miss → heap fetch |
| `campaign_quotas` INSERT | Index Scan + Insert on PK | First chunk for campaign |

Typical single-shard reserve (warm cache, SSD): **~0.3–1.5 ms** CPU + **1× fsync** on commit (`synchronous_commit=on` → +0.5–3 ms depending on storage).

### 3.3 Disk read/write model

| Phase | Read | Write |
| :--- | :--- | :--- |
| Idempotency insert | PK index page (shared buffers hit after warmup) | PK heap + WAL record |
| `campaigns` lock | PK index → heap tuple | row-level lock (no tuple version until sync) |
| `campaign_quotas` lock/upsert | PK or seq scan (miss) | heap tuple + PK index + WAL |
| COMMIT | — | WAL flush (group commit batches under load) |

**No sequential scans** at steady state: PK on `campaigns.id`, composite PK on `(shard_id, campaign_id)`, secondary `idx_campaign_quotas_campaign_id` for admin/recon only.

**Async lag window:** between Redis debit and SyncWorker updating `current_spend`, overspend bound ≈ `N × chunk_size` (Phase 1.7 tests) — PG invariant prevents issuing chunks beyond `budget_limit`.

---

## 4. Network overhead summary

| Link | Protocol | Payload | When |
| :--- | :--- | :--- | :--- |
| Client ↔ Nginx | HTTPS | `/track` body ≤8 KB | Every request |
| Nginx ↔ Tracker | HTTP/1.1 | Protobuf event | Every request |
| Tracker ↔ Redis | RESP | EvalSha ~12 keys, ~24 args | Every filter (1 RTT) |
| QuotaManager ↔ Postgres | pgx/TCP | Reserve tx ~200 B | Refill only (~≤1 per 10 s per hot campaign) |
| QuotaManager ↔ Redis | RESP | INCRBY + DEL lock | After successful reserve |
| SyncWorker ↔ Postgres | pgx | Batch spend delta | Periodic (100 ms–10 s tuning) |

**Design goal:** refill is **cold path** — must not appear on tracker hot path. Hot campaign local block (Phase 1.6) sheds Redis RPS before refill storms.

---

## 5. Idempotency (Phase 1.1.4)

- Keys: `sync_idempotency.id = 'quota:' || idempotency_key` (UUID from QuotaManager).
- Retry with same key → `AlreadyApplied=true`, `reserved_amount` unchanged.
- Same semantics as `CampaignRepo.UpdateSpend` + `sync_idempotency` for SyncWorker.

---

## 6. Schema

```sql
CREATE TABLE campaign_quotas (
    shard_id        SMALLINT NOT NULL,
    campaign_id     UUID NOT NULL,
    reserved_amount BIGINT NOT NULL DEFAULT 0,
    chunk_size      BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, campaign_id)
);
CREATE INDEX idx_campaign_quotas_campaign_id ON campaign_quotas (campaign_id);
```

Migration: `internal/ads/migrations/00027_campaign_quotas.sql`.

---

## 7. Code map

| Piece | Location |
| :--- | :--- |
| Reserve transaction | `internal/ads/quota_repo.go` |
| Shard derivation | `CampaignShardID()` in `quota_repo.go` / `sharding.go` |
| sqlc queries | `internal/ads/queries/quota.sql` |
| Tests | `internal/ads/quota_repo_test.go` |
| Next: QuotaManager | `internal/management/quota_manager.go` (Phase 1.4) |
| Next: Lua quota keys | `unified-filter.lua` (Phase 1.3) |

---

## Related

- `REDIS.md` — ROADMAP Phase 1
- `docs/redis-4-shards.md` — topology, hot-shard observability (Phase 0)
- `docs/redis-quota-lua-phase1-3.md` — Lua quota keys, network/blocking report (Phase 1.3)
- `REDIS.md` — money micro-units, outbox; `ANTIFRAUD.md` — VCC
