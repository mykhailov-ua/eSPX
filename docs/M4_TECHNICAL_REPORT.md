# M4 — Shard Orchestrator & Elastic Triplets: Technical Report

**Date:** 2026-07-20  
**Milestone:** M4 (Exec #12)  
**Status:** Shipped

---

## 1. Summary

Milestone M4 delivers dynamic Redis scaling beyond the static N=4 StaticSlot sharding topology through a dedicated Shard Orchestrator and Elastic Triplets. It implements robust, zero-allocation hot-path routing, Lua fencing via `routing_epoch` to prevent split-brain during migration, and false-sharing-padded capacity scoring.

---

## 2. Components Delivered

| ID | Component | Path | Notes |
|:---|:---|:---|:---|
| **M4.0** | **Standards envelope** | `internal/management/shard_orchestrator.go` | Implements ShardOrchestrator, padded EWMA counters, and migration flow. |
| **M4.1** | **Database Schema** | `internal/ingestion/migrations/00046_campaign_shard_assignment.sql` | Added `campaign_shard_assignment` table for Primary A/B and Reserve R tracking. |
| **M4.1** | **Queries & Mapping** | `internal/ingestion/queries/management.sql`, `budget.sql` | Added upsert/get queries and updated `GetCampaignFull` and `ListActiveCampaigns` to LEFT JOIN with assignments. |
| **M4.2** | **Hot-path Triplet Routing** | `internal/ingestion/unified_filter.go`, `budget_fast.go` | Modulo 100 composite hash routing across Primary A/B (40/40) and Reserve R (20%). |
| **M4.2** | **Lua Fencing** | `internal/ingestion/unified-filter.lua`, `budget-fast.lua` | Fences debits during copy: `if redis_epoch != routing_epoch then return fenced end`. |
| **M4.3** | **Capacity Scoring** | `internal/management/shard_orchestrator.go` | EWMA capacity tracking ($C_{\text{ema}} \leftarrow \text{EWMA}(C_{\text{raw}}, 60\text{s})$), scale-out threshold (0.85), 300s overload limit, and 3600s cooldown. |
| **M4.4** | **Chaos Tests** | `internal/management/shard_orchestrator_chaos_test.go` | Implements `TestChaos_SO_NoFalseMigrate` and `TestChaos_SO_CampaignRoutingMigration`. |

---

## 3. Capacity Scoring & Scaling Logic

The Shard Orchestrator tracks Redis shard capacity using an Exponentially Weighted Moving Average (EWMA) with a smoothing factor $\alpha = 0.15$ (representing a ~60s window with a 10s interval):

$$C_{\text{ema}} \leftarrow \alpha \cdot C_{\text{raw}} + (1 - \alpha) \cdot C_{\text{ema}}$$

- **Overload Detection:** Triggered when $C_{\text{ema}} \ge 0.85$ for 300 seconds.
- **Quorum Gate & Cooldown:** Cooldown period of 3600 seconds is enforced between consecutive scaling operations to prevent migration thrashing.
- **False Sharing Prevention:** EWMA fields are padded with `_ [56]byte` to align to 64-byte cache line boundaries, preventing Level 1/Level 2 cache line invalidations across cores.

---

## 4. Test Results

### 4.1 Chaos Matrix Proofs

```bash
go test -v ./internal/management/... -run 'TestChaos_SO_'
```

```
=== RUN   TestChaos_SO_NoFalseMigrate
    shard_orchestrator_chaos_test.go:78: chaos_proof fault=orchestrator_no_false_migrate subsystem=shard_orchestrator max_ema=0.40 threshold=0.85 false_migrate=false
--- PASS: TestChaos_SO_NoFalseMigrate (3.26s)
=== RUN   TestChaos_SO_CampaignRoutingMigration
    shard_orchestrator_chaos_test.go:163: chaos_proof fault=campaign_routing_migration subsystem=shard_orchestrator source_shard=0 target_shard=1 migration_success=true keys_drained=true
--- PASS: TestChaos_SO_CampaignRoutingMigration (2.80s)
PASS
ok  	espx/internal/management	6.083s
```

```bash
go test -v ./tests/chaos/... -run TestChaos_Shard0Outage
```

```
=== RUN   TestChaos_Shard0Outage
    shard_outage_chaos_test.go:162: chaos_proof fault=shard_0_outage status=recovered shards_123_ok=true outbox=processed
--- PASS: TestChaos_Shard0Outage (9.66s)
PASS
ok  	espx/tests/chaos	9.684s
```

### 4.2 Allocation Gate & Performance Benchmarks

```bash
make test-alloc-gate
```

```
go test -short -count=1 -run 'ZeroAlloc|zeroAlloc_fraudScoring|FraudScoring_LatencySLA|ApplyRtbAuction_shadow_zeroAlloc|RecordRtbShadow' ./internal/ingestion/...
ok  	espx/internal/ingestion	0.039s

go test -run='^$' -bench='BenchmarkAuction$' -benchmem -count=1 ./internal/rtb/
BenchmarkAuction-12    	84832290	        14.28 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	espx/internal/rtb	1.697s
```

```bash
go test -run='^$' -bench='StaticSlotSharder' -benchmem -count=1 ./internal/ingestion/...
```

```
BenchmarkStaticSlotSharder_10-12      	209201756	         5.528 ns/op	       0 B/op	       0 allocs/op
BenchmarkStaticSlotSharder_1024-12    	218344586	         5.651 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	espx/internal/ingestion	3.561s
```

---

## 5. Criterion Coverage

| Test | Criterion | Result |
|:---|:---|:---|
| `TestChaos_SO_NoFalseMigrate` | Orchestrator does not migrate under healthy load | **PASS** |
| `TestChaos_SO_CampaignRoutingMigration` | Orchestrator triggers Triplet migration under overload | **PASS** |
| `TestChaos_Shard0Outage` | Ingestion continues on shards 1-3 when shard 0 is down | **PASS** |
| `BenchmarkStaticSlotSharder` | `GetShard` still 0 allocs/op and ~5.6 ns/op after routing table change | **PASS** (0 allocs, 5.5 ns) |
| `make test-alloc-gate` | Zero heap allocation hot path | **PASS** (0 allocs) |
