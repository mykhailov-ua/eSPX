# Backlog

Open gaps and verification debt. Shipped work: [CAPABILITIES.md](./CAPABILITIES.md).

---

## Priority

| P | Theme | Status |
| :--- | :--- | :--- |
| P1 | Buyer UX / reporting | Open |
| P3 | CH query governance, backlog observability | Open |
| P4 | PII in CH, vendor telemetry | Open |
| P5 | Crypto gateway, Postgres DR, multi-region game days | Open |

---

## 1. RTB (R21–R31)

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-RTB-10 | Inventory expansion | Placement/domain targeting, creative-level auction, video/VAST |
| GAP-RTB-11 | Pre-auction caps | Daypart bitmasks, frequency-cap pre-check |
| GAP-RTB-12 | Platform RTB ops | CTV `gtax`, admin simulate, A/B cohorts, ARTF hooks, multi-region budget |

RTB live mode: admin gate shipped (`rtb_live_gate.go`, M7 R5). Production cutover blocked on GAP-RTB-10..12.

---

## 2. Sharding

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-SHARD-04 | Shard 0 convention | **Closed (M14):** global key fan-out, registry stale-serve, broker fallback, ingest reroute / `503 shard_unavailable`, alert + chaos drill |

Elastic triplets (M2), slot migration (M1), TCP cutover (M2), Lua consolidation (M9): shipped. Detail: [DATA.md](./DATA.md) Part I.

---

## 3. Wire parse

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-WIRE-03 | Deep nested JSON | **Closed (M14-06):** `MaxJSONDepth=16` on `skipJSONValue` / track parse; OpenRTB `OrtbMaxJSONDepth=32` |
| GAP-WIRE-04 | H2 incomplete garbage | **Closed (M14-07):** `H2_INCOMPLETE_MAX` → `gnet.Close` + `ad_h2_hostile_disconnect_total` |

Proofs: `wire_security_gap_chaos_test.go`, `http2_ingress_chaos_test.go`.

---

## 4. Operations

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-OPS-03 | CH admin query governance | `service_mab.go`, `ivtdetector/analyzer.go`, `marginguard/worker.go` bypass `CHQuery` |
| GAP-OPS-04 | Backlog observability | **Partial (M14-11):** fraud ring fill/pending + PEL age + Grafana; DLQ/spool unified dashboard still open |

`healthz`/`readyz` split: shipped on tracker (gnet + metrics port), processor, management (`internal/health`, `RegisterOpsRoutes`). Registry pub/sub uses `UpdateAndWarmCampaign` per campaign (`registry.go` `StartWatch`).

---

## 5. Product and admin UX

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-PROD-01 | Buyer / finance dashboards | Routes registered; most handlers return `501 NOT_IMPLEMENTED` (`dashboards_handlers.go`, `reports_handlers.go` scaffold) |
| GAP-PROD-03 | No OpenAPI | godoc only; deferred by design |

---

## 6. Geography and payments

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-GEO-01 | Multi-region game days | `RegionOutboxRelay`, D3 v2 dedup (M4), per-region RPD shipped; automated failover drills not productized |
| GAP-GEO-02 | Postgres DR | Runbook in [DATA.md](./DATA.md); no automated failover |
| GAP-PAY-01 | Crypto gateway | Stripe only; needs `CryptoProvider` + webhooks |

---

## 7. Data, fraud, compliance, engineering

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-DATA-01 | Raw PII in ClickHouse | `ip_address` in schema; hash pipeline + salt rotation |
| GAP-CMP-01 | Optional tarpit | **Partial (M14-08):** `EDGE_TARPIT_ENABLED` + `edge-tarpit.lua`; full compliance matrix still open |
| GAP-ENG-01 | Management monolith | ~190 flat Go files in `internal/management` |
| GAP-ENG-02 | Broker not in default compose | `cmd/broker` optional; not in `docker-compose.yaml` |
| GAP-ENG-03 | Vendor telemetry | Default off; opt-in bundle |

`interval_botnet` IVT rule (inter-arrival variance): shipped (`ivtdetector/interval_rule.go`). `usage_daily` flush: shipped (`UsageDailyFlushWorker`). Blacklist replication lag metric: shipped (`ad_blacklist_replication_lag_seconds`).

---

## 8. Database (optional tuning)

| ID | Gap | Notes |
| :--- | :--- | :--- |
| GAP-DB-01 | Logger group-commit fsync | `pkg/logger/flush_persist.go` |
| GAP-DB-02 | CH spool group-commit | Only if PEL retains unacked |
| GAP-DB-03 | Weighted processor gates | Metrics-driven `AcquireWeighted` |

Rules: [DATA.md](./DATA.md).

---

## 9. Suggested order

```text
GAP-RTB-10..12     → RTB P2 inventory/ops
GAP-PROD-01        → buyer dashboards (implement scaffold routes)
GAP-OPS-03/04      → CHQuery coverage; remaining backlog metrics (fraud slice closed M14-11)
GAP-DATA-01        → PII hashing in CH
GAP-PAY-01         → crypto gateway
GAP-GEO-01/02      → multi-region and Postgres DR automation
```

Chaos backlog: [CHAOS.md](./CHAOS.md).
