# ANTIFRAUD — IVT architecture и rollout

Server-side protection via pixel/redirect. Opt-in JS only. **Perimeter only** — detection, filtering, rate limit, IVT tagging, blocklist export. Без hack-back, poisoning, auto law-enforcement.

Hot path: `internal/ads` + `unified-filter.lua`. Cold path: processor, management, ClickHouse.

## RULES

### Three layers

| Layer | Latency | Что |
| :--- | :--- | :--- |
| Hot | <1 ms, 0 allocs | High-confidence sync filters (tracker + 1× Redis Lua) |
| Warm | ingest-time | Rate limits, velocity, FP collision (Redis counters) |
| Cold | minutes | Anomalies, clustering, blocklist refill (CH, workers) |

**Запрещено:** sync hot path → ClickHouse.

### L1 / L2 / L3 cascade

- **L1 Auto-Reject:** ≥2 независимых high-confidence сигнала → `fraud_accept`, без charge.
- **L2 Shadow:** один слабый сигнал → shadow log, event в main stream с flag.
- **L3 Quarantine:** cold-path anomaly → `blacklist:fraud` → perimeter + Go L1.

### fraud_score tiers (0–100)

| Score | Поведение |
| :--- | :--- |
| 0–30 | Normal billing |
| 31–60 | L2 shadow, bid deflation, 1 imp/day fcap, 429 on excess |
| 61–80 | L1 reject или Ghost bucket; no S2S; 403/429/neutral pixel |
| 81–100 | Edge block 403/429, blocklist, Retry-After; no tarpit/holds |

Ghost tier требует `ghost_ivt_enabled` в контракте кампании.

`StatusGhostAccept`: fraud_stream + `ghost_events` CH; **no budget**, **no S2S**; HTTP neutral 204/GIF/403.

### Scope exclusions

Zip bombs, proxy exhaustion, RST flapping, Reaper auto-abuse, IC3/Spamhaus automation, cross-redirect botnets — **out of scope**.

### Hot path constraints

Любой код на ingestion path: **0 allocs/op** в bench (`go test -benchmem ./internal/ads/...`).

Запрещено: reflection, `encoding/json`, `fmt.Sprintf` для ключей, interface boxing, channels, defer в hot loops.

Парсинг: byte-slice walks / DFA (`requests_parse.go`). Redis keys через `bufPool`. Один `EvalSha` на событие.

SLA: p99 filter overhead <500 µs; FPR <0.1% (shadow before enforce). Критерии приёмки — [PERF.md](PERF.md).

### Practices

| ID | Practice | Priority | JS |
| :--- | :--- | :---: | :---: |
| A | Server-side behavioral (imp/r pixel) | P0 | No |
| B | Temporal coherence (TTC, velocity) | P0 | No |
| C | Client engagement (scroll, dwell) | P2 | Opt-in |
| D | Honeypots on owned forms | P2 | HTML |
| E | Device integrity / FP collision | P1 | No |
| F | Attribution (HMAC-S2S, dedup) | P1 | No |
| G | Inventory (Referer, MFA domains) | P1 | No |
| H | Supply chain (ads.txt) | P2 | No |
| I | Economic firebreaks (fcap, deflation) | P0 | No |
| J | Verified conversion cost (VCC) | P3 | Partial |
| K | Blocklist export (Pub/Sub) | P2 | No |
| L | Protobuf validation / teleport | P1 | No |
| M | TLS JA3/JA4 | P2 | No |
| N | CH statistical anomalies | P1 | No |
| O | Rate limiting / throttling | P1 | No |
| P | Ghost IVT bucket | P0 | No |
| Q | Proof-of-work gate | P2 | Yes |
| R | Honeytoken decoy paths | P2 | No |

Perimeter (O): 429 + Retry-After; sub-2 s delay только при score ≥81 + 2 L1 signals. CDN/mobile ASN whitelist.

## SHIPPED

FraudFilter + MaxMind, TTC в Lua, SET NX dedup, IP rate limit, GeoFilter, budget+fcap Lua, edge blacklist, fraud_stream MPSC, composite routing hash. `fraud_reason` в CH.

Proto-поля `fraud_score`, `ghost_event` добавлены; **не проведены через FilterEngine**.

## TODO

Порядок: **Phase 0 → 1 → 2 → 3 → (4∥5) → 6**. Gates — [PERF.md](PERF.md).

### Phase 0 — scoring core

1. Wire `fraud_score`, `fraud_reason`, `ghost_event` через FilterEngine → `fraud_stream` / CH writer.
2. `domain.Campaign`: fraud thresholds, `ghost_ivt_enabled`, behavior flags. Migration + registry sync (`campaigns:update` outbox).
3. `filter_context.go`: fraud_score accumulator; tier mapping в `filter_errors.go`.
4. Wire L1/L2/L3 в `FilterEngine` после individual filters.
5. Metrics: `ads_fraud_score_histogram`, `ads_fraud_tier_total`, `ads_fraud_reason_total`, `ads_l1_reject_total` — pre-bound labels.
6. Benchmarks: 0 allocs/op с scoring; latency <500 µs.

### Phase 1 — behavioral + Ghost

7. Routes: `GET /track/imp` (43-byte GIF/204, `espx_sid`, `imp_ts`); `GET /track/r` (TTC, 302). Zero-alloc URI parse.
8. Tracker filters: `beh_conv_fast`, `beh_seq_gap`, `beh_dwell_prx` (session_first_seen в Redis).
9. Ghost: `StatusAccept`, `StatusReject`, `StatusGhostAccept`; gate на `ghost_ivt_enabled`. `skip_budget` в Lua для Ghost.
10. Suspect tier (31–60): bid deflation hook; 1 imp/day fcap override в `unified-filter.lua`.
11. Lua behavioral (Practice B):
    - `beh_no_imp` — click без `imp_ts` when `TTC_FAIL_CLOSED`
    - `beh_vel_ip` — events per IP per 60 s
    - `beh_vel_user` — clicks per user-campaign per 1 h
    - return codes + Go mapping в `unified_filter.go`
12. Processor: consumer `ad:fraud:stream`; `ghost_events` table + ingest path.

### Phase 2 — device, attribution, validation

13. `DeviceFilter`: Client Hints vs UA; `FP_Hash = CRC32(UA + Accept-Language + IP/24)`.
14. `AttributionFilter`: fast conv, click_id dedup, HMAC-S2S `subtle.ConstantTimeCompare`.
15. `RefererFilter`: zero-copy host parse; sorted domain hash blocklist.
16. Protobuf validation: coordinate bounds, type enum, teleport (>1000 km/h).
17. `connContext.rate_limit_tier`; sliding window by IP, FP, campaign.

### Phase 3 — cold path analytics

18. CH schemas с `fraud_score`; MV для CTR/TTC anomalies per campaign (`deploy/clickhouse/`).
19. `BehaviorThresholdWorker`: MV aggregates → thresholds → Redis config → outbox replicate.
20. L3 quarantine: flag IP/FP/domain → `SADD blacklist:fraud` all shards; Pub/Sub `fraud:quarantine` → perimeter cache flush.
21. Fraud dashboard HTMX: IVT rate, `fraud_reason` breakdown, ghost vs billed.

### Phase 4 — supply chain + export

22. `AdsTxtCrawler`: fetch ads.txt, validate sellers, cache Redis, inventory signals.
23. `GET /admin/fraud/blocklist`: aggregated IP/domain export; RBAC `PermFraudRead`; DPA format.

### Phase 5 — client signals + VCC

24. `beh-track.js` <2 KB; DFA parser → `BehaviorSignals` stack struct, 0 allocs/op.
25. PoW gate: X-PoW-Challenge, SHA-256 verify <1 µs.
26. Honeytoken `/track/decoy/...`, max 10 redirects → 403.
27. VCC: conversion → pending row PG; worker validates cold signals; charge on confirm; ghost on fail.

### Phase 6 — incident response

28. Auto: L3 blocklist enqueue via outbox, Grafana/ops alert.
29. Admin: incident queue, evidence pack JSON export, manual blocklist + audit log.
30. `incident_log` / `security_incidents` в CH/PG для IOC packs (score ≥81).

### Tests

`TestUnifiedFilter_Behavior`, `TestGhostIVT_NonBilling`, `TestAttributionFilter`, `TestRefererFilter` — bench 0 allocs/op.

## ECONOMIC

Savings ≈ budget × IVT% × (1 − FPR) − platform cost. FPR target <0.1%. Typical IVT reduction 18–40% (A/B per campaign).

## INCIDENT

Score ≥81 → cold path IOC pack (IP, JA4, timestamps, samples); auto L3 blocklist; ops alert. Manual CERT-UA / abuse@hoster after review.

## FILES

`internal/ads/filters.go`, `filter_context.go`, `unified-filter.lua`, `unified_filter.go`, `fraud_stream_queue.go`, `internal/ads/processor.go`, `internal/management/service_fraud.go`, `deploy/clickhouse/`
