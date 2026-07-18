# ESPX-LP-2026-V1: Hybrid Volume Licensing (Proposal)

| Field | Value |
| :--- | :--- |
| **Document ID** | ESPX-LP-2026-V1 |
| **Status** | **PROPOSAL** — не реализовано, не входит в M6 DoD |
| **Domain** | Product license, prepaid volume, feature-tier monetization |
| **Canonical spec** | [LICENSING.md](../LICENSING.md), [SUBSCRIPTIONS.md](../SUBSCRIPTIONS.md), [MANAGEMENT.md](../MANAGEMENT.md) §18–21 |

Этот документ — **опциональная эволюция** коммерческой модели поверх уже описанного non-blocking license server и tenant-подписок. Если proposal не принят, остаётся текущая схема: RPS + RPD + `max_events_per_month`, EXPIRED → fail-closed на track после grace.

---

## 1. Executive summary

Предлагается **гибридная prepaid volume + feature-tier** модель по аналогии с LLM-дистрибьюторами 2026 (OpenAI, Google Cloud, Anthropic):

- Монетизация смещается с жёсткого **peak-RPS** как главного коммерческого рычага на **prepaid monthly volume** (billable events) + feature flags по «слоям» движка.
- **RPS/RPD** остаются как защита инфраструктуры (burst + дневной потолок), не как единственный тарифный параметр.
- Вся верификация license, учёт billable volume и сравнение с квотой — **cold path** (`management`, processor, CH aggregates). Hot path (`/track`, gnet) — только локальный snapshot, 0 сети к License Hub.

Архитектура остаётся **on-premise**: данные клиента не покидают DC; heartbeat — только агрегаты при opt-in telemetry.

---

## 2. Соответствие текущему eSPX (mapping)

| Идея proposal | Уже в коде / docs | Gap (если принять proposal) |
| :--- | :--- | :--- |
| Ed25519 signed JWT | [LICENSING.md](../LICENSING.md) §4 | Расширить claims: `volume_quota_monthly`, `tier_level`, billable weights |
| Non-blocking hot path | `UDPControl`, registry snapshot, M6 | Без изменений |
| RPD (requests/day) | [MANAGEMENT.md](../MANAGEMENT.md) §21 | Остаётся ingress-лимит, не billable unit |
| `usage_meters` / month | SUBSCRIPTIONS, billing | Становится **billable_events**, не raw requests |
| eBPF/XDP L4 drop | `deploy/edge/xdp`, M5 profile `edge_xdp` | Opt-in edge; zero-cost layer |
| Lua short-circuit | `unified-filter.lua` return codes | Нужен cold-path учёт «discounted reject» |
| ivt-detector / fraud-scorer | cold path CH → outbox | «Intelligence tier» feature gate |
| OpenRTB | `internal/ingestion` RTB path | «Auction tier» feature gate |
| EXPIRED fail-closed | LICENSING §7.1 | **Конфликт:** proposal предлагает soft overage; см. §7 |

---

## 3. Architectural rules (unchanged)

1. **Zero-latency injection:** `/track` и RTB hot path не вызывают license server, billing gRPC, Postgres subscription read.
2. **Data sovereignty:** в heartbeat запрещены IP, click_id, ledger amounts, campaign payloads ([LICENSING.md](../LICENSING.md) §8).
3. **Offline resilience:** `ESPX_LICENSE_MODE=file` + last-known-good JWT; кластер работает без License Hub до конца grace / policy breach.
4. **Graceful degradation:** при превышении volume — **алерт и overage billing**, не мгновенный hard stop ingest (опциональная политика; см. §7).

---

## 4. License payload (расширение JWT)

Совместимо с текущим JWT ([LICENSING.md](../LICENSING.md)); добавляются поля proposal. Один тип `Entitlements` в `internal/licensing/`.

```json
{
  "iss": "espx-license",
  "sub": "lic_983471029384",
  "kid": "2026-07",
  "deployment_id": "uuid",
  "customer_name": "AdNet Dubai LLC",
  "issued_at": "2026-07-18T12:00:00Z",
  "valid_until": "2027-07-18T12:00:00Z",
  "grace_days": 14,

  "tier_level": 3,
  "volume_quota_monthly": 100000000000,
  "volume_overage_rate_micro": 1500,
  "volume_soft_ceiling_pct": 115,

  "limits": {
    "max_rps": 200000,
    "max_requests_per_day": 5000000000,
    "max_active_campaigns": 5000,
    "max_regions": 4,
    "max_redis_masters": 6
  },

  "features": {
    "ingestion_gnet": true,
    "openrtb_engine": true,
    "ivt_ml_detector": true,
    "ebpf_xdp_edge": true,
    "ml_fraud_boost": true,
    "multi_region": true
  },

  "billable_weights": {
    "accepted_event": 1.0,
    "lua_dedup_fcap_reject": 0.1,
    "ebpf_l4_drop": 0.0
  },

  "bind": {
    "mode": "soft",
    "allowed_interfaces_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  },

  "support_tier": "pagerduty_sla"
}
```

| Поле | Назначение |
| :--- | :--- |
| `tier_level` | 1–3 prepaid commercial band (см. §5.3); не путать с tenant `basic/pro/enterprise` |
| `volume_quota_monthly` | Prepaid billable units / month (не raw HTTP requests) |
| `billable_weights` | Множители для discounted events (§4.2) |
| `max_redis_masters` | Hardware footprint cap (bind к topology) |
| `volume_soft_ceiling_pct` | Порог «hard breach» policy (default 115%) |

Tenant subscription JWT/PG по-прежнему задаёт потолок **внутри** deployment: `Effective(license, subscription)`.

---

## 5. Адаптации из LLM-провайдеров

### 5.1 Feature-based tiering (Anthropic-style)

Ветвление **не по HTTP**, а по включённым подсистемам в entitlement snapshot:

| Layer | eSPX component | Feature flag | «Tier» metaphor |
| :--- | :--- | :--- | :--- |
| **Ingestion** | gnet tracker, Redis Lua budget/fcap | `ingestion_gnet` (always on) | Flash / Haiku |
| **Auction** | OpenRTB live + shadow | `openrtb_engine` | Pro / Sonnet |
| **Intelligence** | ivt-detector, fraud-scorer, CH ML batch | `ivt_ml_detector`, `ml_fraud_boost` | Reasoning / Opus |

Cold path: `RequireFeature("openrtb_engine")` на RTB admin mutations. Hot path: registry блокирует `RtbMode=live` без флага (уже в [SUBSCRIPTIONS.md](../SUBSCRIPTIONS.md)).

### 5.2 Billable event discounts (OpenAI context-cache analogy)

Цель: не наказывать клиента за ботов, dedup и fcap — снизить billable volume при «дешёвой» обработке.

**Формула (cold path, hourly roll-up):**

```text
billable_units = Σ (event_count[type] × weight[type])

где type ∈ {
  accepted,           weight = 1.0   (успешный ingest → stream)
  lua_short_circuit,  weight = 0.1   (dedup, fcap, rate reject в unified-filter)
  ebpf_drop,          weight = 0.0   (XDP_DROP, не дошло до userspace)
}
```

**Hot path:** не считает billable units (слишком дорого). Считает только:

- RPD counter (`ingress:day:*`) — requests
- optional lightweight counter `volume:raw:day:*` для ops

**Cold path** (`VolumeMeterWorker`, hourly):

1. ClickHouse / PG: `COUNT` по `filter_reject_reason` + edge drop metrics.
2. Применить `billable_weights` из license snapshot.
3. UPSERT `usage_meters` meter=`billable_events`, period=month.

Lua mapping (пример):

| Lua return / path | billable class |
| :--- | :--- |
| Accept + stream write | `accepted` × 1.0 |
| Dedup hit | `lua_short_circuit` × 0.1 |
| FCAP exceeded | `lua_short_circuit` × 0.1 |
| eBPF XDP_DROP | `ebpf_drop` × 0.0 (metric from edge) |

Требует: тегировать reject reason в CH `fraud_events` / events metadata (backlog).

### 5.3 Prepaid account tiers (Google Cloud-style)

Коммерческие **deployment bands** (license `tier_level`), ортогонально tenant Basic/Pro/Enterprise:

| tier_level | Name | volume_quota_monthly (guideline) | Features | Support SKU |
| :--- | :--- | :--- | :--- | :--- |
| **1** | Evaluator | 1B billable units | gnet + basic IVT | dev ticket |
| **2** | Growth | 20B | + OpenRTB + RPD 10B/day | private Slack |
| **3** | Enterprise | 100B+ | + eBPF edge + ML tier + multi-region | PagerDuty SLA |

Support — поле `support_tier` в JWT; не влияет на hot path.

---

## 6. Local enforcement pipeline (cold path)

```text
[ HOT PATH — no license HTTP ]

  Edge XDP (optional) ──► L4 drops (weight 0) ──► metric edge_xdp_drops_total
           │
           v
  gnet /track ──► UDP RPS gate ──► RPD Redis gate ──► FilterEngine + Lua
           │                              │
           │                              └── lua reject reason → CH column
           v
  Redis Stream ──► processor ──► PG events + CH batch

[ COLD PATH — hourly / daily ]

  VolumeMeterWorker:
    CH + Prometheus ──► billable_units MTD
    compare license.volume_quota_monthly
    UPDATE license_status + usage_meters
    emit metrics + alerts

  LicenseWatcher (existing):
    JWT verify, grace state, entitlement snapshot → Redis
```

**management** не блокирует ingest синхронно; только публикует flags в snapshot для **опциональных** degradations (§7).

---

## 7. Overage: soft / hard breach (proposal policy)

**Отличие от canonical [LICENSING.md](../LICENSING.md):** там `EXPIRED` после grace → fail-closed на track. Proposal вводит **volume overage** отдельно от **calendar expiry**.

| Zone | % of prepaid volume | Hot path behavior | Business |
| :--- | :--- | :--- | :--- |
| **Normal** | 0–80% | Full features per tier | — |
| **Warning** | 80–100% | Full; `X-License-Volume-Warn: 0.92` header on admin API | Grafana alert |
| **Soft breach** | 100–115% | Ingest **continues**; `espx_license_volume_breach=1`; overage invoice rate | Alertmanager + UI banner |
| **Hard breach** | >115% OR payment 14d overdue | **Degrade**, not shutdown: disable ML IVT batch scoring, fraud-scorer sync; RTB → shadow-only | Protect host RAM; клиент остаётся на ingest |
| **Calendar EXPIRED** | JWT grace ended | Canonical fail-closed (`filterRejectLicenseExpired`) | License renewal required |

Политика выбирается в JWT: `enforcement_mode: strict|hybrid` (default `strict` для совместимости с M6).

```json
"enforcement_mode": "hybrid",
"volume_soft_ceiling_pct": 115,
"hard_breach_actions": ["disable_ml_ivt", "rtb_shadow_only"]
```

Hard breach **не** останавливает tracker; снимает дорогие cold-path consumers и optional hot-path ML reads — согласовано с «graceful failure» из proposal.

---

## 8. Telemetry & privacy

Разрешено в heartbeat (как сейчас + proposal aggregates):

```json
{
  "license_id": "uuid",
  "deployment_id": "uuid",
  "tier_level": 3,
  "billable_units_mtd": 18400000000,
  "volume_quota_monthly": 20000000000,
  "volume_pct": 0.92,
  "optional_metrics": {
    "ebpf_drops_24h": 1200000,
    "lua_discount_events_24h": 45000000
  }
}
```

Запрещено: IP, geo per user, financial amounts, campaign names, click_id.

100% on-premise режим: `ESPX_LICENSE_TELEMETRY=0` — volume reconciliation только локально в PG/CH.

---

## 9. Data model (extensions)

```sql
-- billing / public (proposal)
ALTER TABLE license_status ADD COLUMN IF NOT EXISTS
    billable_units_mtd BIGINT NOT NULL DEFAULT 0,
    volume_quota_monthly BIGINT NOT NULL DEFAULT 0,
    volume_breach_zone TEXT NOT NULL DEFAULT 'normal';
    -- normal | warn | soft | hard

-- usage_meters: new meter type
-- meter = 'billable_events'  (monthly, weighted)

-- CH / events: optional column
-- filter_reject_class TEXT  -- accepted | lua_short_circuit | ...
```

---

## 10. API (proposal, optional)

| Route | Purpose |
| :--- | :--- |
| `GET /api/v1/license/volume` | MTD billable vs quota, breach zone, projected month-end |
| `GET /api/v1/license/volume/breakdown` | accepted vs discounted vs ebpf_drop counts |
| `GET /api/v1/customers/{id}/subscription` | unchanged; tenant limits inside deployment quota |

```go
type LicenseVolumeDTO struct {
    BillableUnitsMTD    int64   `json:"billable_units_mtd"`
    VolumeQuotaMonthly  int64   `json:"volume_quota_monthly"`
    UtilizationPct      float64 `json:"utilization_pct"`
    BreachZone          string  `json:"breach_zone"`
    OverageUnits        int64   `json:"overage_units,omitempty"`
    DiscountedUnitsMTD  int64   `json:"discounted_units_mtd"`
    EBPFZeroCostDrops   int64   `json:"ebpf_zero_cost_drops_mtd"`
    ResetsAt            string  `json:"resets_at"`
}
```

---

## 11. Package layout (if accepted)

```text
internal/licensing/
  volume_policy.go      # breach zones, enforcement_mode
  billable_weights.go

internal/management/
  volume_meter_worker.go   # hourly CH → billable_units
  license_volume_alerts.go # Prometheus + notifier

internal/ingestion/
  filter_reject_class.go   # tag reject for CH (cold metadata only)
```

Не трогает hot-path alloc budget без bench gate.

---

## 12. Strategic positioning (sales)

| Pitch | Содержание |
| :--- | :--- |
| **Efficiency, not runtime tax** | Продаётся prepaid **billable** volume с discount за dedup/fcap/eBPF, не штраф за bot spike |
| **Network privilege** | RPS/RPD защищают железо; volume — коммерция; клиент не «падает» на 100% при hybrid mode |
| **Tiered intelligence** | OpenRTB и ML — явные feature flags, как model tiers у Anthropic |

---

## 13. Матрица цен: абстрактные коэффициенты (PU)

**PROPOSAL** — коммерческая упаковка on-prem license. Цифры из прайса переведены в **PU (Pricing Unit)** — абстрактную месячную единицу стоимости, **без привязки к валюте**. Оператор умножает PU на локальный `pu_rate` при выставлении счёта (USD, EUR, AED — вне runtime).

### 13.1 Якорь и полосы объёма

| Volume band | Код | Billable events / month | `tier_level` | Профиль клиента |
| :--- | :--- | :--- | :--- | :--- |
| **Small** | `S` | ≤ 10×10⁹ | 1 | Mid-market, одна сеть |
| **Medium** | `M` | ≤ 50×10⁹ | 2 | Highload, RTB + несколько шардов |
| **Large** | `L` | ≥ 100×10⁹ | 3 | Enterprise, edge + ML + SLA |

**Якорь:** базовая платформа Band **S** = **100 PU** / month.

Масштаб полосы относительно S (коэффициент полосы **σ_band**):

| Band | σ_band (к base S) |
| :--- | :--- |
| S | 1.0 |
| M | 2.5 |
| L | 5.0 |

### 13.2 Компоненты и κ-коеффициенты (PU / month)

| Компонент | Feature flag (JWT) | κ[S] | κ[M] | κ[L] | ω (ценность фичи) |
| :--- | :--- | ---: | ---: | ---: | ---: |
| **Базовая платформа** | `ingestion_gnet` + admin + K8s | **100** | **250** | **500** | 1.00 |
| **OpenRTB Engine** | `openrtb_engine` | 50 | 120 | 250 | **1.25** |
| **eBPF/XDP Filter** | `ebpf_xdp_edge` | 40 | 100 | 200 | 1.05 |
| **ML IVT + Analytics** | `ivt_ml_detector` | 40 | 80 | 150 | 1.00 |
| **All-In bundle** | all above | 200 | 480 | 950 | — |

**ω (omega)** — относительный вес ценности модуля для sales/upsell (не входит в формулу PU, используется в CRM и recommendation engine). OpenRTB — максимальный upsell: прямой ROI на арбитраже.

**ν (nu)** — доля add-on к базе в той же полосе (`κ_module / κ_base`):

| Модуль | ν[S] | ν[M] | ν[L] | Тренд |
| :--- | ---: | ---: | ---: | :--- |
| OpenRTB | 0.50 | 0.48 | 0.50 | стабильно ~½ базы |
| eBPF/XDP | 0.40 | 0.40 | 0.40 | инфра-модуль, фикс. доля |
| ML IVT | 0.40 | 0.32 | 0.30 | дешевеет относительно базы на L |

### 13.3 Формулы

**À la carte** (включённые модули `m ∈ {rtb, ebpf, ml}`):

```text
monthly_PU = κ_base[band] + Σₘ enabled(m) × κ_module_m[band]
```

**All-In bundle** (скидка **β**):

```text
monthly_PU_bundle = β[band] × (κ_base[band] + κ_rtb + κ_ebpf + κ_ml)

β[S] ≈ 0.870   (200 / 230)
β[M] ≈ 0.873   (480 / 550)
β[L] ≈ 0.864   (950 / 1100)
β̄  ≈ 0.87      (средняя скидка full stack ~13%)
```

**Локальный счёт** (on-prem, вне hot path):

```text
invoice_amount = monthly_PU × pu_rate_local
```

`pu_rate_local` задаётся в контракте; в JWT/PG хранятся только PU и feature flags.

### 13.4 Матрица включений (что в базе)

| Capability | Base | +RTB | +eBPF | +ML | All-In |
| :--- | :---: | :---: | :---: | :---: | :---: |
| gnet tracker, Redis, PG, CH ingest | ✓ | ✓ | ✓ | ✓ | ✓ |
| Admin API / management | ✓ | ✓ | ✓ | ✓ | ✓ |
| Unlimited shard count (license) | ✓ | ✓ | ✓ | ✓ | ✓ |
| OpenRTB live + shadow | | ✓ | | | ✓ |
| Edge XDP line-rate drop | | | ✓ | | ✓ |
| ivt-detector + CH fraud analytics | | | | ✓ | ✓ |
| `volume_quota_monthly` ceiling | σ_band × 10¹⁰ events | same band | same | same | same |

### 13.5 Mapping → license JWT

```json
{
  "tier_level": 2,
  "volume_band": "M",
  "volume_quota_monthly": 50000000000,
  "pricing": {
    "monthly_pu": 480,
    "pu_components": {
      "base": 250,
      "openrtb_engine": 120,
      "ebpf_xdp_edge": 100,
      "ivt_ml_detector": 80
    },
    "bundle": "all_in",
    "bundle_discount_beta": 0.873
  },
  "features": {
    "ingestion_gnet": true,
    "openrtb_engine": true,
    "ebpf_xdp_edge": true,
    "ivt_ml_detector": true
  }
}
```

`monthly_pu` — для строки инвойса `reference_type=license` (отдельно от tenant `subscription` и ad `spend`).

### 13.6 Orthogonal: deployment PU vs tenant subscription

| Слой | Единица | Пример |
| :--- | :--- | :--- |
| **License (deployment)** | PU / month, κ по band S/M/L | Покупатель платит eSPX vendor |
| **Tenant subscription** | Basic / Pro / Enterprise | Оператор платформы тарифицирует своих клиентов |
| **Ad spend** | micro-units ledger | Бюджет кампаний |

Оператор может перепродавать: `tenant_fee_PU = α_resell × κ_base[band]` (α_resell > 1) — вне scope runtime.

### 13.7 Сводная таблица PU (для прайс-листа)

| | **S** (≤10B) | **M** (≤50B) | **L** (100B+) |
| :--- | ---: | ---: | ---: |
| Base | 100 | 250 | 500 |
| + OpenRTB | 50 | 120 | 250 |
| + eBPF/XDP | 40 | 100 | 200 |
| + ML IVT | 40 | 80 | 150 |
| **All-In** | **200** | **480** | **950** |

Исходный коммерческий прайс (USD/month, on-prem) — один из возможных `pu_rate` (при якоре 100 PU = $1,000 → `pu_rate = $10/PU`). Другие регионы меняют только `pu_rate`, не κ.

---

## 14. Decision checklist (architectural review)

| # | Question | Options |
| :--- | :--- | :--- |
| 1 | Primary commercial meter | A) `max_events_per_month` (canonical) B) weighted `billable_events` (proposal) C) both |
| 2 | 100% volume exceeded | A) fail-closed (M6) B) soft overage + invoice (proposal) C) `enforcement_mode` per license |
| 3 | Lua discount 0.1 | Implement CH reject class + hourly worker? |
| 4 | eBPF zero-cost | Require M5 `edge_xdp` profile; metric contract with edge node |
| 5 | tier_level vs tenant plans | Keep orthogonal: license ceiling ∩ subscription |

---

## 15. Related documents

- [LICENSING.md](../LICENSING.md) — canonical non-blocking license server (M6)
- [SUBSCRIPTIONS.md](../SUBSCRIPTIONS.md) — tenant Basic / Pro / Enterprise
- [MANAGEMENT.md](../MANAGEMENT.md) §18–21 — entitlements, RPD, roadmap
- [MILESTONE.md](../MILESTONE.md) §5 (edge XDP), §6 (M6)
- [CONCEPTS.md](../CONCEPTS.md) §8 — eBPF/XDP

**Status:** PROPOSAL only. Merge into canonical docs after review sign-off.

