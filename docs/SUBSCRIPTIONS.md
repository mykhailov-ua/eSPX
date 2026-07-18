# Tenant Subscriptions (Basic / Pro / Enterprise)

Subscription plans for **platform customers** (advertising and arbitrage networks) inside a deployed eSPX instance. Not the product license — see [LICENSING.md](./LICENSING.md).

**Merged reference:** [MANAGEMENT.md](./MANAGEMENT.md) §18–20 (two-layer entitlements, effective limits).

---

## 1. Purpose

A subscription defines:

1. Quantitative limits (campaigns, RPS, events per month).
2. Included capabilities (RTB live, ML fraud, multi-region).
3. Billing model (base fee + usage overage).

The operator assigns a plan per `customer_id`. Effective limits are `min(license.limits, subscription.limits)` — see MANAGEMENT.md §18.

---

## 2. Distinction from Other Concepts

| Concept | Scope | Example |
| :--- | :--- | :--- |
| **Tenant subscription** (this document) | Customer within an instance | Acme Ads on Pro |
| **Product license** | Right to run eSPX | [LICENSING.md](./LICENSING.md) `deployment_id` |
| **Lua Tier B/C** | Hot-path event routing | `unified-filter.lua` |
| **Ad spend** | Clicks/impressions | `balance_ledger` `reference_type=spend` |

Subscription fee and ad spend are separate invoice lines and ledger `reference_type` values.

---

## 3. Plans

### 3.1 Basic

Entry tier: single buyer or small team, proof of value, strict caps.

| Parameter | Guideline |
| :--- | :--- |
| Regions | 1 |
| Active campaigns | up to 50 |
| RPS (ingress) | до 10,000 | до 50,000 | 200,000+ |
| **Requests / day (RPD)** | 500,000 | 10,000,000 | contract |
| Events / month | up to 5M | up to 50M | committed + overage |
| API keys | up to 2 |
| Self-serve | read-only (balance, usage); no campaign create via API |
| RTB | no |
| ML fraud boost | no |
| Multi-region | no |
| MarginGuard / postback API | no |
| Overdraft | low hard cap |

### 3.2 Pro

Small and mid-size networks, single region, predictable RPS.

| Parameter | Guideline |
| :--- | :--- |
| Regions | 1 |
| Active campaigns | up to 500 |
| RPS | up to 50,000 |
| **Requests / day (RPD)** | 10,000,000 |
| Events / month | up to 50M |
| API keys | up to 5 |
| RTB live | no (shadow allowed) |
| ML fraud boost | no |
| Multi-region | no |
| Ghost IVT / custom fraud | no |
| Fan-out ops API | no |
| MarginGuard, postback ingest, recommendations | yes |
| Overdraft | cap via CreditScoringWorker |

### 3.3 Enterprise

Large networks, multi-region, RTB live, advanced anti-fraud, ops.

| Parameter | Guideline |
| :--- | :--- |
| Regions | per contract (2+) |
| Active campaigns | per contract / no hard cap |
| RPS | per contract (200k+) |
| **Requests / day (RPD)** | per contract | 
| Events / month | committed volume + overage |
| RTB live | yes |
| ML fraud boost | yes |
| Multi-region | yes ([MULTI_REGION.md](./MULTI_REGION.md)) |
| Ghost IVT, slot migration | yes |
| Fan-out ops API | yes |
| White-label scheduled export | yes |
| Overdraft | custom; `customer_subscriptions.overrides_json` |

Numbers live in `subscription_plans.limits_json`. Enterprise allows per-customer overrides without changing `plan_code`.

---

## 4. Feature Matrix

| Feature | Basic | Pro | Enterprise |
| :--- | :---: | :---: | :---: |
| Buyer dashboard API | + | + | + |
| SubID reports (limited export on Basic) | + | + | + |
| Self-serve campaign create | - | + | + |
| RTB shadow | - | + | + |
| RTB live | - | - | + |
| ML fraud boost | - | - | + |
| IVT detector (basic) | + | + | + |
| IVT custom rules | - | - | + |
| Ghost IVT | - | - | + |
| Multi-region | - | - | + |
| Slot migration | - | - | + |
| Priority outbox lanes | - | - | + |
| Fan-out ops API | - | - | + |
| Postback ingest | - | + | + |
| MarginGuard worker | - | + | + |

---

## 5. Three Billing Axes

Ingress is limited on **three independent axes** (see [MANAGEMENT.md](./MANAGEMENT.md) §21):

| Axis | Window | Purpose |
| :--- | :--- | :--- |
| **RPS** | UDP epoch (~seconds) | Burst / infrastructure fairness |
| **RPD** (`max_requests_per_day`) | Calendar day | Commercial daily cap (OpenAI-style) |
| **Events/month** | Calendar month | Subscription fee + overage invoice |

### 5.1 Hard Limits

Enforced on hot path (RPS, RPD) or cold path (campaigns, regions). Hot path reads entitlement snapshot only.

| Meter | Enforcement |
| :--- | :--- |
| `max_active_campaigns` | COUNT in PG before `CreateCampaign` |
| `max_rps` | UDP control plane (`:8190` → tracker `:8191`) |
| `max_requests_per_day` | Redis `ingress:day:{customer_id}:{date}` on `/track`; 429 when exhausted |
| `max_events_per_month` | `usage_meters` + invoice overage (ingest not blocked unless `hard_cap_events_month`) |
| `max_regions` | `enabled_regions` + license ceiling |
| `max_api_keys` | auth schema |
| `max_export_chunk_bytes` | admin API handler |
| `quota_reset_timezone` | IANA TZ for RPD midnight reset (default `UTC`) |

### 5.2 Billing Model

| | Basic | Pro | Enterprise |
| :--- | :--- | :--- | :--- |
| Base fee | low fixed / month | fixed / month | annual / custom |
| Usage overage | events above plan limit | above 50M | committed + overage |
| Ad spend | `balance_ledger` | same | same |

---

## 6. Data Model

```sql
CREATE TABLE subscription_plans (
    code            TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL,
    limits_json     JSONB NOT NULL,
    features_json   JSONB NOT NULL,
    base_fee_micro  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE customer_subscriptions (
    customer_id     UUID PRIMARY KEY REFERENCES customers(id),
    plan_code       TEXT NOT NULL REFERENCES subscription_plans(code),
    status          TEXT NOT NULL,
    period_start    DATE NOT NULL,
    period_end      DATE,
    overrides_json  JSONB,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE usage_meters (
    customer_id     UUID NOT NULL,
    meter           TEXT NOT NULL,
    period          DATE NOT NULL,
    value           BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, meter, period)
);
```

Seed plans: `basic`, `pro`, `enterprise`.

Example `limits_json` (Basic):

```json
{
  "max_active_campaigns": 50,
  "max_rps": 10000,
  "max_requests_per_day": 500000,
  "max_events_per_month": 5000000,
  "max_regions": 1,
  "max_api_keys": 2,
  "max_export_chunk_bytes": 1048576,
  "quota_reset_timezone": "UTC"
}
```

### 6.1 Daily usage mirror

```sql
CREATE TABLE usage_daily (
    customer_id   UUID NOT NULL,
    usage_date    DATE NOT NULL,
    meter         TEXT NOT NULL,
    value         BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, usage_date, meter)
);
```

Flushed from Redis by `DailyQuotaFlushWorker` (cold path). Meters: `requests`, `events`.

`customer_subscriptions.status`: `active`, `past_due`, `cancelled`, `suspended`.

---

## 7. Runtime

### 7.1 Redis snapshot

```text
entitlement:customer:{customer_id}   HASH
  plan=basic|pro|enterprise
  max_rps=10000
  max_requests_per_day=500000
  quota_reset_timezone=UTC
  rtb_live=0
  ...
```

Updated via outbox `UPDATE_ENTITLEMENTS` after plan change.

### 7.2 Hot path checks

| Check | Response |
| :--- | :--- |
| RPS epoch exceeded | 503 `ingress_rps_exceeded` |
| RPD exceeded | 429 `daily_quota_exceeded` + `X-RateLimit-*-Day` headers |
| License EXPIRED | reject spec |

Registry loads effective entitlements per customer. No Postgres, billing gRPC, or license server on `/track`.

### 7.3 Cold path

```text
RequireFeature(customerID, "rtb_live")
RequireUnderLimit(customerID, "active_campaigns", +1)
RequireLicenseFeature("multi_region")   // deployment ceiling
```

403 `plan_feature_required` | `plan_limit_exceeded` | `license_limit_exceeded`.

---

## 8. API

| Method | Route |
| :--- | :--- |
| GET | `/api/v1/customers/{id}/subscription` |
| GET | `/api/v1/customers/{id}/usage` |
| GET | `/api/v1/customers/{id}/usage/daily` |
| GET | `/api/v1/customers/{id}/quota-status` |
| GET | `/api/v1/selfserve/usage` (Pro+) |
| POST | `/admin/customers/{id}/subscription` |
| POST | `/admin/customers/{id}/quota-bump` |

DTO: `SubscriptionDTO` with `effective_limits` after license merge — MANAGEMENT.md §20.8.

---

## 9. Prohibitions

1. External HTTP subscription check on hot path.
2. Plan stored only in env without per-customer snapshot.
3. Mixing subscription fee and ad spend in `customers.balance`.
4. Duplicating limits in Lua beyond RPS/budget already in unified filter.

---

## 10. Related Documents

- [LICENSING.md](./LICENSING.md) — product license, non-blocking license server
- [MANAGEMENT.md](./MANAGEMENT.md) — admin complex, §18–21 (daily RPD quotas)
- [MULTI_REGION.md](./MULTI_REGION.md) — `multi_region` feature
- [MILESTONE.md](./MILESTONE.md) — Milestone 6
